use crate::tests::{assert_checkpoint_eq, test_collection, MaterializationFixture};
use crate::Fixture;
use protocol::{
    collection::CollectionExt,
    materialize::{FenceRequest, SessionRequest},
};
use std::time::Duration;
use tokio::time::timeout;
use tracing::debug;

const SHARD_ID: &str = "transactional-test";

#[tracing::instrument(level = "debug", name = "functional")]
pub async fn test(fixture: &mut Fixture) -> Result<(), anyhow::Error> {
    let collection = setup(fixture).await?;
    run(fixture, collection).await
}

#[tracing::instrument(level = "debug")]
pub async fn setup(fixture: &mut Fixture) -> Result<MaterializationFixture, anyhow::Error> {
    let collection = test_collection();

    let collection_fixture = MaterializationFixture::exec_setup(fixture, collection, false).await?;
    Ok(collection_fixture)
}

#[tracing::instrument(level = "info", skip(collection_fixture))]
pub async fn run(
    fixture: &mut Fixture,
    collection_fixture: MaterializationFixture,
) -> Result<(), anyhow::Error> {
    let mut client = fixture.client.clone();

    let start_session = SessionRequest {
        endpoint_url: fixture.endpoint.clone(),
        target: fixture.target.clone(),
        shard_id: String::from(SHARD_ID),
    };

    let handle_one = client
        .start_session(start_session)
        .await?
        .into_inner()
        .handle;

    // We don't care about this initial value of the checkpoint. We'll set it to a known value
    // before we enter the main test loop.
    let _ = client
        .fence(FenceRequest {
            handle: handle_one.clone(),
            driver_checkpoint: Vec::new(),
        })
        .await?;

    let txn_1_checkpoint = b"initial checkpoint value".to_vec();
    let initial_docs = collection_fixture.rand_test_docs(3, fixture.rng());

    let (mut txn_send, load_results) = collection_fixture
        .start_transaction(&mut client, handle_one.clone(), txn_1_checkpoint.clone())
        .await?;
    txn_send
        .send_load(&collection_fixture.spec, &initial_docs)
        .await?;
    let mut store_send = txn_send.finish_loads().await?;
    // Ignore this initial set of loaded documents since they were not yet stored by this test run.
    let (store_recv, _existing_docs, always_empty_hint) = load_results.await??;

    store_send
        .send_store(&collection_fixture.field_pointers, &initial_docs)
        .await?;
    store_send.finish();
    let first_response = store_recv.recv_store_response().await?;

    let mut last_handle = handle_one;
    let mut last_flow_checkpoint = txn_1_checkpoint;
    let mut last_driver_checkpoint = first_response.driver_checkpoint;

    // Documents that we know should always exist and match what's in the remote store.
    let mut existing_docs = initial_docs;

    // Documents that we know should _not_ exist, either because they're brand new, or because they
    // were added as part of a failed transaction.
    let mut missing_docs = collection_fixture.rand_test_docs(3, fixture.rng());

    // TODO: pass iterations on args?
    let iterations: i32 = 10;

    // We'll reuse the last "good" handle to start a store request and leave it open. This
    // request will never be completed. Then we'll simulate a new instance starting by starting
    // a new session and executing a new fence request. This needs to poison the first store
    // transaction, which ought to return an error.
    for i in 0..iterations {
        let prev_flow_checkpoint = format!("prev checkpoint {}", i).into_bytes();
        // The value that we'll update in each of the existing documents.
        let int_val_projection = collection_fixture.spec.get_projection("intValue").unwrap();

        let mut first_store_docs = missing_docs.clone();
        first_store_docs.extend(collection_fixture.rand_test_docs(3, fixture.rng()));
        first_store_docs.extend(existing_docs.iter().map(|doc| {
            let mut doc = doc.clone();
            doc.update_in_place(fixture.rng(), int_val_projection);
            doc
        }));
        let loader = collection_fixture
            .start_transaction(
                &mut client,
                last_handle.clone(),
                prev_flow_checkpoint.clone(),
            )
            .await?;
        // Attempt to load all known documents, and assert that only the expected are returned, and
        // that they have the expected values.
        let (mut prev_store_send, prev_store_resp, _) = collection_fixture
            .verify_load_docs(
                loader,
                Some(always_empty_hint),
                &first_store_docs,
                &existing_docs,
            )
            .await?;
        // We'll send the first documents over, but leave this transaction open for now
        prev_store_send
            .send_store(&collection_fixture.field_pointers, &first_store_docs)
            .await?;

        // Now we'll start a new session and execute a new fence request from it. If the driver
        // behaves correctly, this can play out in one of two ways.
        // 1. As soon as this Fence is executed, the in-progress transaction will be "poisoned",
        //    and will return an error (either immediately, or when we try to commit by closing the
        //    send stream).
        // 2. This Fence request will block until the first transaction finishes.
        // Since either possibility is acceptable, the assertions following this point will depend
        // on the outcome of the first transaction.
        let new_handle = client
            .start_session(SessionRequest {
                endpoint_url: fixture.endpoint.clone(),
                target: fixture.target.clone(),
                shard_id: SHARD_ID.to_string(),
            })
            .await?
            .into_inner()
            .handle;

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let prev_resp_bar = barrier.clone();

        let prev_resp_task = tokio::spawn(async move {
            // Either the Fence or the previous transaction may complete first, but we want to
            // ensure that the Fence request has at least started executing before we try to
            // complete this transaction.
            let _ = timeout(Duration::from_millis(500), prev_resp_bar.wait()).await;
            prev_store_send.finish();
            prev_store_resp.recv_store_response().await
        });

        // Note that `last_driver_checkpoint` here is that last value that is known at this point,
        // which cannot account for a value returned by `prev_resp_task` if that completes
        // successfully. If a driver allows the previous transaction to commit after this fence
        // begins, then we may send a stale value for this `driver_checkpoint`. The expectation
        // here is that, if a driver cares about the driver_checkpoint, then it must either guarantee
        // that an in-progress transaction _cannot_ commit, or it must be able to deal with the
        // stale driver_checkpoint (likely by returning an error).
        let new_fence = client
            .fence(FenceRequest {
                handle: new_handle.clone(),
                driver_checkpoint: last_driver_checkpoint.clone(),
            })
            .await?
            .into_inner();

        debug!("outer wait on barrier");
        barrier.wait().await;

        let prev_resp = prev_resp_task.await?;
        match prev_resp {
            Ok(resp) => {
                // The first transaction completed successfully, so we'll expect that the stored
                // docs should be returned and verified on the next go around.
                existing_docs = first_store_docs;
                missing_docs.clear(); // and don't expect any of them to be missing
                                      // If the previous transaction committed successfully, then the fence request must
                                      // return the flow_checkpoint from that transaction.
                assert_checkpoint_eq(&prev_flow_checkpoint, &new_fence.flow_checkpoint)?;
                last_flow_checkpoint = new_fence.flow_checkpoint;
                last_driver_checkpoint = resp.driver_checkpoint;
            }
            Err(err) => {
                // If the first transaction failed to commit, then we'll expect the flow_checkpoint
                // from the fence request to match the value from the last successful transaction.
                debug!("poisoned transaction returned expected error: {:?}", err);
                assert_checkpoint_eq(&last_flow_checkpoint, &new_fence.flow_checkpoint)?;
            }
        }

        let next_flow_checkpoint = format!("good checkpoint {}", i).into_bytes();
        // Add new documents on each iteration. This is done to gradually increase the transaction
        // size so we can start testing small batches and work up to testing large ones.
        let new_good_docs = collection_fixture.rand_test_docs(3, fixture.rng());

        // Perform a transaction that's always expected to commit successfully. We won't do
        // anything to mess with this one. This just asserts that we're able to store documents
        // after the previous Fence switched the active session.
        let good_loader = collection_fixture
            .start_transaction(
                &mut client,
                new_handle.clone(),
                next_flow_checkpoint.clone(),
            )
            .await?;
        let (mut good_store_send, good_store_recv, _) = collection_fixture
            .verify_load_docs(good_loader, Some(always_empty_hint), &new_good_docs, &[])
            .await?;
        good_store_send
            .send_store(&collection_fixture.field_pointers, &new_good_docs)
            .await?;
        existing_docs
            .iter_mut()
            .for_each(|doc| doc.update_in_place(fixture.rng(), int_val_projection));
        good_store_send
            .send_store(&collection_fixture.field_pointers, &existing_docs)
            .await?;
        good_store_send.finish();
        let good_resp = good_store_recv.recv_store_response().await?;

        existing_docs.extend(new_good_docs);
        last_driver_checkpoint = good_resp.driver_checkpoint;
        last_handle = new_handle;
    }
    Ok(())
}
