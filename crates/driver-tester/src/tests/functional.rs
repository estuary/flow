//! The functional test is meant as a basic "tire kicker" for drivers. It sequentially executes a
//! few transactions from a few different sessions, verifying that documents can be stored and
//! loaded. Verification of loaded documents will be skipped for drivers that return an
//! `always_empty_hint` of `true`.
use crate::tests::{assert_checkpoint_eq, test_collection, MaterializationFixture};
use crate::Fixture;
use protocol::{
    collection::CollectionExt,
    flow::CollectionSpec,
    materialize::{FenceRequest, SessionRequest},
};
use tracing::debug;

#[tracing::instrument(level = "debug", name = "functional")]
pub async fn test(fixture: &mut Fixture) -> Result<(), anyhow::Error> {
    let collection = setup(fixture).await?;
    run(fixture, collection).await
}

#[tracing::instrument(level = "debug")]
pub async fn setup(fixture: &mut Fixture) -> Result<CollectionSpec, anyhow::Error> {
    Ok(test_collection())
}

#[tracing::instrument(level = "info", skip(collection))]
pub async fn run(fixture: &mut Fixture, collection: CollectionSpec) -> Result<(), anyhow::Error> {
    let mut client = fixture.client.clone();

    let shard_id = "functional-test";

    // The setup is part of the actual test here, not the setup, since this is a basic functional
    // test and we want to consider the validate and apply steps as part of the behavior we are
    // verifying.
    let collection_fixture = MaterializationFixture::exec_setup(fixture, collection, false).await?;

    let start_session = SessionRequest {
        endpoint_url: fixture.endpoint.clone(),
        target: fixture.target.clone(),
        shard_id: String::from(shard_id),
    };

    let handle = client
        .start_session(start_session)
        .await?
        .into_inner()
        .handle;
    debug!(
        "using handle: {}",
        String::from_utf8_lossy(handle.as_slice())
    );

    // Test the initial Fence call. We can't make any assumptions about the checkpoint returned
    // here, since it may either be empty or have come from some prior test run.
    let flow_checkpoint = client
        .fence(FenceRequest {
            handle: handle.clone(),
            driver_checkpoint: Vec::new(),
        })
        .await?
        .into_inner()
        .flow_checkpoint;
    debug!(
        "executed first fence request, ignoring initial checkpoint value: {}",
        String::from_utf8_lossy(&flow_checkpoint)
    );

    // Some initial docs, which won't exist yet.
    let initial_docs = collection_fixture.rand_test_docs(3, fixture.rng());
    let first_checkpoint: Vec<u8> = b"the first checkpoint value".to_vec();

    let (txn_send, txn_recv) = collection_fixture
        .start_transaction(&mut client, handle.clone(), first_checkpoint.clone())
        .await?;
    debug!("started transaction");
    let (mut txn_send, txn_recv, always_empty_hint) = collection_fixture
        .verify_load(txn_send, txn_recv, None, &[])
        .await?;
    debug!("verified load");

    txn_send
        .send_store(&collection_fixture.field_pointers, &initial_docs)
        .await?;
    txn_send.finish();

    let store_resp = txn_recv.recv_store_response().await?;
    debug!(
        "store completed with driver checkpoint: {}",
        String::from_utf8_lossy(&store_resp.driver_checkpoint)
    );

    let second_checkpoint: Vec<u8> = b"second checkpoint value".to_vec();
    let (txn_send, txn_recv) = collection_fixture
        .start_transaction(&mut client, handle.clone(), second_checkpoint.clone())
        .await?;

    let (mut txn_send, txn_recv, _) = collection_fixture
        .verify_load(txn_send, txn_recv, Some(always_empty_hint), &initial_docs)
        .await?;

    // Make another store request that adds some new documents as well as updates some existing
    // documents.
    let mut new_docs = collection_fixture.rand_test_docs(3, fixture.rng());

    // The value that we'll update in each of the existing documents.
    let int_val_projection = collection_fixture.spec.get_projection("intValue").unwrap();
    new_docs.extend(initial_docs.into_iter().map(|mut doc| {
        doc.update_in_place(fixture.rng(), int_val_projection);
        doc
    }));
    txn_send
        .send_store(&collection_fixture.field_pointers, &new_docs)
        .await?;
    txn_send.finish();
    let store_resp = txn_recv.recv_store_response().await?;

    // Start a new session and ensure that the previous checkpoint is returned
    let start_session = SessionRequest {
        endpoint_url: fixture.endpoint.clone(),
        target: fixture.target.clone(),
        shard_id: String::from(shard_id),
    };

    let handle = client
        .start_session(start_session)
        .await?
        .into_inner()
        .handle;

    let flow_checkpoint = client
        .fence(FenceRequest {
            handle: handle.clone(),
            driver_checkpoint: store_resp.driver_checkpoint,
        })
        .await?
        .into_inner()
        .flow_checkpoint;
    debug!("executed second fence request");
    assert_checkpoint_eq(&second_checkpoint, &flow_checkpoint)?;

    let (txn_send, txn_recv) = collection_fixture
        .start_transaction(&mut client, handle.clone(), b"third checkpoint".to_vec())
        .await?;
    let (txn_send, txn_recv, _) = collection_fixture
        .verify_load(txn_send, txn_recv, Some(always_empty_hint), &new_docs)
        .await?;
    txn_send.finish();
    txn_recv.recv_store_response().await?;
    Ok(())
}
