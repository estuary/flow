use crate::support::{self, factory, redactor, test_context};

use control::models::builds::{Build, State};
use control::repo::builds::{dequeue_build, fetch_for_account, update_build_state};

#[tokio::test]
async fn index_test() {
    let mut t = test_context!();
    let batman = factory::BatmanAccount.create(t.db()).await;
    let joker = factory::JokerAccount.create(t.db()).await;
    let build_batman = factory::AcmeBuild.create(t.db(), batman.id).await;
    let build_joker = factory::AcmeBuild.create(t.db(), joker.id).await;

    let redactor = redactor::Redactor::default()
        .redact(batman.id, "account<batman>")
        .redact(joker.id, "account<joker>")
        .redact(build_batman.id, "build<batman>")
        .redact(build_joker.id, "build<joker>");

    // Batman views their build.
    t.login(batman.clone());
    let mut response = t.get("/builds").await;

    // Assert we get Batman's build (only).
    assert!(response.status().is_success());
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn create_test() {
    let mut t = test_context!();
    let account = factory::BatmanAccount.create(t.db()).await;
    t.login(account.clone());
    let catalog = factory::AcmeBuild.attrs();

    let mut response = t.post("/builds", &catalog).await;

    // Expect build was created.
    let builds = fetch_for_account(t.db(), account.id)
        .await
        .expect("to insert test data");
    assert_eq!(1, builds.len());

    // Expect build was returned in the response.
    assert_eq!(201, response.status().as_u16());
    let redactor = redactor::Redactor::default()
        .redact(account.id, "a1")
        .redact(builds[0].id, "b1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn show_test() {
    let mut t = test_context!();
    let batman = factory::BatmanAccount.create(t.db()).await;
    let joker = factory::JokerAccount.create(t.db()).await;
    let build = factory::AcmeBuild.create(t.db(), batman.id).await;

    let build_url = format!("/builds/{}", build.id);
    let redactor = redactor::Redactor::default()
        .redact(batman.id, "account<batman>")
        .redact(build.id, "build<batman>");

    // Joker attempts to view Batman's build, but it 404's.
    t.login(joker.clone());
    let response = t.get(&build_url).await;
    assert_eq!(response.status().canonical_reason(), Some("Not Found"));

    // Batman can view their own build.
    t.login(batman.clone());
    let mut response = t.get(&build_url).await;

    assert!(response.status().is_success());
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn dequeue_builds_test() {
    let t = test_context!();
    let batman = factory::BatmanAccount.create(t.db()).await;
    let build_1 = factory::AcmeBuild.create(t.db(), batman.id).await;

    // First dequeue obtains build_1 and holds its transaction open.
    let mut txn_1 = t.db().begin().await.unwrap();
    let build = dequeue_build(&mut txn_1).await.unwrap();
    assert!(matches!(build, Some(Build { id, .. }) if id == build_1.id));

    // Second build is added, and a parallel transaction dequeues it.
    let build_2 = factory::AcmeBuild.create(t.db(), batman.id).await;

    let mut txn_2 = t.db().begin().await.unwrap();
    let build = dequeue_build(&mut txn_2).await.unwrap();
    assert!(matches!(build, Some(Build { id, .. }) if id == build_2.id));

    // Third dequeue is attempted, but doesn't obtain a build as both are locked.
    let mut txn_3 = t.db().begin().await.unwrap();
    let build = dequeue_build(&mut txn_3).await.unwrap();
    assert!(matches!(build, None));
    std::mem::drop(txn_3);

    // Second transaction updates its build and commits.
    update_build_state(&mut txn_2, build_2.id, State::Done)
        .await
        .unwrap();
    txn_2.commit().await.unwrap();

    // First transaction aborts.
    std::mem::drop(txn_1);

    // First build is now eligible for dequeue again.
    let mut txn_4 = t.db().begin().await.unwrap();
    let build = dequeue_build(&mut txn_4).await.unwrap();
    assert!(matches!(build, Some(Build { id, .. }) if id == build_1.id));

    // It's completed.
    update_build_state(&mut txn_4, build_1.id, State::TestFailed { code: None })
        .await
        .unwrap();
    txn_4.commit().await.unwrap();

    // No further builds are queued.
    let mut txn_5 = t.db().begin().await.unwrap();
    let build = dequeue_build(&mut txn_5).await.unwrap();
    assert!(matches!(build, None));
}
