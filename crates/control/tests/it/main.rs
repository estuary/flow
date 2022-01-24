use control::config::app_env::{self, AppEnv};

#[macro_use]
extern crate ctor;

#[macro_use]
extern crate insta;

mod connectors;
mod health_check;
mod support;

/// Setup runs exactly once before any tests run. This allows the test suite to
/// perform any one-time setup.
#[ctor]
fn setup() {
    app_env::force_env(AppEnv::Test);

    let test_db = support::TestDatabase::new();

    // Dropping the database may not be possible, as it may not yet exist, but this is okay.
    let _ = test_db.drop();

    // Setup will create the database and run all migrations.
    test_db.setup().expect("To setup the database");
}

/// Teardown runs exactly once after all tests have run. This allows the test
/// suite to perform any one-time cleanup.
#[dtor]
fn teardown() {}
