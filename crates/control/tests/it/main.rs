use control::config::app_env::{self, AppEnv};

#[macro_use]
extern crate ctor;
#[macro_use]
extern crate insta;
extern crate sqlx;

mod accounts;
mod connector_images;
mod connectors;
mod health_check;
mod support;

/// Setup runs exactly once before any tests run. This allows the test suite to
/// perform any one-time setup.
#[ctor]
fn setup() {
    // TODO: Make this a flag we can pass to the test invocation?
    // Uncomment to flip on detailed tracing in tests. Sometimes useful.
    // flow_cli_common::init_logging(&flow_cli_common::LogArgs {
    //     level: "debug".to_owned(),
    //     format: None,
    // });

    app_env::force_env(AppEnv::Test);
    let settings =
        control::config::load_settings("config/test.toml").expect("to load configuration file");

    // Setup will create the database and run all migrations.
    support::test_database::setup(settings.database.clone()).expect("To setup the database");
}

/// Teardown runs exactly once after all tests have run. This allows the test
/// suite to perform any one-time cleanup.
#[dtor]
fn teardown() {}
