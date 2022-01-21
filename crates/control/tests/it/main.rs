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
}

/// Teardown runs exactly once after all tests have run. This allows the test
/// suite to perform any one-time cleanup.
#[dtor]
fn teardown() {}
