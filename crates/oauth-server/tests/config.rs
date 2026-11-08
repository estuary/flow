use clap::Parser;

#[derive(clap::Parser)]
struct Args {
    #[command(flatten)]
    oauth: Option<oauth_server::Config>,
}

#[test]
fn optional_config_requires_all_fields() {
    let key = "A".repeat(86);
    // Disable ambient deployment configuration so each case tests only its flags.
    let mut command = <Args as clap::CommandFactory>::command();
    for id in ["issuer", "consent_url", "encryption_key"] {
        command = command.mut_arg(id, |arg| arg.env(None::<&str>));
    }

    let all = command
        .clone()
        .try_get_matches_from([
            "agent",
            "--oauth-issuer",
            "https://auth.example.test",
            "--oauth-consent-url",
            "https://dashboard.example.test/consent",
            "--oauth-encryption-key",
            key.as_str(),
        ])
        .unwrap();
    let parsed = <Args as clap::FromArgMatches>::from_arg_matches(&all).unwrap();
    parsed.oauth.unwrap().validate().unwrap();

    let none = command.clone().try_get_matches_from(["agent"]).unwrap();
    let parsed = <Args as clap::FromArgMatches>::from_arg_matches(&none).unwrap();
    assert!(parsed.oauth.is_none());

    for (missing, argv) in [
        (
            "issuer",
            [
                "agent",
                "--oauth-consent-url",
                "https://dashboard.example.test/consent",
                "--oauth-encryption-key",
                key.as_str(),
            ],
        ),
        (
            "consent_url",
            [
                "agent",
                "--oauth-issuer",
                "https://auth.example.test",
                "--oauth-encryption-key",
                key.as_str(),
            ],
        ),
        (
            "encryption_key",
            [
                "agent",
                "--oauth-issuer",
                "https://auth.example.test",
                "--oauth-consent-url",
                "https://dashboard.example.test/consent",
            ],
        ),
    ] {
        let error = command.clone().try_get_matches_from(argv).unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "missing {missing}",
        );
    }
}

#[test]
fn config_from_environment() {
    // Each child gets an isolated environment; parallel tests never mutate the process environment.
    const CHILD: &str = "OAUTH_CONFIG_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let config = Args::try_parse_from(["agent"]).unwrap().oauth.unwrap();
        config.validate().unwrap();
        assert_eq!(config.issuer.as_str(), "https://auth.example.test/");
        assert_eq!(config.encryption_key, "A".repeat(86));
        let help = <Args as clap::CommandFactory>::command()
            .render_long_help()
            .to_string();
        assert!(!help.contains(&config.encryption_key));
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "config_from_environment", "--nocapture"])
        .env(CHILD, "1")
        .env("OAUTH_ISSUER", "https://auth.example.test")
        .env(
            "OAUTH_CONSENT_URL",
            "https://dashboard.example.test/consent",
        )
        .env("OAUTH_ENCRYPTION_KEY", "A".repeat(86))
        .status()
        .unwrap();
    assert!(status.success());
}
