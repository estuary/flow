use crate::config::Config;
use crate::output::{CliOutput, Output};
use anyhow::Context;
use models::authorizations::ControlClaims;
use std::path::Path;

/// List configuration profiles, and show or save the active profile.
///
/// A profile is a `<name>.json` file in the flowctl config directory.
/// `flowctl --profile <name> auth login` creates one.
#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Profile {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// List profiles with their signed-in accounts. `*` marks the active profile.
    List,
    /// Show the active profile and where the choice came from.
    ///
    /// The active profile is the first of: the `--profile` flag,
    /// $FLOWCTL_PROFILE, the profile saved by `flowctl profile use`, and "default".
    Current,
    /// Save a profile as the active profile.
    Use(Use),
    /// Delete a profile. The active profile cannot be deleted.
    Delete(Delete),
}

#[derive(Debug, clap::Args)]
pub struct Delete {
    /// Name of the profile to delete.
    name: String,
    /// Delete without a confirmation prompt.
    #[clap(long)]
    yes: bool,
}

#[derive(Debug, clap::Args)]
pub struct Use {
    /// Name of an existing profile.
    name: String,
}

const DEFAULT_PROFILE: &str = "default";
const FLOWCTL_PROFILE: &str = "FLOWCTL_PROFILE";
/// File in the config directory that holds the name saved by `profile use`.
const ACTIVE_PROFILE_FILE: &str = "active-profile";

/// What chose the active profile.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Source {
    Flag,
    Env,
    Saved,
    Default,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Source::Flag => "--profile flag",
            Source::Env => "FLOWCTL_PROFILE environment variable",
            Source::Saved => "saved by `flowctl profile use`",
            Source::Default => "default",
        })
    }
}

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct Active {
    pub name: String,
    pub source: Source,
}

/// Resolve the active profile from the `--profile` flag, $FLOWCTL_PROFILE,
/// and the name saved in the config directory `dir`.
pub fn active(dir: &Path, flag: Option<&str>) -> anyhow::Result<Active> {
    let saved = read_saved(dir)?;
    Ok(resolve(flag, env_profile().as_deref(), saved.as_deref()))
}

fn env_profile() -> Option<String> {
    // clap's `env` ignored an empty variable, and this keeps that behavior.
    std::env::var(FLOWCTL_PROFILE)
        .ok()
        .filter(|value| !value.is_empty())
}

fn resolve(flag: Option<&str>, env: Option<&str>, saved: Option<&str>) -> Active {
    let (name, source) = if let Some(name) = flag {
        (name, Source::Flag)
    } else if let Some(name) = env {
        (name, Source::Env)
    } else if let Some(name) = saved {
        (name, Source::Saved)
    } else {
        (DEFAULT_PROFILE, Source::Default)
    };
    Active {
        name: name.to_string(),
        source,
    }
}

/// The name saved by `profile use`, or None if no name is saved.
fn read_saved(dir: &Path) -> anyhow::Result<Option<String>> {
    let path = dir.join(ACTIVE_PROFILE_FILE);
    match std::fs::read_to_string(&path) {
        Ok(name) => {
            let name = name.trim();
            Ok((!name.is_empty()).then(|| name.to_string()))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

/// Rejects a name that is not a plain file name in the config directory,
/// such as `../x`. `use` and `delete` build a path from the name.
fn validate_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() || name.starts_with('.') || name.contains(['/', '\\']) {
        anyhow::bail!("invalid profile name {name:?}");
    }
    Ok(())
}

fn require_profile(dir: &Path, name: &str) -> anyhow::Result<()> {
    validate_name(name)?;
    if !dir.join(format!("{name}.json")).is_file() {
        anyhow::bail!(
            "profile {name:?} does not exist. Run `flowctl profile list` to see profiles, \
            or `flowctl --profile {name} auth login` to create it"
        );
    }
    Ok(())
}

fn save_active(dir: &Path, name: &str) -> anyhow::Result<()> {
    require_profile(dir, name)?;
    let path = dir.join(ACTIVE_PROFILE_FILE);
    std::fs::write(&path, format!("{name}\n"))
        .with_context(|| format!("writing {}", path.display()))
}

/// Returns the file of profile `name`, if flowctl may delete it.
fn deletable(dir: &Path, name: &str, active: &Active) -> anyhow::Result<std::path::PathBuf> {
    require_profile(dir, name)?;
    if name == active.name {
        anyhow::bail!(
            "profile {name:?} is the active profile ({}). Switch to another profile first",
            active.source
        );
    }
    // A flag or $FLOWCTL_PROFILE can hide the saved profile. It becomes active
    // again when they are unset, so it must still exist then.
    if read_saved(dir)?.as_deref() == Some(name) {
        anyhow::bail!(
            "profile {name:?} is saved by `flowctl profile use`. \
            Run `flowctl profile use` with another profile first"
        );
    }
    Ok(dir.join(format!("{name}.json")))
}

fn confirm(prompt: &str) -> anyhow::Result<bool> {
    use crossterm::tty::IsTty;
    use std::io::Write;

    if !std::io::stdin().is_tty() {
        anyhow::bail!("cannot ask for confirmation without a terminal. Pass --yes to confirm");
    }
    print!("{prompt} [y/N] ");
    std::io::stdout().flush()?;

    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .context("reading confirmation")?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes" | "YES"))
}

impl Profile {
    pub fn run(&self, flag: Option<&str>, output: &Output) -> anyhow::Result<()> {
        let dir = Config::config_dir()?;
        let active = active(&dir, flag)?;

        match &self.cmd {
            Command::List => {
                let rows = list(&dir, &active.name, now_unix())?;
                crate::output::print_all(output.output_type(), rows, ())
            }
            Command::Current => crate::output::print_all(output.output_type(), [active], ()),
            Command::Use(Use { name }) => {
                save_active(&dir, name)?;
                println!("Active profile is now {name:?}.");

                // Without this warning, `use` looks like it did nothing.
                if let Some(env) = env_profile() {
                    tracing::warn!(
                        "FLOWCTL_PROFILE={env} is set and overrides the saved profile. \
                        Unset it to use {name:?}."
                    );
                }
                Ok(())
            }
            Command::Delete(Delete { name, yes }) => {
                let path = deletable(&dir, name, &active)?;
                if !yes && !confirm(&format!("Delete profile {name:?}?"))? {
                    println!("Profile {name:?} was not deleted.");
                    return Ok(());
                }
                std::fs::remove_file(&path)
                    .with_context(|| format!("deleting {}", path.display()))?;
                println!("Deleted profile {name:?}.");
                Ok(())
            }
        }
    }
}

/// Sign-in state of a profile, from its stored tokens.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Status {
    SignedIn,
    /// The access token is expired and there is no refresh token to renew it.
    Expired,
    LoggedOut,
    /// The access token is not a JWT with the expected claims.
    InvalidToken,
}

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct ProfileRow {
    pub name: String,
    pub active: bool,
    /// The email of the signed-in user, or their user ID if the token has no email.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    pub status: Status,
}

/// Profile names in `dir`, sorted. A missing `dir` has no profiles.
fn profile_names(dir: &Path) -> anyhow::Result<Vec<String>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("reading config dir {}", dir.display()));
        }
    };

    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.context("reading config dir entry")?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str().and_then(|n| n.strip_suffix(".json")) else {
            continue;
        };
        if !name.is_empty() {
            names.push(name.to_string());
        }
    }
    names.sort();
    Ok(names)
}

fn list(dir: &Path, active: &str, now: u64) -> anyhow::Result<Vec<ProfileRow>> {
    let mut rows = Vec::new();
    for name in profile_names(dir)? {
        let config = Config::load_file(&dir.join(format!("{name}.json")))?;
        let (account, status) = account_status(&config, now);
        rows.push(ProfileRow {
            active: name == active,
            name,
            account,
            status,
        });
    }
    Ok(rows)
}

/// Reads the account from the access token without verifying it. The token
/// only labels a local profile here, and is never sent anywhere.
fn account_status(config: &Config, now: u64) -> (Option<String>, Status) {
    let Some(token) = &config.user_access_token else {
        return (None, Status::LoggedOut);
    };
    let Ok(parsed) = tokens::jwt::parse_unverified::<ControlClaims>(token.as_bytes()) else {
        return (None, Status::InvalidToken);
    };
    let claims = parsed.claims();
    let account = claims
        .email
        .clone()
        .unwrap_or_else(|| claims.sub.to_string());

    // The next command renews an expired access token with the refresh token,
    // so only a profile without a refresh token is really expired.
    if claims.exp <= now && config.user_refresh_token.is_none() {
        (Some(account), Status::Expired)
    } else {
        (Some(account), Status::SignedIn)
    }
}

fn now_unix() -> u64 {
    tokens::now().timestamp().max(0) as u64
}

impl CliOutput for ProfileRow {
    type TableAlt = ();
    type CellValue = String;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Active", "Name", "Account"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        let account = match (self.account, self.status) {
            (Some(account), Status::SignedIn) => account,
            (Some(account), Status::Expired) => format!("{account} (expired)"),
            (_, Status::LoggedOut) => "logged out".to_string(),
            (_, Status::InvalidToken) => "invalid token".to_string(),
            (None, _) => String::new(),
        };
        vec![
            if self.active { "*" } else { "" }.to_string(),
            self.name,
            account,
        ]
    }
}

impl CliOutput for Active {
    type TableAlt = ();
    type CellValue = String;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Name", "Source"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        vec![self.name, self.source.to_string()]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const NOW: u64 = 1_800_000_000;

    fn access_token(email: Option<&str>, exp: u64) -> String {
        let claims = ControlClaims {
            aud: "authenticated".to_string(),
            iat: exp - 3600,
            exp,
            sub: uuid::Uuid::from_u128(0x1234),
            role: "authenticated".to_string(),
            email: email.map(str::to_string),
            capability_mask: None,
            prefix_mask: None,
        };
        tokens::jwt::sign(claims, &tokens::jwt::EncodingKey::from_secret(b"test")).unwrap()
    }

    fn write_profile(dir: &Path, name: &str, config: serde_json::Value) {
        std::fs::write(
            dir.join(format!("{name}.json")),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn resolve_order() {
        insta::assert_debug_snapshot!(
            [
                resolve(Some("flag"), Some("env"), Some("saved")),
                resolve(None, Some("env"), Some("saved")),
                resolve(None, None, Some("saved")),
                resolve(None, None, None),
            ],
            @r#"
        [
            Active {
                name: "flag",
                source: Flag,
            },
            Active {
                name: "env",
                source: Env,
            },
            Active {
                name: "saved",
                source: Saved,
            },
            Active {
                name: "default",
                source: Default,
            },
        ]
        "#
        );
    }

    #[test]
    fn list_profiles() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let refresh = serde_json::json!({"id": "0101010101010101", "secret": "secret"});
        write_profile(
            dir,
            "default",
            serde_json::json!({
                "user_access_token": access_token(Some("alice@example.com"), NOW + 60),
                "user_refresh_token": refresh,
            }),
        );
        // Expired, but the refresh token renews it.
        write_profile(
            dir,
            "renews",
            serde_json::json!({
                "user_access_token": access_token(Some("bob@example.com"), NOW - 60),
                "user_refresh_token": refresh,
            }),
        );
        write_profile(
            dir,
            "expired",
            serde_json::json!({
                "user_access_token": access_token(Some("carol@example.com"), NOW - 60),
            }),
        );
        write_profile(
            dir,
            "no-email",
            serde_json::json!({"user_access_token": access_token(None, NOW + 60)}),
        );
        write_profile(dir, "logged-out", serde_json::json!({}));
        write_profile(
            dir,
            "garbled",
            serde_json::json!({"user_access_token": "not-a-jwt"}),
        );
        // Not profiles: another extension, a temp file of `Config::write`,
        // and a directory.
        std::fs::write(dir.join("notes.txt"), "").unwrap();
        std::fs::write(dir.join(".tmpAbC123"), "").unwrap();
        std::fs::create_dir(dir.join("subdir.json")).unwrap();

        let rows = list(dir, "renews", NOW).unwrap();
        insta::assert_debug_snapshot!(rows, @r#"
        [
            ProfileRow {
                name: "default",
                active: false,
                account: Some(
                    "alice@example.com",
                ),
                status: SignedIn,
            },
            ProfileRow {
                name: "expired",
                active: false,
                account: Some(
                    "carol@example.com",
                ),
                status: Expired,
            },
            ProfileRow {
                name: "garbled",
                active: false,
                account: None,
                status: InvalidToken,
            },
            ProfileRow {
                name: "logged-out",
                active: false,
                account: None,
                status: LoggedOut,
            },
            ProfileRow {
                name: "no-email",
                active: false,
                account: Some(
                    "00000000-0000-0000-0000-000000001234",
                ),
                status: SignedIn,
            },
            ProfileRow {
                name: "renews",
                active: true,
                account: Some(
                    "bob@example.com",
                ),
                status: SignedIn,
            },
        ]
        "#);
    }

    #[test]
    fn use_saves_an_existing_profile() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();
        write_profile(dir, "work", serde_json::json!({}));

        assert_eq!(read_saved(dir).unwrap(), None);
        save_active(dir, "work").unwrap();
        assert_eq!(read_saved(dir).unwrap().as_deref(), Some("work"));

        // The saved name is not a profile.
        assert_eq!(profile_names(dir).unwrap(), vec!["work".to_string()]);
    }

    #[test]
    fn use_rejects_missing_and_invalid_names() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();
        write_profile(dir, "work", serde_json::json!({}));

        let errors: Vec<String> = ["missing", "", "../work", ".hidden", "a/b", "a\\b"]
            .into_iter()
            .map(|name| format!("{:#}", save_active(dir, name).unwrap_err()))
            .collect();
        insta::assert_debug_snapshot!(errors, @r#"
        [
            "profile \"missing\" does not exist. Run `flowctl profile list` to see profiles, or `flowctl --profile missing auth login` to create it",
            "invalid profile name \"\"",
            "invalid profile name \"../work\"",
            "invalid profile name \".hidden\"",
            "invalid profile name \"a/b\"",
            "invalid profile name \"a\\\\b\"",
        ]
        "#);
        assert_eq!(read_saved(dir).unwrap(), None);
    }

    #[test]
    fn delete_checks() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();
        for name in ["default", "work", "saved"] {
            write_profile(dir, name, serde_json::json!({}));
        }
        save_active(dir, "saved").unwrap();

        // `--profile default` hides the saved profile.
        let active = resolve(Some("default"), None, Some("saved"));
        let results: Vec<String> = ["work", "default", "saved", "missing", "../work"]
            .into_iter()
            .map(|name| match deletable(dir, name, &active) {
                Ok(path) => format!("ok: {}", path.strip_prefix(dir).unwrap().display()),
                Err(err) => format!("{err:#}"),
            })
            .collect();
        insta::assert_debug_snapshot!(results, @r#"
        [
            "ok: work.json",
            "profile \"default\" is the active profile (--profile flag). Switch to another profile first",
            "profile \"saved\" is saved by `flowctl profile use`. Run `flowctl profile use` with another profile first",
            "profile \"missing\" does not exist. Run `flowctl profile list` to see profiles, or `flowctl --profile missing auth login` to create it",
            "invalid profile name \"../work\"",
        ]
        "#);
    }

    #[test]
    fn list_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let rows = list(&dir.path().join("missing"), "default", NOW).unwrap();
        assert!(rows.is_empty());
    }
}
