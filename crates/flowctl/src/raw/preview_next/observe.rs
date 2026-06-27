//! Preview-rendering observer for `flowctl preview`.
//!
//! Installs into runtime-next through its [`runtime_next::ObserverFactory`]
//! seam — the user-facing event channel. It is the observation half of the
//! legacy combined preview opener (the document half is [`super::publish`]):
//!
//! - [`Observer::log`](runtime_next::Observer::log) forwards the connector log
//!   stream to the chosen `flowctl` log handler (stderr JSON or tracing).
//! - [`Observer::applied`](runtime_next::Observer::applied) renders the legacy
//!   `--output-apply` line when enabled.
//! - [`Observer::persist`](runtime_next::Observer::persist) renders the legacy
//!   `--output-state` line(s) when enabled, decoding the connector-state delta
//!   from runtime-next's tab-framed patch wire format.
//!
//! All other event routines keep their crate defaults (tracing to stderr).
//!
//! `--output-state` / `--output-apply` (and `--fixture`) require `--shards 1`,
//! so there is exactly one observer writing to stdout when these render; its
//! whole-line atomic `write_all`s never splice with the publisher's document
//! lines, which runtime-next flushes before each committing `persist`.

use bytes::Bytes;
use std::io::Write as _;

/// [`runtime_next::ObserverFactory`] producing preview-rendering observers. The
/// `log_handler` sinks connector logs; `emit_state` / `emit_apply` gate the
/// `--output-state` / `--output-apply` lines.
#[derive(Clone)]
pub struct PreviewObserverFactory {
    log_handler: fn(&::ops::Log),
    emit_state: bool,
    emit_apply: bool,
}

impl PreviewObserverFactory {
    pub fn new(log_handler: fn(&::ops::Log), emit_state: bool, emit_apply: bool) -> Self {
        Self {
            log_handler,
            emit_state,
            emit_apply,
        }
    }
}

impl runtime_next::ObserverFactory for PreviewObserverFactory {
    type Observer = PreviewObserver;

    fn open(&self, _task_name: &str) -> PreviewObserver {
        PreviewObserver {
            log_handler: self.log_handler,
            emit_state: self.emit_state,
            emit_apply: self.emit_apply,
        }
    }
}

/// Per-session preview observer. Cheap to clone (the connector log pump holds
/// its own handle); all fields are `Copy`.
#[derive(Clone)]
pub struct PreviewObserver {
    log_handler: fn(&::ops::Log),
    emit_state: bool,
    emit_apply: bool,
}

impl runtime_next::Observer for PreviewObserver {
    fn log(&self, log: &::ops::Log) {
        (self.log_handler)(log)
    }

    fn applied(&self, action_description: &str) {
        if self.emit_apply {
            write_line(&applied_line(action_description));
        } else {
            tracing::info!(%action_description, "connector applied");
        }
    }

    fn persist(&self, persist: &runtime_next::proto::Persist) {
        if !self.emit_state {
            // Keep the crate's default reporting: skip persists with no
            // connector-state delta, else log the delta at debug.
            if !persist.connector_patches_json.is_empty() {
                tracing::debug!(
                    patches = %String::from_utf8_lossy(&persist.connector_patches_json),
                    "persisted connector-state delta",
                );
            }
            return;
        }
        if persist.connector_patches_json.is_empty() {
            return;
        }
        write_line(&connector_state_line(&persist.connector_patches_json));
    }
}

/// Encode a `--output-state` line from runtime-next's tab-framed connector-state
/// patch payload: `["connectorState",{"updated":<state>,"mergePatch":<bool>}]\n`.
///
/// The wire form is a JSON array of merge patches; a leading `null` patch marks
/// a full state replacement. The common single-merge-patch case embeds the
/// connector's update document verbatim (byte-for-byte the legacy `flowctl
/// preview` serialization, which connector snapshots pin). A replacement or a
/// reduced multi-patch transaction is rendered as the reduced update document.
fn connector_state_line(connector_patches_json: &Bytes) -> Vec<u8> {
    let patches =
        runtime_next::patches::split_state_patches(connector_patches_json).unwrap_or_default();

    match patches.as_slice() {
        // Common case: a single merge patch is the connector's update document.
        [single] if single.as_ref() != b"null" => connector_state_kv(single, true),
        _ => {
            // Replacement (leading `null`) or multiple reduced patches: apply the
            // patches to an empty base to recover the effective update document.
            let is_replace = patches
                .first()
                .map(|p| p.as_ref() == b"null")
                .unwrap_or(false);
            let reduced =
                runtime_next::patches::apply_state_patches(&Bytes::new(), connector_patches_json)
                    .unwrap_or_else(|_| Bytes::from_static(b"{}"));
            connector_state_kv(&reduced, !is_replace)
        }
    }
}

/// Frame an `updated` document (verbatim bytes) into a `connectorState` line.
/// `<state>` is the legacy `flow::ConnectorState` serialization —
/// `{"updated":<raw>,"mergePatch":true}` with default-valued fields omitted, so
/// an absent update encodes as `{}`. The update bytes are embedded verbatim.
fn connector_state_kv(updated_json: &[u8], merge_patch: bool) -> Vec<u8> {
    let mut line = Vec::with_capacity(updated_json.len() + 64);
    line.extend_from_slice(b"[\"connectorState\",{");
    if !updated_json.is_empty() {
        line.extend_from_slice(b"\"updated\":");
        line.extend_from_slice(updated_json);
    }
    if merge_patch {
        if !updated_json.is_empty() {
            line.push(b',');
        }
        line.extend_from_slice(b"\"mergePatch\":true");
    }
    line.extend_from_slice(b"}]\n");
    line
}

/// Emit the run's final reduced connector state as the legacy `--output-state`
/// final line: `["connectorState",{"updated":<state>}]` (`mergePatch:false`,
/// since this is the whole reduced document, not a patch). An empty / absent
/// final state renders as `["connectorState",{}]`. Called once at run end, after
/// the session loop closes the runtime's RocksDB and flowctl re-reads it.
pub fn emit_final_state(state_json: &[u8]) {
    write_line(&connector_state_kv(state_json, false));
}

/// Encode a `--output-apply` line:
/// `["applied.actionDescription", "<text>"]\n` — byte-for-byte the legacy
/// `flowctl preview` format, including the space after the comma and Rust
/// `{:?}` escaping of the description text.
fn applied_line(action_description: &str) -> Vec<u8> {
    format!("[\"applied.actionDescription\", {action_description:?}]\n").into_bytes()
}

/// Write a complete, newline-terminated output line to stdout as a single atomic
/// `write_all` under the stdout lock, so lines never splice together.
fn write_line(line: &[u8]) {
    std::io::stdout().write_all(line).unwrap();
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn preview_lines_match_legacy_serialization() {
        // `connector_state_kv` must reproduce the legacy `flowctl preview`
        // serialization byte-for-byte: serde of `flow::ConnectorState` with
        // default fields omitted, framed as `["connectorState",<state>]\n`.
        for (updated, merge_patch) in [
            (br#"{"cursor":"abc"}"#.as_slice(), true),
            (br#"{"cursor":"abc"}"#.as_slice(), false),
            (b"".as_slice(), false),
            (b"".as_slice(), true),
        ] {
            let state = proto_flow::flow::ConnectorState {
                updated_json: bytes::Bytes::copy_from_slice(updated),
                merge_patch,
            };
            let legacy = format!(
                "[\"connectorState\",{}]\n",
                serde_json::to_string(&state).unwrap()
            );
            assert_eq!(
                String::from_utf8(connector_state_kv(updated, merge_patch)).unwrap(),
                legacy,
            );
        }

        // A single merge patch (the canonical wire form `[{patch}\t]`) embeds the
        // connector's update document verbatim with `mergePatch:true`.
        assert_eq!(
            String::from_utf8(connector_state_line(&Bytes::from_static(
                b"[{\"cursor\":\"abc\"}\t]"
            )))
            .unwrap(),
            "[\"connectorState\",{\"updated\":{\"cursor\":\"abc\"},\"mergePatch\":true}]\n",
        );

        // `applied_line` matches the legacy format, space after comma included.
        assert_eq!(
            String::from_utf8(applied_line("create table \"foo\"")).unwrap(),
            "[\"applied.actionDescription\", \"create table \\\"foo\\\"\"]\n",
        );
    }
}
