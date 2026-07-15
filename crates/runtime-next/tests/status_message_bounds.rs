//! Regression coverage for estuary/flow#3177: an oversized error must not
//! produce a `tonic::Status` message that exceeds the h2 trailer limit and
//! gets swallowed as `too_many_continuations`. Errors are truncated (from the
//! tail) so the message stays bounded while preserving the root-cause prefix.

#[test]
fn oversized_error_is_bounded_and_keeps_prefix() {
    // Mimic the causal-hint-timeout error: a real prefix followed by an
    // unbounded per-journal dump.
    let prefix = "causal hint resolution timed out: 1413 hint(s) unresolved across 537 journal(s)";
    let detail = "\n  journal \"acmeCo/collection/pivot=00\" binding=1 producer=... last_commit=... hinted_commit=...".repeat(2000);
    let err = anyhow::anyhow!("{prefix}{detail}");

    let status = runtime_next::anyhow_to_status(err);
    let message = status.message();

    assert!(
        message.len() <= runtime_next::MAX_STATUS_MESSAGE_LEN,
        "message len {} exceeds cap {}",
        message.len(),
        runtime_next::MAX_STATUS_MESSAGE_LEN,
    );
    assert!(
        message.starts_with(prefix),
        "message lost its root-cause prefix: {message:?}",
    );
    assert!(
        message.ends_with("… [truncated]"),
        "truncated message should carry the truncation marker: {message:?}",
    );
}

#[test]
fn oversized_preexisting_status_is_bounded_preserving_code() {
    // A Status that we did not format ourselves — e.g. round-tripped from a
    // misbehaving connector — must also be bounded, and not only when it
    // carries the `Unknown` code (`Unknown` alone is flattened to a string by
    // `status_to_anyhow`; other codes downcast straight back to a Status).
    let huge = "connector went haywire ".repeat(2000);
    let status = tonic::Status::internal(huge);
    let err = runtime_next::status_to_anyhow(status);

    let status = runtime_next::anyhow_to_status(err);

    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().len() <= runtime_next::MAX_STATUS_MESSAGE_LEN);
    assert!(status.message().starts_with("connector went haywire"));
    assert!(status.message().ends_with("… [truncated]"));
}

#[test]
fn small_error_is_not_truncated() {
    let err = anyhow::anyhow!("causal hint resolution timed out: 1 hint(s) unresolved");
    let status = runtime_next::anyhow_to_status(err);
    let message = status.message();

    // Under-budget messages pass through without a truncation marker. (We avoid
    // asserting exact equality because anyhow's `{:?}` appends a backtrace when
    // `RUST_BACKTRACE` is set.)
    assert!(message.starts_with("causal hint resolution timed out: 1 hint(s) unresolved"));
    assert!(!message.ends_with("… [truncated]"));
    assert!(message.len() <= runtime_next::MAX_STATUS_MESSAGE_LEN);
}
