/// Compute a delta encoding that transforms `prev` into `current`.
///
/// Returns `(truncate_count, suffix)`: how many bytes to remove from the end
/// of `prev`, then the suffix to append, to produce `current`.
///
/// The split always falls on a valid UTF-8 code-point boundary. In the common
/// case of ASCII strings this adds no overhead; for multi-byte characters the
/// byte-level prefix is adjusted back by at most 3 bytes.
pub fn encode<'a>(prev: &str, current: &'a str) -> (i32, &'a str) {
    // Fast path: identical strings are the most common case (consecutive
    // enqueues from the same journal).
    if prev == current {
        return (0, "");
    }

    // Find the common prefix length in bytes.
    let mut common = prev
        .as_bytes()
        .iter()
        .zip(current.as_bytes())
        .take_while(|(a, b)| a == b)
        .count();

    // The byte-level prefix may split a multi-byte code point.
    // Back up (at most 3 bytes) to a character boundary.
    while !current.is_char_boundary(common) {
        common -= 1;
    }

    let truncate = (prev.len() - common) as i32;
    let suffix = &current[common..];

    (truncate, suffix)
}

/// Apply a delta encoding to reconstruct a string in place.
///
/// Removes `truncate` bytes from the end of `value`, then appends `suffix`.
/// A negative `truncate` (conventionally -1) discards the previous value
/// entirely: `suffix` becomes the complete new value. This allows senders
/// to encode a full name without tracking the receiver's prior state.
pub fn decode(value: &mut String, truncate: i32, suffix: &str) {
    let new_len = value.len().saturating_sub(truncate as usize);
    value.truncate(new_len);
    value.push_str(suffix);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode then decode a sequence of strings, verifying each round-trips correctly.
    fn assert_round_trips(sequence: &[&str]) {
        let mut prev = String::new();
        let mut decoded = String::new();

        for &current in sequence {
            let (truncate, suffix) = encode(&prev, current);
            decode(&mut decoded, truncate, suffix);
            assert_eq!(
                decoded, current,
                "round-trip failed: prev={prev:?} current={current:?} truncate={truncate} suffix={suffix:?}"
            );
            prev = current.to_string();
        }
    }

    #[test]
    fn test_identical_strings() {
        // The fast-path: identical strings produce (0, "").
        assert_eq!(encode("foo/bar", "foo/bar"), (0, ""));
        assert_eq!(encode("", ""), (0, ""));
    }

    #[test]
    fn test_no_common_prefix() {
        assert_eq!(encode("abc", "xyz"), (3, "xyz"));
        assert_eq!(encode("", "abc"), (0, "abc"));
        assert_eq!(encode("abc", ""), (3, ""));
    }

    #[test]
    fn test_partial_prefix() {
        assert_eq!(encode("foo/aaa", "foo/bbb"), (3, "bbb"));
        assert_eq!(encode("foo/bar", "foo/baz"), (1, "z"));
    }

    #[test]
    fn test_one_is_prefix_of_other() {
        assert_eq!(encode("foo", "foo/bar"), (0, "/bar"));
        assert_eq!(encode("foo/bar", "foo"), (4, ""));
    }

    #[test]
    fn test_journal_name_sequences() {
        assert_round_trips(&[
            "estuary/tenants/acme/collections/orders/pivot=00",
            "estuary/tenants/acme/collections/orders/pivot=00",
            "estuary/tenants/acme/collections/orders/pivot=01",
            "estuary/tenants/acme/collections/users/pivot=00",
            "estuary/tenants/other/collections/events/pivot=00",
        ]);
    }

    #[test]
    fn test_utf8_2byte_latin() {
        // 'é' is 2 bytes (0xC3 0xA9). Strings that diverge mid-code-point
        // must back up so the split is on a character boundary.
        assert_round_trips(&["café/menu/drinks", "café/menu/food", "café/staff"]);
    }

    #[test]
    fn test_utf8_3byte_cjk() {
        assert_round_trips(&["日本/東京/渋谷", "日本/東京/新宿", "日本/大阪"]);
    }

    #[test]
    fn test_utf8_4byte_emoji() {
        assert_round_trips(&["music/🎵/classical", "music/🎵/jazz", "music/🎸/rock"]);
    }

    #[test]
    fn test_utf8_diverge_at_multibyte_boundary() {
        // 'α' (U+03B1) and 'β' (U+03B2) are 2-byte sequences that share
        // their leading byte (0xCE). The byte-level prefix includes this
        // shared leading byte, but the fixup must back up to avoid splitting.
        let (truncate, suffix) = encode("test/α", "test/β");
        assert_eq!(&"test/α"[.."test/α".len() - truncate as usize], "test/");
        assert_eq!(suffix, "β");

        assert_round_trips(&["test/α", "test/β", "test/γ"]);
    }

    #[test]
    fn test_utf8_prefix_is_multibyte() {
        // Common prefix ends exactly at a multi-byte character.
        assert_round_trips(&["données_a", "données_b"]);
    }

    #[test]
    fn test_decode_from_empty() {
        let mut value = String::new();
        decode(&mut value, 0, "hello");
        assert_eq!(value, "hello");
    }

    #[test]
    fn test_decode_full_replacement() {
        let mut value = "old".to_string();
        decode(&mut value, 3, "new");
        assert_eq!(value, "new");
    }

    #[test]
    fn test_decode_negative_truncate_discards_previous() {
        let mut value = "some/long/previous/journal/name".to_string();
        decode(&mut value, -1, "completely/different/name");
        assert_eq!(value, "completely/different/name");

        // Works from empty too.
        let mut value = String::new();
        decode(&mut value, -1, "fresh/start");
        assert_eq!(value, "fresh/start");

        // Suffix can be empty (clears the value).
        let mut value = "leftover".to_string();
        decode(&mut value, -1, "");
        assert_eq!(value, "");
    }
}
