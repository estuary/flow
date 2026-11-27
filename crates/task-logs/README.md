# task-logs

Filtered scans of a task's ops logs journal, for log viewer queries: level and time filters, text search, and paging in either direction.

## Entry points

- `scan()` reads the journal in a `Direction`, from an optional offset. It stops upon matching `Limits::lines` or `Limits::line_bytes`, upon reading `Limits::bytes` or `Limits::duration`, or upon passing a time bound of its `Filter`. Its `Page` carries a `resume` offset from which a later scan continues.
- `Filter` selects lines by level, by `ts` bounds, and by text.

## Non-obvious details

- Logs are published outside of transactions (`PublishCommitted`), so every line is committed and scans don't sequence transactions. Lines which a retried append repeats are returned as they appear in the journal.
- A scan plans its reads from `ListFragments`, and uses fragment persist times to skip fragments outside of the Filter's time bounds. A persisted fragment holds only lines appended before it was persisted, and its successor holds only lines appended after it was rolled.
- Persisted fragments are gzip and are decompressed from their beginning. A scan reads each one whole, in one pass, with up to `Limits::concurrency` of them in flight as separate tasks. A read which begins within one pays to decompress its prefix (`Stats::bytes_discarded`).
- Fragments that aren't yet persisted are served by brokers from uncompressed local files. A Forward scan reads them through the write head with one request, and a Backward scan reads them in windows which grow as it continues.
- Matching prefilters the raw bytes of a whole batch, and parses only lines it hits. Text with characters that JSON encoders may escape (`"`, `\`, `<`, `>`, `&`, control characters) can't be found in raw bytes, and neither can non-ASCII text, because the raw search folds only ASCII case. Such text is matched after decoding each line, which is several times slower.
