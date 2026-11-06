use crate::Filter;
use crate::matcher::{Line, Matcher, line_ts};
use futures::StreamExt;
use proto_gazette::broker;
use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

/// Lines from different shards interleave in the journal, and a shard may
/// append its lines a little after their `ts`. Decisions which assume that
/// journal order is `ts` order allow for this much disorder.
const SLACK: chrono::TimeDelta = chrono::TimeDelta::minutes(5);

/// Initial and maximum sizes of the windows that a Backward scan reads from
/// fragments which aren't yet persisted. Brokers serve these from
/// uncompressed local files, so reading at any offset needs no decompression.
const WINDOW_MIN: i64 = 1 << 20;
const WINDOW_MAX: i64 = 1 << 26;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Read from older lines towards newer ones.
    Forward,
    /// Read from newer lines towards older ones.
    Backward,
}

/// Limits of a single scan. A scan stops at whichever is reached first.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum number of lines to return.
    pub lines: usize,
    /// Maximum total bytes of the lines returned.
    pub line_bytes: usize,
    /// Maximum journal bytes to read, including bytes decompressed and
    /// discarded to reach a starting offset within a persisted fragment.
    /// A scan reads at least one fragment or window, even if it's larger.
    pub bytes: u64,
    /// Maximum duration of the scan.
    pub duration: Duration,
    /// Maximum number of persisted fragments to read at once.
    pub concurrency: usize,
}

/// Why a scan stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Stop {
    /// `Limits::lines` or `Limits::line_bytes` were matched.
    Lines,
    /// `Limits::bytes` were read.
    Bytes,
    /// `Limits::duration` elapsed.
    Duration,
    /// The scan passed the `until` (Forward) or `since` (Backward) of its Filter.
    Bound,
    /// A Forward scan read through the journal's write head,
    /// or a Backward scan read through its first fragment.
    End,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct Stats {
    /// Journal bytes which were read and filtered.
    pub bytes_read: u64,
    /// Bytes decompressed and discarded to reach a starting offset
    /// within a persisted fragment.
    pub bytes_discarded: u64,
    /// Number of journal read requests.
    pub reads: u32,
    #[serde(serialize_with = "serialize_millis")]
    pub elapsed: Duration,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Page {
    /// Matched lines, in ascending journal order.
    pub lines: Vec<Line>,
    /// Offset from which a scan in the same direction continues.
    /// A Forward scan resumes reading at it, and a Backward scan
    /// resumes reading below it.
    pub resume: i64,
    /// Approximate `ts` of the line at `resume`, for showing how far a
    /// scan has progressed.
    pub resume_ts: Option<chrono::DateTime<chrono::Utc>>,
    pub stop: Stop,
    pub stats: Stats,
}

/// Scan the logs `journal` in `direction`, starting from offset `from`.
/// A Forward scan without `from` begins with the oldest fragment which may
/// hold lines within the Filter's time bounds, and a Backward scan without
/// `from` begins at the journal's write head.
pub async fn scan(
    client: &gazette::journal::Client,
    journal: &str,
    direction: Direction,
    from: Option<i64>,
    filter: &Filter,
    limits: Limits,
) -> gazette::Result<Page> {
    let started = tokio::time::Instant::now();
    let fragments = list_fragments(client, journal).await?;
    let plan = plan(&fragments, direction, from, filter);

    let ctx = Ctx {
        client,
        journal,
        matcher: Arc::new(Matcher::new(filter)),
        limits,
        deadline: started + limits.duration,
    };
    let mut page = match direction {
        Direction::Forward => forward(&ctx, plan).await?,
        Direction::Backward => backward(&ctx, plan).await?,
    };
    page.stats.elapsed = started.elapsed();

    Ok(page)
}

/// Inputs which are shared by the reads of one scan.
struct Ctx<'a> {
    client: &'a gazette::journal::Client,
    journal: &'a str,
    matcher: Arc<Matcher>,
    limits: Limits,
    deadline: tokio::time::Instant,
}

/// Journal content [begin, end) which a scan reads with one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    begin: i64,
    /// End offset, or zero to read through the journal's write head.
    end: i64,
    /// Begin offset of the fragment holding `begin`. A read from `begin`
    /// decompresses a persisted fragment from here.
    decode_from: i64,
}

/// The fragments that a scan reads, in the order it reads them.
#[derive(Debug, Default, PartialEq)]
struct Plan {
    /// Offset at which the scan begins: the start offset of a Forward
    /// scan, or the exclusive upper bound of a Backward scan.
    from: i64,
    /// Content of persisted fragments, which are read concurrently.
    persisted: Vec<Span>,
    /// Content of fragments which aren't yet persisted. A Forward scan reads
    /// it after `persisted`, and a Backward scan reads it before.
    unpersisted: Option<Span>,
    /// Planning stopped at a time bound of the Filter.
    bounded: bool,
}

/// Plan the reads of a scan from a listing of the journal's fragments.
///
/// A persisted fragment holds only lines appended before it was persisted.
/// A fragment's lines were appended after its predecessors were rolled,
/// which is shortly before they were persisted. Planning uses these
/// facts to skip fragments outside of the Filter's time bounds.
fn plan(
    fragments: &[broker::Fragment],
    direction: Direction,
    from: Option<i64>,
    filter: &Filter,
) -> Plan {
    // `appended_after[i]` is the latest persist time of a fragment preceding `fragments[i]`.
    let appended_after: Vec<i64> = fragments
        .iter()
        .scan(0, |latest, fragment| {
            let prior = *latest;
            *latest = prior.max(fragment.mod_time);
            Some(prior)
        })
        .collect();

    // Are all lines of a fragment newer than `until`? Allow for the time
    // taken to persist its predecessor, as well as append delays.
    let after_until = |appended_after: i64| matches!(filter.until, Some(until) if appended_after > (until + SLACK * 2).timestamp());
    let before_since = |fragment: &broker::Fragment| {
        matches!(filter.since, Some(since)
            if fragment.mod_time != 0 && fragment.mod_time < (since - SLACK).timestamp())
    };

    match direction {
        Direction::Forward => {
            let start = from.unwrap_or_default();
            let mut plan = Plan {
                from: start,
                ..Default::default()
            };
            let mut live_begin = start;

            for (fragment, &appended_after) in fragments.iter().zip(&appended_after) {
                if fragment.end <= start {
                    continue;
                } else if after_until(appended_after) {
                    plan.bounded = true;
                    return plan;
                } else if fragment.mod_time == 0 {
                    break; // Fragments which aren't persisted are read together, below.
                }
                live_begin = live_begin.max(fragment.end);

                if !before_since(fragment) {
                    plan.persisted.push(Span {
                        begin: fragment.begin.max(start),
                        end: fragment.end,
                        decode_from: fragment.begin,
                    });
                }
            }
            // Read everything else through the write head with one request,
            // which also covers content appended after the listing.
            plan.unpersisted = Some(Span {
                begin: live_begin,
                end: 0,
                decode_from: live_begin,
            });
            plan
        }
        Direction::Backward => {
            let upper = from.unwrap_or_else(|| fragments.last().map_or(0, |f| f.end));
            let mut plan = Plan {
                from: upper,
                ..Default::default()
            };

            for (fragment, &appended_after) in fragments.iter().zip(&appended_after).rev() {
                if fragment.begin >= upper || after_until(appended_after) {
                    continue;
                } else if before_since(fragment) {
                    plan.bounded = true;
                    break;
                }
                let end = fragment.end.min(upper);

                if fragment.mod_time != 0 {
                    plan.persisted.push(Span {
                        begin: fragment.begin,
                        end,
                        decode_from: fragment.begin,
                    });
                } else if let Some(span) = &mut plan.unpersisted {
                    // Fragments which aren't persisted are the newest, and contiguous.
                    span.begin = fragment.begin;
                    span.decode_from = fragment.begin;
                } else {
                    plan.unpersisted = Some(Span {
                        begin: fragment.begin,
                        end,
                        decode_from: fragment.begin,
                    });
                }
            }
            plan
        }
    }
}

async fn forward(ctx: &Ctx<'_>, plan: Plan) -> gazette::Result<Page> {
    let mut out = Collected::default();
    let mut stats = Stats::default();
    let mut resume = plan.from;
    let mut resume_ts = None;

    let stop = 'scan: {
        let budgeted = budget(&plan.persisted, 0, ctx.limits.bytes);
        let mut outputs = read_spans(
            ctx,
            &plan.persisted[..budgeted],
            Keep::First(ctx.limits.lines),
        );

        while let Some((_span, output)) = outputs.next().await {
            let output = output?;
            stats.add(&output.stats);

            for line in output.lines {
                let ts = line.ts;
                if let ControlFlow::Break(end) = out.push_back(line, &ctx.limits) {
                    (resume, resume_ts) = (end, Some(ts));
                    break 'scan Stop::Lines;
                }
            }
            (resume, resume_ts) = (output.end, output.last_ts.or(resume_ts));

            // A read which stopped early didn't match the rest of its fragment.
            if let Some(stop) = output.stopped {
                break 'scan stop;
            }
        }
        if budgeted != plan.persisted.len() {
            break 'scan Stop::Bytes;
        }
        let Some(span) = plan.unpersisted else {
            break 'scan if plan.bounded { Stop::Bound } else { Stop::End };
        };

        let spent = stats.bytes_read + stats.bytes_discarded;
        if spent >= ctx.limits.bytes {
            break 'scan Stop::Bytes;
        }
        let output = read_span(
            ctx.client.clone(),
            ctx.journal.to_string(),
            ctx.matcher.clone(),
            SpanRead {
                span: Span {
                    begin: span.begin.max(resume),
                    ..span
                },
                partial: false,
                keep: Keep::First(ctx.limits.lines - out.lines.len()),
                max_bytes: ctx.limits.bytes - spent,
            },
            ctx.deadline,
        )
        .await?;
        stats.add(&output.stats);

        for line in output.lines {
            let ts = line.ts;
            if let ControlFlow::Break(end) = out.push_back(line, &ctx.limits) {
                (resume, resume_ts) = (end, Some(ts));
                break 'scan Stop::Lines;
            }
        }
        (resume, resume_ts) = (output.end.max(resume), output.last_ts.or(resume_ts));

        output.stopped.unwrap_or(Stop::End)
    };

    Ok(Page {
        lines: out.lines.into(),
        resume,
        resume_ts,
        stop,
        stats,
    })
}

async fn backward(ctx: &Ctx<'_>, plan: Plan) -> gazette::Result<Page> {
    let mut out = Collected::default();
    let mut stats = Stats::default();
    let mut resume = plan.from;
    let mut resume_ts = None;

    let (since, _until) = ctx.matcher.bounds();
    let passed_since = |first_ts: Option<chrono::DateTime<chrono::Utc>>| matches!((since, first_ts), (Some(since), Some(ts)) if ts < since - SLACK);

    let stop = 'scan: {
        if let Some(span) = plan.unpersisted {
            // Step backward through windows which grow as the scan continues.
            let mut window = WINDOW_MIN;
            resume = resume.min(span.end);

            while resume > span.begin {
                if tokio::time::Instant::now() >= ctx.deadline {
                    break 'scan Stop::Duration;
                } else if stats.bytes_read >= ctx.limits.bytes {
                    break 'scan Stop::Bytes;
                }
                let begin = (resume - window).max(span.begin);

                let output = read_span(
                    ctx.client.clone(),
                    ctx.journal.to_string(),
                    ctx.matcher.clone(),
                    SpanRead {
                        span: Span {
                            begin,
                            end: resume,
                            decode_from: begin,
                        },
                        partial: begin != span.begin,
                        keep: Keep::Last(ctx.limits.lines),
                        max_bytes: u64::MAX,
                    },
                    ctx.deadline,
                )
                .await?;
                stats.add(&output.stats);

                if output.stopped == Some(Stop::Duration) {
                    break 'scan Stop::Duration;
                } else if output.begin == resume && begin != span.begin {
                    window *= 2; // The window held no line boundary. Widen it.
                    continue;
                }
                resume = if begin == span.begin {
                    span.begin
                } else {
                    output.begin
                };
                resume_ts = output.first_ts.or(resume_ts);
                window = (window * 2).min(WINDOW_MAX);

                if out.prepend(output.lines, &ctx.limits) {
                    (resume, resume_ts) = out.front();
                    break 'scan Stop::Lines;
                } else if passed_since(output.first_ts) {
                    break 'scan Stop::Bound;
                }
            }
        }

        let budgeted = budget(&plan.persisted, stats.bytes_read, ctx.limits.bytes);
        let mut outputs = read_spans(
            ctx,
            &plan.persisted[..budgeted],
            Keep::Last(ctx.limits.lines),
        );

        while let Some((span, output)) = outputs.next().await {
            let output = output?;
            stats.add(&output.stats);

            // A fragment read in part is missing its newest lines.
            if output.stopped == Some(Stop::Duration) {
                break 'scan Stop::Duration;
            }
            (resume, resume_ts) = (span.begin, output.first_ts.or(resume_ts));

            if out.prepend(output.lines, &ctx.limits) {
                (resume, resume_ts) = out.front();
                break 'scan Stop::Lines;
            } else if passed_since(output.first_ts) {
                break 'scan Stop::Bound;
            }
        }
        if budgeted != plan.persisted.len() {
            Stop::Bytes
        } else if plan.bounded {
            Stop::Bound
        } else {
            Stop::End
        }
    };

    Ok(Page {
        lines: out.lines.into(),
        resume,
        resume_ts,
        stop,
        stats,
    })
}

/// Lines collected by a scan, in ascending journal order.
#[derive(Default)]
struct Collected {
    lines: VecDeque<Line>,
    bytes: usize,
}

impl Collected {
    /// Append a line read by a Forward scan. Breaks with the offset that
    /// follows the line, if it reached the scan's Limits.
    fn push_back(&mut self, line: Line, limits: &Limits) -> ControlFlow<i64> {
        let end = line.end;
        self.bytes += (line.end - line.offset) as usize;
        self.lines.push_back(line);

        if self.lines.len() >= limits.lines || self.bytes >= limits.line_bytes {
            ControlFlow::Break(end)
        } else {
            ControlFlow::Continue(())
        }
    }

    /// Prepend ascending lines read by a Backward scan. Returns true if the
    /// scan's Limits were reached, after dropping the oldest lines beyond them.
    fn prepend(&mut self, lines: VecDeque<Line>, limits: &Limits) -> bool {
        for line in lines.into_iter().rev() {
            self.bytes += (line.end - line.offset) as usize;
            self.lines.push_front(line);
        }
        while self.lines.len() > limits.lines
            || (self.bytes > limits.line_bytes && self.lines.len() > 1)
        {
            let line = self.lines.pop_front().unwrap();
            self.bytes -= (line.end - line.offset) as usize;
        }
        self.lines.len() >= limits.lines || self.bytes >= limits.line_bytes
    }

    /// The offset and `ts` of the oldest collected line.
    fn front(&self) -> (i64, Option<chrono::DateTime<chrono::Utc>>) {
        let line = self.lines.front().expect("Limits were reached");
        (line.offset, Some(line.ts))
    }
}

/// The number of leading `spans` that a scan reads, given that `spent`
/// of its byte budget `limit` is used. A scan reads at least one span.
fn budget(spans: &[Span], spent: u64, limit: u64) -> usize {
    let mut total = spent;
    let mut count = 0;

    for span in spans {
        if total >= limit && !(count == 0 && spent == 0) {
            break;
        }
        total += (span.end - span.decode_from) as u64;
        count += 1;
    }
    count
}

/// Which matched lines of a span to keep.
#[derive(Debug, Clone, Copy)]
enum Keep {
    /// Keep the first N, and stop reading once they're matched.
    First(usize),
    /// Keep the last N.
    Last(usize),
}

#[derive(Debug, Clone, Copy)]
struct SpanRead {
    span: Span,
    /// If true, `span.begin` may fall within a line, and content through
    /// the first newline is skipped.
    partial: bool,
    keep: Keep,
    /// Stop after reading this many bytes.
    max_bytes: u64,
}

struct SpanOutput {
    lines: VecDeque<Line>,
    /// Offset of the span's first complete line, or `span.end` if it has none.
    begin: i64,
    /// Offset through which the span was read and matched.
    end: i64,
    first_ts: Option<chrono::DateTime<chrono::Utc>>,
    last_ts: Option<chrono::DateTime<chrono::Utc>>,
    stats: Stats,
    /// Why the read stopped before the end of its span, if it did.
    stopped: Option<Stop>,
}

/// Read the persisted `spans`, with up to `Limits::concurrency` reads in
/// flight, yielding their outputs in the order of `spans`. Each read runs
/// as a task, so that their decompression is spread across threads.
/// Dropping the stream aborts reads still in flight.
fn read_spans(
    ctx: &Ctx<'_>,
    spans: &[Span],
    keep: Keep,
) -> impl futures::Stream<Item = (Span, gazette::Result<SpanOutput>)> + use<> {
    let client = ctx.client.clone();
    let journal = ctx.journal.to_string();
    let matcher = ctx.matcher.clone();
    let deadline = ctx.deadline;

    futures::stream::iter(spans.to_vec())
        .map(move |span| {
            let read = SpanRead {
                span,
                partial: false,
                keep,
                max_bytes: u64::MAX,
            };
            let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(read_span(
                client.clone(),
                journal.clone(),
                matcher.clone(),
                read,
                deadline,
            )));

            async move {
                match task.await {
                    Ok(output) => (span, output),
                    Err(err) => std::panic::resume_unwind(err.into_panic()),
                }
            }
        })
        .buffered(ctx.limits.concurrency.max(1))
}

/// Read and match a span of the journal. The read stops early if it keeps
/// the first N lines and has matched them, if it keeps the first N lines and
/// passes the Filter's `until`, upon reading `max_bytes`, or at `deadline`.
async fn read_span(
    client: gazette::journal::Client,
    journal: String,
    matcher: Arc<Matcher>,
    read: SpanRead,
    deadline: tokio::time::Instant,
) -> gazette::Result<SpanOutput> {
    let SpanRead {
        span,
        partial,
        keep,
        max_bytes,
    } = read;
    let (since, until) = matcher.bounds();

    let mut lines = std::pin::pin!(read_lines(&client, &journal, span.begin, span.end));
    let mut out = SpanOutput {
        lines: VecDeque::new(),
        begin: span.end,
        end: span.begin,
        first_ts: None,
        last_ts: None,
        stats: Stats {
            reads: 1,
            ..Default::default()
        },
        stopped: None,
    };
    let mut skip_partial = partial;
    let mut first = true;

    loop {
        let batch = match tokio::time::timeout_at(deadline, next_batch(lines.as_mut())).await {
            Err(_elapsed) => {
                out.stopped = Some(Stop::Duration);
                break;
            }
            Ok(Err(err)) => return Err(err),
            Ok(Ok(None)) => break,
            Ok(Ok(Some(batch))) => batch,
        };

        // A read which begins within a persisted fragment decompresses
        // and discards its content up to the requested offset.
        if std::mem::take(&mut first) {
            let fragment = lines.fragment();
            if !fragment.backing_store.is_empty() && batch.offset > fragment.begin {
                out.stats.bytes_discarded += (batch.offset - fragment.begin) as u64;
            }
        }
        out.stats.bytes_read += batch.content.len() as u64;
        out.end = batch.offset + batch.content.len() as i64;

        let mut content = &batch.content[..];
        let mut offset = batch.offset;

        if std::mem::take(&mut skip_partial) {
            let skip = memchr::memchr(b'\n', content).map_or(content.len(), |i| i + 1);
            content = &content[skip..];
            offset += skip as i64;
        }
        if content.is_empty() {
            continue;
        }
        if out.first_ts.is_none() {
            out.begin = offset;
            out.first_ts = memchr::memchr(b'\n', content).and_then(|i| line_ts(&content[..i + 1]));
        }
        let last_ts = last_line(content).and_then(line_ts);
        out.last_ts = last_ts.or(out.last_ts);

        // Skip matching a batch whose lines are all before `since`.
        if !matches!((since, last_ts), (Some(since), Some(ts)) if ts < since - SLACK) {
            let flow = matcher.for_each_match(content, offset, |line| match keep {
                Keep::First(n) => {
                    out.lines.push_back(line);
                    if out.lines.len() >= n {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                }
                Keep::Last(n) => {
                    if out.lines.len() == n {
                        out.lines.pop_front();
                    }
                    out.lines.push_back(line);
                    ControlFlow::Continue(())
                }
            });
            if flow.is_break() {
                out.end = out.lines.back().unwrap().end;
                out.stopped = Some(Stop::Lines);
                break;
            }
        }

        // `timeout_at` polls its future before its timer, so it doesn't fire
        // while batches are always ready. Check the deadline here as well.
        if matches!((keep, until, last_ts), (Keep::First(_), Some(until), Some(ts)) if ts >= until + SLACK)
        {
            out.stopped = Some(Stop::Bound);
            break;
        } else if out.stats.bytes_read + out.stats.bytes_discarded >= max_bytes {
            out.stopped = Some(Stop::Bytes);
            break;
        } else if tokio::time::Instant::now() >= deadline {
            out.stopped = Some(Stop::Duration);
            break;
        }
    }
    Ok(out)
}

fn read_lines(
    client: &gazette::journal::Client,
    journal: &str,
    offset: i64,
    end_offset: i64,
) -> gazette::journal::read::ReadLines<
    { 1 << 20 },
    0,
    impl futures::Stream<Item = gazette::RetryResult<broker::ReadResponse>> + use<>,
> {
    let req = broker::ReadRequest {
        journal: journal.to_string(),
        offset,
        end_offset,
        block: false,
        do_not_proxy: true,
        ..Default::default()
    };
    // A non-blocking read which begins at the write head yields a response
    // with neither a fragment nor content. ReadLines doesn't expect one at the
    // start of a read, and it holds nothing to read, so drop such responses.
    let responses = client.clone().read(req).filter(|resp| {
        let empty = matches!(resp, Ok(resp) if resp.fragment.is_none() && resp.content.is_empty());
        std::future::ready(!empty)
    });
    gazette::journal::read::ReadLines::new(responses, 0, false)
}

/// Poll the next batch of lines, retrying transient errors a few times.
async fn next_batch<S>(
    mut lines: std::pin::Pin<&mut gazette::journal::read::ReadLines<{ 1 << 20 }, 0, S>>,
) -> gazette::Result<Option<gazette::journal::read::LinesBatch>>
where
    S: futures::Stream<Item = gazette::RetryResult<broker::ReadResponse>>,
{
    loop {
        match lines.next().await {
            None => return Ok(None),
            Some(Ok(batch)) => return Ok(Some(batch)),
            // A non-blocking read has no content at its offset. This is the end
            // of the journal when its broker's write head trails its persisted
            // fragments, as after all replicas of a journal lose their spool.
            Some(Err(gazette::RetryError {
                inner: gazette::Error::BrokerStatus(broker::Status::OffsetNotYetAvailable),
                ..
            })) => return Ok(None),
            Some(Err(err)) if err.inner.is_transient() && err.attempt < 3 => {
                tracing::warn!(?err, "transient error reading task logs (will retry)");
            }
            Some(Err(err)) => return Err(err.inner),
        }
    }
}

/// List all fragments of `journal` in ascending offset order,
/// including fragments which aren't yet persisted.
async fn list_fragments(
    client: &gazette::journal::Client,
    journal: &str,
) -> gazette::Result<Vec<broker::Fragment>> {
    let mut out = Vec::new();
    let mut next_page_token = 0;

    loop {
        let resp = client
            .list_fragments(broker::FragmentsRequest {
                journal: journal.to_string(),
                next_page_token,
                page_limit: 1000,
                ..Default::default()
            })
            .await?;

        out.extend(resp.fragments.into_iter().filter_map(|f| f.spec));
        next_page_token = resp.next_page_token;

        if next_page_token == 0 {
            return Ok(out);
        }
    }
}

impl Stats {
    fn add(&mut self, other: &Stats) {
        self.bytes_read += other.bytes_read;
        self.bytes_discarded += other.bytes_discarded;
        self.reads += other.reads;
    }
}

/// The last line of newline-aligned `content`.
fn last_line(content: &[u8]) -> Option<&[u8]> {
    let (_, head) = content.split_last()?;
    let begin = memchr::memrchr(b'\n', head).map_or(0, |i| i + 1);
    Some(&content[begin..])
}

fn serialize_millis<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(d.as_millis() as u64)
}

#[cfg(test)]
mod test {
    use super::*;

    fn at(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        Some(chrono::DateTime::parse_from_rfc3339(s).unwrap().to_utc())
    }

    fn span(begin: i64, end: i64, decode_from: i64) -> Span {
        Span {
            begin,
            end,
            decode_from,
        }
    }

    #[test]
    fn test_plan() {
        // Three daily fragments persisted just after midnight, and a spool.
        let fragment = |begin, end, mod_time: &str| broker::Fragment {
            begin,
            end,
            mod_time: at(mod_time).map_or(0, |ts| ts.timestamp()),
            ..Default::default()
        };
        let fragments = vec![
            fragment(0, 100, "2026-01-02T00:01:00Z"),
            fragment(100, 200, "2026-01-03T00:01:00Z"),
            fragment(200, 300, "2026-01-04T00:01:00Z"),
            fragment(300, 350, "1970-01-01T00:00:00Z"),
        ];
        let plan = |direction, from, since: &str, until: &str| {
            let filter = Filter {
                since: (!since.is_empty()).then(|| at(since).unwrap()),
                until: (!until.is_empty()).then(|| at(until).unwrap()),
                ..Default::default()
            };
            super::plan(&fragments, direction, from, &filter)
        };

        // A Forward scan reads persisted fragments, then through the write head.
        assert_eq!(
            plan(Direction::Forward, None, "", ""),
            Plan {
                from: 0,
                persisted: vec![span(0, 100, 0), span(100, 200, 100), span(200, 300, 200)],
                unpersisted: Some(span(300, 0, 300)),
                bounded: false,
            }
        );
        // From a cursor within a fragment, which is decoded from its beginning.
        assert_eq!(
            plan(Direction::Forward, Some(150), "", "").persisted,
            vec![span(150, 200, 100), span(200, 300, 200)],
        );
        // `since` skips fragments persisted before it,
        // and `until` stops before fragments appended after it.
        assert_eq!(
            plan(
                Direction::Forward,
                None,
                "2026-01-02T12:00:00Z",
                "2026-01-02T18:00:00Z"
            ),
            Plan {
                from: 0,
                persisted: vec![span(100, 200, 100)],
                unpersisted: None,
                bounded: true,
            }
        );
        // A Backward scan reads the spool, then persisted fragments newest first.
        assert_eq!(
            plan(Direction::Backward, None, "", ""),
            Plan {
                from: 350,
                persisted: vec![span(200, 300, 200), span(100, 200, 100), span(0, 100, 0)],
                unpersisted: Some(span(300, 350, 300)),
                bounded: false,
            }
        );
        // `until` skips fragments appended after it,
        // and `since` stops at a fragment persisted before it.
        assert_eq!(
            plan(
                Direction::Backward,
                Some(320),
                "2026-01-02T12:00:00Z",
                "2026-01-02T18:00:00Z"
            ),
            Plan {
                from: 320,
                persisted: vec![span(100, 200, 100)],
                unpersisted: None,
                bounded: true,
            }
        );
    }

    #[test]
    fn test_budget() {
        let spans = vec![span(50, 100, 0), span(100, 200, 100), span(200, 300, 200)];

        assert_eq!(budget(&spans, 0, 1), 1); // At least one span is read.
        assert_eq!(budget(&spans, 0, 100), 1);
        assert_eq!(budget(&spans, 0, 101), 2);
        assert_eq!(budget(&spans, 0, 1000), 3);
        assert_eq!(budget(&spans, 10, 5), 0); // Prior reads spent the budget.
    }
}
