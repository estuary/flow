/// What blocks the Slice's send loop on a Log.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Reason {
    /// The window of quota is full, awaiting the release of this Log.
    Quota,
    /// The Log's channel lacks capacity for a next round.
    Capacity,
}

/// Tracker accrues the time the Slice's send loop is blocked on each Log,
/// for periodic report as an event rather than as metrics: a series for every
/// (Slice, Log) pair would be too many.
///
/// The send loop unblocks at the start of each round and blocks again at its
/// end, so an episode spans the consecutive rounds blocked on one Log for one
/// reason.
pub struct Tracker {
    /// Log blocking the send loop, why, and since when.
    current: Option<(std::time::Instant, usize, Reason)>,
    /// Log and reason which blocked the round just begun, if any.
    prev: Option<(usize, Reason)>,
    /// Microseconds blocked on each Log's release of quota since the last report.
    quota_micros: Vec<u64>,
    /// Microseconds blocked on each Log's channel capacity since the last report.
    capacity_micros: Vec<u64>,
    /// Episodes begun since the last report.
    episodes: u64,
}

/// Time the send loop was blocked on Logs over a report interval.
#[derive(Debug)]
pub struct Report {
    /// Microseconds blocked on Logs' release of quota.
    pub quota_micros: u64,
    /// Microseconds blocked on Log channel capacity.
    pub capacity_micros: u64,
    /// Episodes begun over the interval.
    pub episodes: u64,
    /// The Log most blocked on, and its microseconds blocked.
    pub log: usize,
    pub log_micros: u64,
}

/// Take a report from `blocked`, and emit it as an event of the "blocked" track.
pub fn emit_report(blocked: &mut Tracker) {
    let Report {
        quota_micros,
        capacity_micros,
        episodes,
        log,
        log_micros,
    } = blocked.report(std::time::Instant::now());

    service_kit::event!(
        tracing::Level::DEBUG,
        "blocked",
        quota_micros,
        capacity_micros,
        episodes,
        log,
        log_micros,
        "blocked on Log quota or channel capacity",
    );
}

impl Tracker {
    pub fn new(num_logs: usize) -> Self {
        Self {
            current: None,
            prev: None,
            quota_micros: vec![0; num_logs],
            capacity_micros: vec![0; num_logs],
            episodes: 0,
        }
    }

    /// Unblock as a round begins, accruing the time since the send loop blocked.
    pub fn unblock(&mut self, now: std::time::Instant) {
        self.prev = self.current.take().map(|(since, log, reason)| {
            self.accrue(now, since, log, reason);
            (log, reason)
        });
    }

    /// Block on `log` for `reason` as a round ends.
    pub fn block(&mut self, now: std::time::Instant, log: usize, reason: Reason) {
        if self.prev != Some((log, reason)) {
            self.episodes += 1;
        }
        self.current = Some((now, log, reason));
    }

    /// Take a report of the time blocked since the last, which is zeroed if
    /// the send loop wasn't blocked. A current episode is accrued through `now`
    /// and continues.
    pub fn report(&mut self, now: std::time::Instant) -> Report {
        if let Some((since, log, reason)) = &mut self.current {
            let (from, log, reason) = (*since, *log, *reason);
            *since = now;
            self.accrue(now, from, log, reason);
        }

        let quota_micros: u64 = self.quota_micros.iter().sum();
        let capacity_micros: u64 = self.capacity_micros.iter().sum();

        // Ties go to the least Log, so that a zeroed report names Log 0.
        let (log, log_micros) = std::iter::zip(&self.quota_micros, &self.capacity_micros)
            .map(|(w, c)| w + c)
            .enumerate()
            .max_by_key(|&(log, micros)| (micros, std::cmp::Reverse(log)))
            .unwrap();

        let report = Report {
            quota_micros,
            capacity_micros,
            episodes: self.episodes,
            log,
            log_micros,
        };
        self.quota_micros.fill(0);
        self.capacity_micros.fill(0);
        self.episodes = 0;

        report
    }

    fn accrue(
        &mut self,
        now: std::time::Instant,
        since: std::time::Instant,
        log: usize,
        reason: Reason,
    ) {
        let micros = now.duration_since(since).as_micros() as u64;
        match reason {
            Reason::Quota => self.quota_micros[log] += micros,
            Reason::Capacity => self.capacity_micros[log] += micros,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tracker() {
        let t0 = std::time::Instant::now();
        let at = |ms: u64| t0 + std::time::Duration::from_millis(ms);
        let mut b = Tracker::new(3);
        let mut trace = Vec::new();

        // Never blocked: a zeroed report.
        b.unblock(at(0));
        trace.push(format!("never blocked: {:?}", b.report(at(1))));

        // One episode on Log 1's quota, spanning three rounds.
        b.block(at(10), 1, Reason::Quota);
        b.unblock(at(20));
        b.block(at(25), 1, Reason::Quota);
        b.unblock(at(40));
        b.block(at(40), 1, Reason::Quota);
        b.unblock(at(50));
        // Then an episode on Log 2's capacity.
        b.block(at(60), 2, Reason::Capacity);
        b.unblock(at(65));
        // And a second episode on Log 1, after an unblocked round.
        b.unblock(at(70));
        b.block(at(70), 1, Reason::Quota);
        b.unblock(at(75));
        trace.push(format!("episodes: {:?}", b.report(at(80))));

        // A current episode accrues through each report, and continues.
        b.block(at(100), 0, Reason::Capacity);
        trace.push(format!("current: {:?}", b.report(at(130))));
        trace.push(format!("continued: {:?}", b.report(at(150))));
        b.unblock(at(155));
        trace.push(format!("ended: {:?}", b.report(at(160))));
        trace.push(format!("after: {:?}", b.report(at(170))));

        insta::assert_snapshot!(trace.join("\n"));
    }
}
