pub enum Outcome {
    Idle,
    WaitForRetry(std::time::Duration),
    /// Wait until scheduled work is next due.
    Scheduled(std::time::Duration),
}

impl Outcome {
    /// This function determines the next action given two outcomes.
    /// The following rules apply when combining outcomes.
    ///
    /// A timed outcome (WaitForRetry or Scheduled) is always chosen before
    /// Idle, and the timed outcome with the smallest duration is used.
    pub fn combine(self, other: Outcome) -> Outcome {
        match (self.wait(), other.wait()) {
            (_, None) => self,
            (None, Some(_)) => other,
            (Some(d1), Some(d2)) if d1 <= d2 => self,
            (Some(_), Some(_)) => other,
        }
    }

    /// The duration to wait before polling again, or None to wait for a message.
    pub fn wait(&self) -> Option<std::time::Duration> {
        match self {
            Outcome::Idle => None,
            Outcome::WaitForRetry(duration) | Outcome::Scheduled(duration) => Some(*duration),
        }
    }
}
