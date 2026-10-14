pub enum Outcome {
    Idle,
    Sleep(std::time::Duration),
}

impl Outcome {
    /// Wait for the earliest pending work, or for a message if both are idle.
    pub fn combine(self, other: Outcome) -> Outcome {
        match (self, other) {
            (Outcome::Idle, other) => other,
            (this, Outcome::Idle) => this,
            (Outcome::Sleep(d1), Outcome::Sleep(d2)) => Outcome::Sleep(d1.min(d2)),
        }
    }

    /// The duration to wait before polling again, or None to wait for a message.
    pub fn wait(&self) -> Option<std::time::Duration> {
        match self {
            Outcome::Idle => None,
            Outcome::Sleep(duration) => Some(*duration),
        }
    }
}
