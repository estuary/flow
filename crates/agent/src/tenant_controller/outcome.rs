pub enum Outcome {
    Idle,
    WaitForRetry(std::time::Duration),
}

impl Outcome {
    /// This function determines the next action given two outcomes.
    /// The following rules apply when combining outcomes.
    ///
    /// WaitForRetry is always chosen, before Idle, and the WaitForRetry
    /// with the smallest duration is used.
    pub fn next_action(self, other: Outcome) -> Outcome {
        match (self, other) {
            (Outcome::Idle, Outcome::Idle) => todo!(),
            (Outcome::WaitForRetry(duration), Outcome::Idle)
            | (Outcome::Idle, Outcome::WaitForRetry(duration)) => Self::WaitForRetry(duration),
            (Outcome::WaitForRetry(d1), Outcome::WaitForRetry(d2)) => {
                Self::WaitForRetry(d1.min(d2))
            }
        }
    }
}
