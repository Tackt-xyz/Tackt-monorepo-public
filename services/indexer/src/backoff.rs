//! The one restart-backoff discipline every supervisor in this service uses.
//!
//! Rapid failures double the delay up to a cap; sustained uptime resets it, so a routine
//! disconnect after hours of health resumes at full speed while a flapping dependency is
//! throttled. One implementation, because three subtly different copies of "exponential
//! backoff with uptime reset" is how supervisors silently diverge.

use std::time::Duration;

/// How long a cycle must have run for its failure to count as fresh rather than repeated.
const RESET_UPTIME: Duration = Duration::from_secs(60);

pub struct RestartBackoff {
    start: Duration,
    cap: Duration,
    delay: Duration,
}

impl RestartBackoff {
    #[must_use]
    pub fn new(start: Duration, cap: Duration) -> Self {
        Self {
            start,
            cap,
            delay: start,
        }
    }

    /// The delay to sleep before the next attempt, given how long the failed cycle ran.
    pub fn next_delay(&mut self, ran_for: Duration) -> Duration {
        if ran_for > RESET_UPTIME {
            self.delay = self.start;
        }
        let delay = self.delay;
        self.delay = (delay * 2).min(self.cap);
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_to_the_cap_and_resets_after_sustained_uptime() {
        let mut backoff = RestartBackoff::new(Duration::from_secs(1), Duration::from_secs(4));
        let crash = Duration::from_secs(1);
        assert_eq!(backoff.next_delay(crash), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(crash), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(crash), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(crash), Duration::from_secs(4));
        // A cycle that ran healthily for over a minute starts the ladder over.
        assert_eq!(
            backoff.next_delay(Duration::from_secs(61)),
            Duration::from_secs(1)
        );
        assert_eq!(backoff.next_delay(crash), Duration::from_secs(2));
    }
}
