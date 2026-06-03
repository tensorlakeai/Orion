use std::cmp::Ordering;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
}

impl HybridTimestamp {
    pub fn now() -> Self {
        Self {
            physical_ms: current_time_millis(),
            logical: 0,
        }
    }
}

impl Ord for HybridTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.physical_ms
            .cmp(&other.physical_ms)
            .then_with(|| self.logical.cmp(&other.logical))
    }
}

impl PartialOrd for HybridTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
pub struct HybridClock {
    last: Mutex<HybridTimestamp>,
}

impl HybridClock {
    pub fn global() -> &'static Self {
        static CLOCK: OnceLock<HybridClock> = OnceLock::new();
        CLOCK.get_or_init(HybridClock::default)
    }

    pub fn next(&self) -> HybridTimestamp {
        self.next_after(None)
    }

    pub fn next_after(&self, observed: Option<HybridTimestamp>) -> HybridTimestamp {
        let mut last = self.last.lock().expect("hybrid clock mutex poisoned");
        let physical_ms = current_time_millis();
        if let Some(observed) = observed
            && observed > *last
        {
            *last = observed;
        }

        if physical_ms > last.physical_ms {
            *last = HybridTimestamp {
                physical_ms,
                logical: 0,
            };
        } else {
            last.logical = last
                .logical
                .checked_add(1)
                .expect("hybrid clock logical counter overflow");
        }
        *last
    }

    pub fn observe(&self, timestamp: HybridTimestamp) {
        let mut last = self.last.lock().expect("hybrid clock mutex poisoned");
        if timestamp > *last {
            *last = timestamp;
        }
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis()
        .try_into()
        .expect("system clock millis do not fit in u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_clock_is_monotonic_under_rapid_calls() {
        let clock = HybridClock::default();
        let first = clock.next();
        let second = clock.next();
        let third = clock.next();

        assert!(first < second);
        assert!(second < third);
    }

    #[test]
    fn hybrid_clock_moves_past_observed_timestamp() {
        let clock = HybridClock::default();
        let observed = HybridTimestamp {
            physical_ms: current_time_millis() + 60_000,
            logical: 41,
        };

        let next = clock.next_after(Some(observed));

        assert!(next > observed);
        assert_eq!(next.physical_ms, observed.physical_ms);
        assert_eq!(next.logical, observed.logical + 1);
    }

    #[test]
    fn observe_never_moves_clock_backwards() {
        let clock = HybridClock::default();
        let first = clock.next_after(Some(HybridTimestamp {
            physical_ms: current_time_millis() + 60_000,
            logical: 9,
        }));
        clock.observe(HybridTimestamp {
            physical_ms: 1,
            logical: 0,
        });
        let second = clock.next();

        assert!(second > first);
    }
}
