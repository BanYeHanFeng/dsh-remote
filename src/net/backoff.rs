use std::time::Duration;

pub struct Backoff {
    base: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            current: base,
        }
    }

    pub fn reset(&mut self) {
        self.current = self.base;
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        let jitter_cap = (delay.as_millis() / 5) as u64;
        let jitter = if jitter_cap == 0 {
            0
        } else {
            rand::random_range(0..=jitter_cap)
        };
        self.current = (self.current * 2).min(self.max);
        delay + Duration::from_millis(jitter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_and_caps() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
        for _ in 0..10 {
            let delay = backoff.next_delay();
            assert!(delay >= Duration::from_millis(100));
            assert!(delay <= Duration::from_millis(1200));
        }
    }

    #[test]
    fn reset_restores_base() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();
        let delay = backoff.next_delay();
        assert!(delay >= Duration::from_millis(100));
        assert!(delay <= Duration::from_millis(120));
    }
}
