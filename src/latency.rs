//! LATENCY monitor: per-event latest samples and history.

use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Default)]
pub struct EventStats {
    pub samples: VecDeque<(i64, u64)>,
    pub max: u64,
}

#[derive(Default)]
pub struct Latency {
    pub events: BTreeMap<String, EventStats>,
}

impl Latency {
    pub fn add(&mut self, event: &str, ms: u64) {
        let now = crate::util::now_ms() / 1000;
        let e = self.events.entry(event.to_string()).or_default();
        if let Some(last) = e.samples.back_mut() {
            if last.0 == now {
                last.1 = last.1.max(ms);
                e.max = e.max.max(ms);
                return;
            }
        }
        e.samples.push_back((now, ms));
        if e.samples.len() > 160 {
            e.samples.pop_front();
        }
        e.max = e.max.max(ms);
    }
    pub fn reset(&mut self, events: &[String]) -> usize {
        if events.is_empty() {
            let n = self.events.len();
            self.events.clear();
            return n;
        }
        events.iter().filter(|e| self.events.remove(e.as_str()).is_some()).count()
    }
}
