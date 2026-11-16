//! Allocation-tracking global allocator: gives `used_memory` like Redis' zmalloc.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Counting;

static USED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            USED.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        USED.fetch_sub(l.size(), Ordering::Relaxed);
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            USED.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let np = unsafe { System.realloc(p, l, new) };
        if !np.is_null() {
            if new > l.size() {
                USED.fetch_add(new - l.size(), Ordering::Relaxed);
            } else {
                USED.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        np
    }
}

pub fn used_memory() -> usize {
    USED.load(Ordering::Relaxed)
}

/// Resident set size from /proc (Linux), 0 elsewhere.
pub fn rss() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|v| v.parse::<usize>().ok()))
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

pub fn total_system_memory() -> usize {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<usize>().ok()))
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

pub fn human(n: usize) -> String {
    let n = n as f64;
    if n < 1024.0 {
        format!("{}B", n as u64)
    } else if n < 1024.0 * 1024.0 {
        format!("{:.2}K", n / 1024.0)
    } else if n < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2}M", n / 1024.0 / 1024.0)
    } else if n < 1024.0 * 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2}G", n / 1024.0 / 1024.0 / 1024.0)
    } else {
        format!("{:.2}T", n / 1024.0 / 1024.0 / 1024.0 / 1024.0)
    }
}
