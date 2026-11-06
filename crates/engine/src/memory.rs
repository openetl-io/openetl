//! The engine's allocator: the system's, counting what the process holds, which
//! ends the process at the job's memory limit (ADR-0027 §4). An allocator can't
//! return an error that reaches the job, so the runner reads this exit code as
//! RESOURCE_EXHAUSTED instead; a restart resumes from the last checkpoint.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

/// The exit code for reaching the limit, which is in `Limits.memory_bytes`'s
/// contract. It's EX_TEMPFAIL: the job can run again.
const EXIT_OVER_LIMIT: i32 = 75;

static HELD: AtomicUsize = AtomicUsize::new(0);
static LIMIT: AtomicUsize = AtomicUsize::new(usize::MAX);
static EXITING: AtomicBool = AtomicBool::new(false);

pub struct Counting;

// SAFETY: every call goes to System with the same arguments; the counters only
// watch.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        HELD.fetch_sub(layout.size(), Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            charge(new_size - layout.size());
        } else {
            HELD.fetch_sub(layout.size() - new_size, Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[inline]
fn charge(bytes: usize) {
    if HELD.fetch_add(bytes, Relaxed).saturating_add(bytes) > LIMIT.load(Relaxed) {
        over_limit();
    }
}

/// Exiting may allocate, and so may other threads meanwhile, which get through.
#[cold]
fn over_limit() {
    if EXITING.swap(true, Relaxed) {
        return;
    }
    let _ = std::io::stderr()
        .write_all(b"{\"level\":\"ERROR\",\"message\":\"the engine reached its memory limit\"}\n");
    std::process::exit(EXIT_OVER_LIMIT);
}

/// Sets the limit in bytes; zero means none.
pub fn set_limit(bytes: u64) {
    let limit = match usize::try_from(bytes) {
        Ok(0) | Err(_) => usize::MAX,
        Ok(n) => n,
    };
    LIMIT.store(limit, Relaxed);
}

/// How many bytes the process's allocations hold now.
pub fn held() -> u64 {
    HELD.load(Relaxed) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Other tests allocate at the same time, so this leaves them room.
    #[test]
    fn counts_what_is_held() {
        const SIZE: usize = 256 << 20;
        let before = held();
        let mut v: Vec<u8> = Vec::with_capacity(SIZE);
        v.push(1);
        assert!(held() > before + (SIZE as u64) / 2, "{} {before}", held());
        drop(v);
        assert!(held() < before + (SIZE as u64) / 2, "{} {before}", held());
    }
}
