//! The engine's allocator: the system's, counting what the process holds, which
//! ends the process at the job's memory limit. An allocator can't
//! return an error that reaches the job, so the runner reads this exit code as
//! RESOURCE_EXHAUSTED instead; a restart resumes from the last checkpoint.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering::Relaxed};

/// The exit code for reaching the limit, which is in `Limits.memory_bytes`'s
/// contract. It's EX_TEMPFAIL: the job can run again.
const EXIT_OVER_LIMIT: i32 = 75;

static HELD: AtomicIsize = AtomicIsize::new(0);
static LIMIT: AtomicUsize = AtomicUsize::new(usize::MAX);
static EXITING: AtomicBool = AtomicBool::new(false);

/// What a thread counts before it adds to HELD: lanes' threads allocating at
/// once would otherwise all write its one cache line. So HELD trails what
/// threads hold by less than this each, and a larger allocation counts at once.
const BATCH: isize = 1 << 20;

thread_local! {
    static PENDING: Cell<isize> = const { Cell::new(0) };
}

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
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            charge(new_size - layout.size());
        } else {
            count(-((layout.size() - new_size) as isize));
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[inline]
fn charge(bytes: usize) {
    count(bytes as isize);
}

/// Adds to the thread's pending count, and to HELD once that's a batch: then
/// what's held is checked against the limit.
#[inline]
fn count(bytes: isize) {
    let due = PENDING.try_with(|p| {
        let n = p.get() + bytes;
        if n.abs() < BATCH {
            p.set(n);
            return 0;
        }
        p.set(0);
        n
    });
    let due = due.unwrap_or(bytes);
    if due != 0
        && HELD.fetch_add(due, Relaxed) + due
            > LIMIT.load(Relaxed).min(isize::MAX as usize) as isize
    {
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
    HELD.load(Relaxed).max(0) as u64
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
