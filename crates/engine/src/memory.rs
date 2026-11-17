//! The engine's allocator: mimalloc, counting what the process holds, which
//! ends the process at the job's memory limit. An allocator can't
//! return an error that reaches the job, so the runner reads this exit code as
//! RESOURCE_EXHAUSTED instead; a restart resumes from the last checkpoint.
//! Before that, near the limit, stateful operators flush their state's
//! changes early, and fail the job themselves where their state is what
//! doesn't fit: [`pressure`], and [`set_state`] for what the state holds.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering::Relaxed};

use mimalloc::MiMalloc;

/// The exit code for reaching the limit, which is in `Limits.memory_bytes`'s
/// contract. It's EX_TEMPFAIL: the job can run again.
const EXIT_OVER_LIMIT: i32 = 75;

static HELD: AtomicIsize = AtomicIsize::new(0);
static LIMIT: AtomicUsize = AtomicUsize::new(usize::MAX);
static EXITING: AtomicBool = AtomicBool::new(false);
/// What the job's state holds of HELD, as its operators last counted.
static STATE: AtomicUsize = AtomicUsize::new(0);

/// What a thread counts before it adds to HELD: lanes' threads allocating at
/// once would otherwise all write its one cache line. So HELD trails what
/// threads hold by less than this each, and a larger allocation counts at once.
const BATCH: isize = 1 << 20;

thread_local! {
    static PENDING: Cell<isize> = const { Cell::new(0) };
}

pub struct Counting;

// SAFETY: every call goes to MiMalloc with the same arguments; the counters only
// watch.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { MiMalloc.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { MiMalloc.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { MiMalloc.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            charge(new_size - layout.size());
        } else {
            count(-((layout.size() - new_size) as isize));
        }
        unsafe { MiMalloc.realloc(ptr, layout, new_size) }
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
    // No allocation: a line of a fixed size, on the stack.
    let mut line = [0u8; 160];
    let mut w = std::io::Cursor::new(&mut line[..]);
    let _ = writeln!(
        w,
        "{{\"level\":\"ERROR\",\"message\":\"the engine reached its memory limit, with {} MiB of it the state's\"}}",
        STATE.load(Relaxed) >> 20
    );
    let n = usize::try_from(w.position()).unwrap_or(0);
    let _ = std::io::stderr().write_all(&line[..n]);
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

/// The limit, if there's one.
pub fn limit() -> Option<u64> {
    match LIMIT.load(Relaxed) {
        usize::MAX => None,
        n => Some(n as u64),
    }
}

/// Whether the process holds more than seven eighths of its limit: what
/// can be released should be, before the limit ends it.
pub fn pressure() -> bool {
    limit().is_some_and(|l| held() > l / 8 * 7)
}

/// Notes what the job's state holds in memory, in bytes.
pub fn set_state(bytes: u64) {
    STATE.store(usize::try_from(bytes).unwrap_or(usize::MAX), Relaxed);
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
