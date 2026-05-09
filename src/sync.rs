use parking_lot::{Condvar, Mutex};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct WaitGroup {
    counter: Arc<AtomicUsize>,
    cond: Arc<(Mutex<()>, Condvar)>,
}

impl WaitGroup {
    pub fn new() -> Self {
        WaitGroup {
            counter: Arc::new(AtomicUsize::new(0)),
            cond: Arc::new((Mutex::new(()), Condvar::new())),
        }
    }

    #[inline]
    pub fn add(&self, delta: usize) {
        self.counter.fetch_add(delta, Ordering::Relaxed);
    }

    #[inline]
    pub fn done(&self) {
        if self.counter.fetch_sub(1, Ordering::Release) == 1 {
            self.cond.1.notify_all();
        }
    }

    pub fn wait(&self) {
        let mut guard = self.cond.0.lock();
        while self.counter.load(Ordering::Acquire) > 0 {
            self.cond.1.wait(&mut guard);
        }
    }

    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut guard = self.cond.0.lock();
        let start = Instant::now();

        while self.counter.load(Ordering::Acquire) > 0 {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return false;
            }
            let remaining = timeout - elapsed;
            // 修复：直接使用 self.cond.1 和 &mut guard
            let result = self.cond.1.wait_for(&mut guard, remaining);
            if result.timed_out() {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

/// 原子计数器（简化版 WaitGroup）
#[derive(Clone)]
pub struct AtomicCounter {
    counter: Arc<AtomicUsize>,
}

impl AtomicCounter {
    pub fn new() -> Self {
        AtomicCounter {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn inc(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn dec(&self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }
}

/// Once 实现（只执行一次）
pub struct Once {
    done: AtomicBool,
}

impl Once {
    pub const fn new() -> Self {
        Once {
            done: AtomicBool::new(false),
        }
    }

    pub fn call_once<F>(&self, f: F)
    where
        F: FnOnce(),
    {
        if self.done.load(Ordering::Acquire) {
            return;
        }

        // 使用 CAS 确保只执行一次
        if self
            .done
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            f();
        }
    }

    pub fn is_completed(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

impl Default for Once {
    fn default() -> Self {
        Self::new()
    }
}

// 线程局部存储（优化性能）
thread_local! {
    static TASK_ID: Cell<usize> = Cell::new(0);
}

pub fn current_task_id() -> usize {
    TASK_ID.with(|id| id.get())
}

// pub(crate) fn set_current_task_id(id: usize) {
//     TASK_ID.with(|task_id| task_id.set(id));
// }
