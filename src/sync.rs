use parking_lot::{Mutex as PLMutex, Condvar};
use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct WaitGroup {
    counter: Arc<AtomicUsize>,
    cond: Arc<(PLMutex<()>, Condvar)>,
}

impl WaitGroup {
    pub fn new() -> Self {
        WaitGroup {
            counter: Arc::new(AtomicUsize::new(0)),
            cond: Arc::new((PLMutex::new(()), Condvar::new())),
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

pub struct Mutex<T: ?Sized> {
    inner: PLMutex<T>,
}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Mutex {
            inner: PLMutex::new(value),
        }
    }

    pub fn lock(&self) -> parking_lot::MutexGuard<'_, T> {
        self.inner.lock()
    }

    pub fn try_lock(&self) -> Option<parking_lot::MutexGuard<'_, T>> {
        self.inner.try_lock()
    }
}

pub struct RWMutex<T: ?Sized> {
    inner: parking_lot::RwLock<T>,
}

impl<T> RWMutex<T> {
    pub fn new(value: T) -> Self {
        RWMutex {
            inner: parking_lot::RwLock::new(value),
        }
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, T> {
        self.inner.read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, T> {
        self.inner.write()
    }

    pub fn try_read(&self) -> Option<parking_lot::RwLockReadGuard<'_, T>> {
        self.inner.try_read()
    }

    pub fn try_write(&self) -> Option<parking_lot::RwLockWriteGuard<'_, T>> {
        self.inner.try_write()
    }
}

pub struct Pool<T> {
    items: Arc<parking_lot::Mutex<VecDeque<T>>>,
    factory: Arc<dyn Fn() -> T + Send + Sync>,
    max_size: usize,
}

impl<T: Send + 'static> Pool<T> {
    pub fn new<F>(factory: F, max_size: usize) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        Pool {
            items: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(max_size))),
            factory: Arc::new(factory),
            max_size,
        }
    }

    pub fn get(&self) -> T {
        let mut items = self.items.lock();
        if let Some(item) = items.pop_front() {
            item
        } else {
            (self.factory)()
        }
    }

    pub fn put(&self, item: T) {
        let mut items = self.items.lock();
        if items.len() < self.max_size {
            items.push_back(item);
        }
    }

    pub fn len(&self) -> usize {
        self.items.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Send + 'static> Clone for Pool<T> {
    fn clone(&self) -> Self {
        Pool {
            items: self.items.clone(),
            factory: self.factory.clone(),
            max_size: self.max_size,
        }
    }
}

#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

struct ContextInner {
    done: AtomicBool,
    deadline: parking_lot::Mutex<Option<Instant>>,
    err: parking_lot::Mutex<Option<String>>,
    children: parking_lot::Mutex<Vec<Arc<parking_lot::Condvar>>>,
}

impl Context {
    pub fn background() -> Self {
        Context {
            inner: Arc::new(ContextInner {
                done: AtomicBool::new(false),
                deadline: parking_lot::Mutex::new(None),
                err: parking_lot::Mutex::new(None),
                children: parking_lot::Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn with_timeout(_parent: Context, timeout: Duration) -> (Context, impl FnOnce()) {
        let deadline = Instant::now() + timeout;
        let child = Context {
            inner: Arc::new(ContextInner {
                done: AtomicBool::new(false),
                deadline: parking_lot::Mutex::new(Some(deadline)),
                err: parking_lot::Mutex::new(None),
                children: parking_lot::Mutex::new(Vec::new()),
            }),
        };

        let child_inner = child.inner.clone();
        let canceller = move || {
            if !child_inner.done.load(Ordering::Acquire) {
                child_inner.done.store(true, Ordering::Release);
                *child_inner.err.lock() = Some("context deadline exceeded".to_string());
            }
        };

        (child, canceller)
    }

    pub fn with_cancel(_parent: Context) -> (Context, impl FnOnce()) {
        let child = Context {
            inner: Arc::new(ContextInner {
                done: AtomicBool::new(false),
                deadline: parking_lot::Mutex::new(None),
                err: parking_lot::Mutex::new(None),
                children: parking_lot::Mutex::new(Vec::new()),
            }),
        };

        let child_inner = child.inner.clone();
        let canceller = move || {
            if !child_inner.done.load(Ordering::Acquire) {
                child_inner.done.store(true, Ordering::Release);
                *child_inner.err.lock() = Some("context canceled".to_string());
            }
        };

        (child, canceller)
    }

    pub fn done(&self) -> bool {
        self.inner.done.load(Ordering::Acquire)
    }

    pub fn err(&self) -> Option<String> {
        self.inner.err.lock().clone()
    }

    pub fn deadline(&self) -> Option<Instant> {
        *self.inner.deadline.lock()
    }

    pub fn is_expired(&self) -> bool {
        if let Some(deadline) = self.deadline() {
            Instant::now() >= deadline
        } else {
            false
        }
    }
}

thread_local! {
    static TASK_ID: Cell<usize> = Cell::new(0);
}

pub fn current_task_id() -> usize {
    TASK_ID.with(|id| id.get())
}
