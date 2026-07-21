// src/scheduler.rs
use crate::go_runtime::Runtime;
use crate::timer;
use crate::stack::{GoroutineStack, StackAllocator};
use crate::channel::BoundedQueue;
use lazy_static::lazy_static;
use log::debug;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

// ============== 优化参数 ==============
const LOCAL_QUEUE_SIZE: usize = 256;
const WORK_STEALING_ATTEMPTS: usize = 2;

// ============= thread_local! ===========
thread_local! {
    static CURRENT_G: RefCell<Option<Arc<G>>> = RefCell::new(None);
    static YIELD_REQUESTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// ============== G (Goroutine) ==============
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GStatus {
    Idle = 0,
    Runnable = 1,
    Running = 2,
    Waiting = 3,
    Dead = 4,
}

/// 支持两种 goroutine 类型：
/// - `Once`: 一次性执行（FnOnce），运行到结束
/// - `Mut`: 可重入执行（FnMut -> bool），返回 true=完成, false=让出
enum GFunc {
    Once(Option<Box<dyn FnOnce() + Send + 'static>>),
    Mut(Box<dyn FnMut() -> bool + Send + 'static>),
}

pub struct G {
    pub id: usize,
    status: AtomicU8,
    func: Mutex<Option<GFunc>>,
    created_at: Instant,
    stack: Option<Arc<GoroutineStack>>,
    stack_used: AtomicUsize,
}

unsafe impl Send for G {}
unsafe impl Sync for G {}

impl G {
    #[inline]
    pub fn new_once<F>(id: usize, f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let stack_allocator = StackAllocator::new();
        let stack = stack_allocator.alloc().ok();
        Runtime::track_goroutine();
        G {
            id,
            status: AtomicU8::new(GStatus::Idle as u8),
            func: Mutex::new(Some(GFunc::Once(Some(Box::new(f))))),
            created_at: Instant::now(),
            stack: stack.map(Arc::new),
            stack_used: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn new_mut<F>(id: usize, f: F) -> Self
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let stack_allocator = StackAllocator::new();
        let stack = stack_allocator.alloc().ok();
        Runtime::track_goroutine();
        G {
            id,
            status: AtomicU8::new(GStatus::Idle as u8),
            func: Mutex::new(Some(GFunc::Mut(Box::new(f)))),
            created_at: Instant::now(),
            stack: stack.map(Arc::new),
            stack_used: AtomicUsize::new(0),
        }
    }

    /// 执行 goroutine。
    /// - Once 型：消费闭包，运行到结束，完成后 untrack。
    /// - Mut 型：调用闭包，返回 true=完成并移除闭包，false=让出（保留闭包）。
    /// 
    /// 使用 catch_unwind 捕获 panic，防止单个 goroutine 崩溃导致整个进程退出。
    #[inline]
    pub fn run(&self) {
        clear_yield_flag();
        let mut guard = self.func.lock();
        if let Some(gfunc) = guard.as_mut() {
            match gfunc {
                GFunc::Once(opt) => {
                    if let Some(f) = opt.take() {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                            f();
                        }));
                        if let Err(e) = result {
                            log::error!("[Goroutine {}] panicked: {:?}", self.id, e);
                        }
                        *guard = None;
                        Runtime::untrack_goroutine();
                    }
                }
                GFunc::Mut(f) => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        f()
                    }));
                    match result {
                        Ok(true) => {
                            // 完成
                            *guard = None;
                            Runtime::untrack_goroutine();
                        }
                        Ok(false) => {
                            // 让出：保留闭包，等待重新调度
                        }
                        Err(e) => {
                            log::error!("[Goroutine {}] panicked: {:?}", self.id, e);
                            *guard = None;
                            Runtime::untrack_goroutine();
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn status(&self) -> GStatus {
        self.status.load(Ordering::Acquire).into()
    }

    #[inline]
    pub fn set_status(&self, status: GStatus) {
        self.status.store(status as u8, Ordering::Release);
    }

    /// 检查 goroutine 是否已完成执行（func 已被消费）
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.func.lock().is_none()
    }

    pub fn check_stack(&self) -> bool {
        if let Some(stack) = &self.stack {
            let _used = self.stack_used.load(Ordering::Relaxed);
            if stack.needs_grow() {
                return false;
            }
        }
        true
    }
}

impl Drop for G {
    fn drop(&mut self) {
        if cfg!(debug_assertions) {
            debug!(
                "[G{}] Dropped (ran for {:?})",
                self.id,
                self.created_at.elapsed()
            );
        }
    }
}

impl From<u8> for GStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0 => GStatus::Idle,
            1 => GStatus::Runnable,
            2 => GStatus::Running,
            3 => GStatus::Waiting,
            4 => GStatus::Dead,
            _ => GStatus::Dead,
        }
    }
}

// ============== P (Processor) ==============
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PStatus {
    Idle = 0,
    Running = 1,
}

pub struct P {
    id: usize,
    status: AtomicU8,
    local_queue: BoundedQueue<Arc<G>>,
    runnext: AtomicPtr<G>,
    work_count: AtomicUsize,
    steals: AtomicUsize,
    park_thread: Mutex<Option<std::thread::Thread>>,
}

impl P {
    pub fn new(id: usize) -> Self {
        P {
            id,
            status: AtomicU8::new(PStatus::Idle as u8),
            local_queue: BoundedQueue::new(LOCAL_QUEUE_SIZE),
            runnext: AtomicPtr::new(ptr::null_mut()),
            work_count: AtomicUsize::new(0),
            steals: AtomicUsize::new(0),
            park_thread: Mutex::new(None),
        }
    }

    #[inline]
    pub fn add_g(&self, g: Arc<G>) {
        let old_ptr = self
            .runnext
            .swap(Arc::into_raw(g.clone()) as *mut _, Ordering::Release);
        if !old_ptr.is_null() {
            let old_g = unsafe { Arc::from_raw(old_ptr) };
            if self.local_queue.push(old_g.clone()).is_err() {
                Scheduler::push_global_batch(&[old_g]);
            }
        }
        self.work_count.fetch_add(1, Ordering::Relaxed);
        if let Some(thread) = self.park_thread.lock().as_ref() {
            thread.unpark();
        }
    }

    #[inline]
    pub fn pop_g(&self) -> Option<Arc<G>> {
        let runnext_ptr = self.runnext.swap(ptr::null_mut(), Ordering::AcqRel);
        if !runnext_ptr.is_null() {
            let g = unsafe { Arc::from_raw(runnext_ptr) };
            self.work_count.fetch_sub(1, Ordering::Relaxed);
            return Some(g);
        }

        let g = self.local_queue.pop();
        if let Some(ref _g) = g {
            self.work_count.fetch_sub(1, Ordering::Relaxed);
        }
        g
    }

    #[inline]
    pub fn steal_work(&self) -> Vec<Arc<G>> {
        let len = self.local_queue.len();
        if len <= 1 {
            return Vec::new();
        }

        let steal_count = len / 3;
        let mut stolen = Vec::with_capacity(steal_count);

        for _ in 0..steal_count {
            if let Some(g) = self.local_queue.pop() {
                stolen.push(g);
            } else {
                break;
            }
        }

        if !stolen.is_empty() {
            self.work_count.fetch_sub(stolen.len(), Ordering::Relaxed);
            self.steals.fetch_add(1, Ordering::Relaxed);
        }
        stolen
    }

    #[inline]
    pub fn status(&self) -> PStatus {
        self.status.load(Ordering::Acquire).into()
    }

    #[inline]
    pub fn set_status(&self, status: PStatus) {
        self.status.store(status as u8, Ordering::Release);
    }

    #[inline]
    pub fn work_count(&self) -> usize {
        self.work_count.load(Ordering::Relaxed)
    }

    pub fn steal_count(&self) -> usize {
        self.steals.load(Ordering::Relaxed)
    }
}

impl From<u8> for PStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0 => PStatus::Idle,
            1 => PStatus::Running,
            _ => PStatus::Idle,
        }
    }
}

// ============== 全局调度器 ==============
lazy_static! {
    static ref SCHEDULER: Scheduler = Scheduler::new();
}

pub struct Scheduler {
    global_queue: Mutex<Vec<Arc<G>>>,
    processors: Vec<Arc<P>>,
    next_g_id: AtomicUsize,
    running: AtomicBool,
}

impl Scheduler {
    fn new() -> Self {
        let p_count = num_cpus::get() * 2 + 4;
        let mut processors = Vec::with_capacity(p_count);

        for i in 0..p_count {
            processors.push(Arc::new(P::new(i)));
        }

        Scheduler {
            global_queue: Mutex::new(Vec::with_capacity(1024)),
            processors,
            next_g_id: AtomicUsize::new(1),
            running: AtomicBool::new(true),
        }
    }

    pub fn init() {
        let p_count = SCHEDULER.processors.len();
        let m_count = p_count;

        debug!("   Starting {} workers (GOMAXPROCS={})", m_count, num_cpus::get());

        timer::init_timer_thread();
        crate::netpoller::start();

        for i in 0..m_count {
            let p = SCHEDULER.processors[i].clone();

            thread::Builder::new()
                .name(format!("gorust-worker-{}", i))
                .stack_size(512 * 1024) // 512KB 栈
                .spawn(move || {
                    Self::worker_loop(i, p);
                })
                .unwrap();
        }
    }

    fn get_runnable_g(p: &P) -> Option<Arc<G>> {
        if let Some(g) = p.pop_g() {
            return Some(g);
        }

        {
            let mut global = SCHEDULER.global_queue.lock();
            if let Some(g) = global.pop() {
                let batch_size = global.len().min(8);
                for _ in 0..batch_size {
                    if let Some(g_batch) = global.pop() {
                        p.add_g(g_batch);
                    }
                }
                return Some(g);
            }
        }

        for _ in 0..WORK_STEALING_ATTEMPTS {
            for other_p in SCHEDULER.processors.iter() {
                if other_p.id == p.id {
                    continue;
                }

                let stolen = other_p.steal_work();
                if !stolen.is_empty() {
                    for g in stolen {
                        p.add_g(g);
                    }
                    return p.pop_g();
                }
            }
            thread::yield_now();
        }

        None
    }

    pub fn push_global_batch(gs: &[Arc<G>]) {
        let mut global = SCHEDULER.global_queue.lock();
        global.extend_from_slice(gs);
        if let Some(p) = SCHEDULER.processors.first() {
            if let Some(thread) = p.park_thread.lock().as_ref() {
                thread.unpark();
            }
        }
    }

    /// 创建一次性 goroutine（FnOnce），运行到完成自动释放
    pub fn go<F>(f: F) -> Arc<G>
    where
        F: FnOnce() + Send + 'static,
    {
        let id = SCHEDULER.next_g_id.fetch_add(1, Ordering::Relaxed);
        let g = Arc::new(G::new_once(id, f));

        let p_idx = id % SCHEDULER.processors.len();
        SCHEDULER.processors[p_idx].add_g(g.clone());

        g
    }

    /// 创建可让出的 goroutine（FnMut() -> bool），返回 true=完成, false=让出
    pub fn go_task<F>(f: F) -> Arc<G>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let id = SCHEDULER.next_g_id.fetch_add(1, Ordering::Relaxed);
        let g = Arc::new(G::new_mut(id, f));

        let p_idx = id % SCHEDULER.processors.len();
        SCHEDULER.processors[p_idx].add_g(g.clone());

        g
    }

    fn worker_loop(id: usize, p: Arc<P>) {
        if cfg!(debug_assertions) {
            debug!("[Worker {}] Started with P{}", id, p.id);
        }
        p.set_status(PStatus::Running);

        *p.park_thread.lock() = Some(thread::current());

        while SCHEDULER.running.load(Ordering::Relaxed) {
            if let Some(g) = Self::get_runnable_g(&p) {
                Self::set_current_g(Some(g.clone()));

                g.set_status(GStatus::Running);
                if cfg!(debug_assertions) {
                    debug!("[Worker {}] Executing G{}", id, g.id);
                }

                g.run();

                // 检查 func 是否已被消费：Once 型运行完会被消费，Mut 型 yield 时会保留
                if g.is_completed() {
                    g.set_status(GStatus::Dead);
                    if cfg!(debug_assertions) {
                        debug!("[Worker {}] G{} completed", id, g.id);
                    }
                } else {
                    if cfg!(debug_assertions) {
                        let st = g.status();
                        if st == GStatus::Waiting {
                            debug!("[Worker {}] G{} yielded (Waiting)", id, g.id);
                        } else {
                            debug!("[Worker {}] G{} status={:?} (preserved)", id, g.id, st);
                        }
                    }
                }

                Self::set_current_g(None);
            } else {
                thread::park();
            }
        }

        if cfg!(debug_assertions) {
            debug!("[Worker {}] Shutting down", id);
        }
    }

    pub fn shutdown() {
        SCHEDULER.running.store(false, Ordering::Relaxed);
        for p in SCHEDULER.processors.iter() {
            if let Some(thread) = p.park_thread.lock().as_ref() {
                thread.unpark();
            }
        }
        timer::shutdown_timer();
        crate::netpoller::stop();
    }

    pub fn yield_now() {
        thread::yield_now();
    }

    pub fn is_running() -> bool {
        SCHEDULER.running.load(Ordering::Relaxed)
    }

    pub fn pending_goroutines() -> usize {
        let global_size = SCHEDULER.global_queue.lock().len();
        let local_size: usize = SCHEDULER.processors.iter().map(|p| p.local_queue.len()).sum();
        let runnext_count = SCHEDULER.processors.iter().filter(|p| !p.runnext.load(Ordering::Acquire).is_null()).count();
        global_size + local_size + runnext_count
    }

    // ============== Yield 机制 ==============

    /// 获取当前正在执行的 G
    pub fn current_g() -> Option<Arc<G>> {
        CURRENT_G.with(|cell| cell.borrow().clone())
    }

    pub fn set_current_g(g: Option<Arc<G>>) {
        CURRENT_G.with(|cell| *cell.borrow_mut() = g);
    }

    /// 让出当前 goroutine（设置为 Waiting，等待 FD 就绪后被 netpoller 唤醒）
    #[inline]
    pub fn yield_goroutine() {
        if let Some(g) = CURRENT_G.with(|c| c.borrow().clone()) {
            g.set_status(GStatus::Waiting);
            YIELD_REQUESTED.set(true);
        }
    }

    /// 检查当前 worker 是否被请求让出
    #[inline]
    pub fn is_yield_requested() -> bool {
        YIELD_REQUESTED.get()
    }

    /// 清除让出标志
    #[inline]
    pub fn clear_yield_flag() {
        YIELD_REQUESTED.set(false);
    }

    /// 唤醒一个等待中的 goroutine（被 netpoller 回调调用）
    pub fn wake_g(g: Arc<G>) {
        if g.status() == GStatus::Dead {
            if cfg!(debug_assertions) {
                log::debug!("[Scheduler] Attempted to wake dead G{}", g.id);
            }
            return;
        }

        g.set_status(GStatus::Runnable);

        let p_idx = g.id % SCHEDULER.processors.len();
        SCHEDULER.processors[p_idx].add_g(g);
    }
}

// ============== 公共 API ==============
pub fn go<F>(f: F) -> Arc<G>
where
    F: FnOnce() + Send + 'static,
{
    Scheduler::go(f)
}

pub fn go_task<F>(f: F) -> Arc<G>
where
    F: FnMut() -> bool + Send + 'static,
{
    Scheduler::go_task(f)
}

pub fn yield_now() {
    Scheduler::yield_now()
}

pub fn shutdown() {
    Scheduler::shutdown()
}

pub fn current_g() -> Option<Arc<G>> {
    Scheduler::current_g()
}

pub fn yield_goroutine() {
    Scheduler::yield_goroutine()
}

pub fn is_yield_requested() -> bool {
    Scheduler::is_yield_requested()
}

pub fn clear_yield_flag() {
    Scheduler::clear_yield_flag()
}

pub fn pending_goroutines() -> usize {
    Scheduler::pending_goroutines()
}

pub fn wake_g(g: Arc<G>) {
    Scheduler::wake_g(g)
}