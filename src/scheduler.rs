use crate::go_runtime::Runtime;
use crossbeam::queue::ArrayQueue;
use lazy_static::lazy_static;
use log::debug;
use parking_lot::Mutex;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// ============== 优化参数 ==============
const LOCAL_QUEUE_SIZE: usize = 256;
const WORK_STEALING_ATTEMPTS: usize = 2; // 减少窃取尝试
const MAX_SPIN_ITERATIONS: usize = 100; // 自旋等待次数
const SLEEP_DURATION: Duration = Duration::from_micros(50); // 减少休眠时间

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

pub struct G {
    id: usize,
    status: AtomicU8,
    func: Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
    created_at: Instant,
}

unsafe impl Send for G {}
unsafe impl Sync for G {}

impl G {
    #[inline]
    pub fn new<F>(id: usize, f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Runtime::track_goroutine();
        G {
            id,
            status: AtomicU8::new(GStatus::Idle as u8),
            func: Mutex::new(Some(Box::new(f))),
            created_at: Instant::now(),
        }
    }

    #[inline]
    pub fn run(&self) {
        if let Some(func) = self.func.lock().take() {
            func();
        }
        Runtime::untrack_goroutine();
    }

    #[inline]
    pub fn status(&self) -> GStatus {
        self.status.load(Ordering::Acquire).into()
    }

    #[inline]
    pub fn set_status(&self, status: GStatus) {
        self.status.store(status as u8, Ordering::Release);
    }
}

impl Drop for G {
    fn drop(&mut self) {
        // 只在调试模式打印
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
            1 => GStatus::Idle,
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
    local_queue: ArrayQueue<Arc<G>>,
    runnext: AtomicPtr<G>,
    work_count: AtomicUsize,
    steals: AtomicUsize, // 统计窃取次数
}

impl P {
    pub fn new(id: usize) -> Self {
        P {
            id,
            status: AtomicU8::new(PStatus::Idle as u8),
            local_queue: ArrayQueue::new(LOCAL_QUEUE_SIZE),
            runnext: AtomicPtr::new(ptr::null_mut()),
            work_count: AtomicUsize::new(0),
            steals: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn add_g(&self, g: Arc<G>) {
        // 使用 LIFO 优化缓存局部性
        let old_ptr = self
            .runnext
            .swap(Arc::into_raw(g.clone()) as *mut _, Ordering::Release);
        if !old_ptr.is_null() {
            let old_g = unsafe { Arc::from_raw(old_ptr) };
            // 本地队列满时尝试立即处理
            if self.local_queue.push(old_g.clone()).is_err() {
                Scheduler::push_global_batch(&[old_g]);
            }
        }
        self.work_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn pop_g(&self) -> Option<Arc<G>> {
        // 优先从 runnext 获取
        let runnext_ptr = self.runnext.swap(ptr::null_mut(), Ordering::AcqRel);
        if !runnext_ptr.is_null() {
            let g = unsafe { Arc::from_raw(runnext_ptr) };
            self.work_count.fetch_sub(1, Ordering::Relaxed);
            return Some(g);
        }

        // 从本地队列获取
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

        let steal_count = len / 3; // 窃取 1/3 而不是 1/2
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
    stats: SchedulerStats,
}

struct SchedulerStats {
    total_spins: AtomicUsize,
    total_sleeps: AtomicUsize,
}

impl Scheduler {
    fn new() -> Self {
        let p_count = num_cpus::get();
        let mut processors = Vec::with_capacity(p_count);

        for i in 0..p_count {
            processors.push(Arc::new(P::new(i)));
        }

        Scheduler {
            global_queue: Mutex::new(Vec::with_capacity(1024)),
            processors,
            next_g_id: AtomicUsize::new(1),
            running: AtomicBool::new(true),
            stats: SchedulerStats {
                total_spins: AtomicUsize::new(0),
                total_sleeps: AtomicUsize::new(0),
            },
        }
    }

    pub fn init() {
        let p_count = num_cpus::get();
        let m_count = p_count;

        debug!("   Starting {} workers (GOMAXPROCS={})", m_count, p_count);

        for i in 0..m_count {
            let p = SCHEDULER.processors[i % p_count].clone();

            thread::Builder::new()
                .name(format!("rgo-worker-{}", i))
                .spawn(move || {
                    Self::worker_loop(i, p);
                })
                .unwrap();
        }
    }

    fn worker_loop(id: usize, p: Arc<P>) {
        if cfg!(debug_assertions) {
            debug!("[Worker {}] Started with P{}", id, p.id);
        }
        p.set_status(PStatus::Running);

        let mut spin_count = 0;

        while SCHEDULER.running.load(Ordering::Relaxed) {
            // 尝试获取 G
            if let Some(g) = Self::get_runnable_g(&p) {
                spin_count = 0;

                // 执行 G
                g.set_status(GStatus::Running);
                if cfg!(debug_assertions) {
                    debug!("[Worker {}] Executing G{}", id, g.id);
                }

                g.run();

                g.set_status(GStatus::Dead);
                if cfg!(debug_assertions) {
                    debug!("[Worker {}] G{} completed", id, g.id);
                }
            } else {
                // 自适应休眠策略
                if spin_count < MAX_SPIN_ITERATIONS {
                    spin_count += 1;
                    SCHEDULER.stats.total_spins.fetch_add(1, Ordering::Relaxed);
                    thread::yield_now();
                } else {
                    SCHEDULER.stats.total_sleeps.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(SLEEP_DURATION);
                }
            }
        }

        if cfg!(debug_assertions) {
            debug!(
                "[Worker {}] Shutting down (steals: {})",
                id,
                p.steal_count()
            );
        }
    }

    #[inline]
    fn get_runnable_g(p: &P) -> Option<Arc<G>> {
        // 1. 优先从本地队列获取
        if let Some(g) = p.pop_g() {
            return Some(g);
        }

        // 2. 从全局队列获取（批量）
        {
            let mut global = SCHEDULER.global_queue.lock();
            if let Some(g) = global.pop() {
                // 尝试多取几个到本地队列
                let batch_size = global.len().min(8);
                for _ in 0..batch_size {
                    if let Some(g_batch) = global.pop() {
                        p.add_g(g_batch);
                    }
                }
                return Some(g);
            }
        }

        // 3. 工作窃取
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
    }

    // fn push_global(g: Arc<G>) {
    //     let mut global = SCHEDULER.global_queue.lock();
    //     global.push(g);
    // }

    pub fn go<F>(f: F) -> Arc<G>
    where
        F: FnOnce() + Send + 'static,
    {
        let id = SCHEDULER.next_g_id.fetch_add(1, Ordering::Relaxed);
        let g = Arc::new(G::new(id, f));

        // 负载均衡：轮流分配到不同的 P
        let p_idx = id % SCHEDULER.processors.len();
        SCHEDULER.processors[p_idx].add_g(g.clone());

        g
    }

    pub fn shutdown() {
        SCHEDULER.running.store(false, Ordering::Relaxed);
    }

    pub fn yield_now() {
        thread::yield_now();
    }

    pub fn print_stats() {
        let mut total_work = 0;
        let mut total_steals = 0;
        for p in SCHEDULER.processors.iter() {
            total_work += p.work_count();
            total_steals += p.steal_count();
        }

        debug!("=== Scheduler Stats ===");
        debug!("Active goroutines: {}", Runtime::active_goroutines());
        debug!("Global queue size: {}", SCHEDULER.global_queue.lock().len());
        debug!("Total work: {}", total_work);
        debug!("Total steals: {}", total_steals);
        debug!(
            "Total spins: {}",
            SCHEDULER.stats.total_spins.load(Ordering::Relaxed)
        );
        debug!(
            "Total sleeps: {}",
            SCHEDULER.stats.total_sleeps.load(Ordering::Relaxed)
        );
        debug!("Processors: {}", SCHEDULER.processors.len());
    }
}

// ============== 公共 API ==============
pub fn go<F>(f: F) -> Arc<G>
where
    F: FnOnce() + Send + 'static,
{
    Scheduler::go(f)
}

pub fn yield_now() {
    Scheduler::yield_now()
}

pub fn print_scheduler_stats() {
    Scheduler::print_stats()
}
