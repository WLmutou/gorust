// src/scheduler.rs
// 使用线程池+工作队列实现 goroutine
// 跨平台兼容，线程动态创建和复用
//
// 优化说明：
// 1. 后台线程专门负责线程创建，避免 go() 阻塞
// 2. 预创建更多线程，覆盖中低并发场景
// 3. 增大批量创建大小，减少批次数
// 4. 使用定时器线程管理睡眠任务，避免阻塞工作线程

use crate::go_runtime::Runtime;
use lazy_static::lazy_static;
use log::debug;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ============== 任务类型 ==============
// 普通任务：FnOnce，运行到完成
type Task = Box<dyn FnOnce() + Send + 'static>;
// 可挂起任务：FnMut() -> bool，返回 true 表示完成，false 表示需要挂起
type SuspendableTask = Box<dyn FnMut() -> bool + Send + 'static>;

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
    pub id: usize,
    status: AtomicU8,
    created_at: Instant,
    completed: Arc<AtomicBool>,
}

unsafe impl Send for G {}
unsafe impl Sync for G {}

impl G {
    pub fn new(id: usize) -> Self {
        G {
            id,
            status: AtomicU8::new(GStatus::Running as u8),
            created_at: Instant::now(),
            completed: Arc::new(AtomicBool::new(false)),
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

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    fn completed_flag(&self) -> Arc<AtomicBool> {
        self.completed.clone()
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

// ============== 定时器任务 ==============
struct TimerEntry {
    wake_at: Instant,
    task: Option<SuspendableTask>,
    worker: Option<thread::Thread>,
}

// ============== 调度器状态 ==============
struct SchedulerState {
    task_queue: VecDeque<Task>,
    idle_workers: VecDeque<thread::Thread>,
    worker_count: usize,
    shutdown: bool,
    // 定时器相关
    timers: VecDeque<TimerEntry>,
    timer_thread: Option<thread::JoinHandle<()>>,
}

// 批量创建线程的批次大小（增大以减少批次数）
const BATCH_CREATE_SIZE: usize = 512;
// 预创建线程池的大小（增大以覆盖更多并发场景）
const PREALLOC_POOL_SIZE: usize = 8192;
// 线程栈大小（8KB，最小栈大小）
const THREAD_STACK_SIZE: usize = 8 * 1024;

// ============== 全局调度器 ==============
lazy_static! {
    static ref SCHEDULER: Scheduler = Scheduler::new();
}

pub(crate) struct Scheduler {
    next_g_id: AtomicUsize,
    state: Mutex<SchedulerState>,
    // 通知后台线程创建线程
    creator_signal: Arc<AtomicBool>,
}

/// 批量创建工作线程
fn spawn_worker_threads(count: usize) {
    for _ in 0..count {
        let _ = thread::Builder::new()
            .name("worker".to_string())
            .stack_size(THREAD_STACK_SIZE)
            .spawn(|| worker_loop());
    }
}

impl Scheduler {
    #[allow(dead_code)]
    fn new() -> Self {
        Scheduler {
            next_g_id: AtomicUsize::new(1),
            state: Mutex::new(SchedulerState {
                task_queue: VecDeque::new(),
                idle_workers: VecDeque::new(),
                worker_count: 0,
                shutdown: false,
                timers: VecDeque::new(),
                timer_thread: None,
            }),
            creator_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn init() {
        use std::sync::atomic::AtomicBool;
        static INITIALIZED: AtomicBool = AtomicBool::new(false);
        if INITIALIZED.swap(true, Ordering::SeqCst) {
            return;
        }

        debug!("   GoRust thread-pool scheduler initialized");
        debug!("   GOMAXPROCS={}", num_cpus::get());

        // 启动后台线程创建器
        let signal = SCHEDULER.creator_signal.clone();
        thread::Builder::new()
            .name("thread-creator".to_string())
            .stack_size(4096)
            .spawn(move || creator_loop(signal))
            .ok();

        // 启动定时器线程
        Scheduler::start_timer_thread();

        // 预创建线程池，避免后续逐个创建线程的开销
        {
            let mut state = SCHEDULER.state.lock();
            state.worker_count = PREALLOC_POOL_SIZE;
            drop(state);
        }
        spawn_worker_threads(PREALLOC_POOL_SIZE);

        crate::netpoller::start();
    }

    /// 启动定时器线程
    fn start_timer_thread() {
        let mut state = SCHEDULER.state.lock();
        if state.timer_thread.is_some() {
            return;
        }
        let handle = thread::Builder::new()
            .name("timer".to_string())
            .stack_size(4096)
            .spawn(|| timer_loop())
            .unwrap();
        state.timer_thread = Some(handle);
    }

    /// 注册定时器：在指定时间后唤醒一个任务
    #[allow(dead_code)]
    fn register_timer(duration: Duration, task: SuspendableTask) {
        let wake_at = Instant::now() + duration;
        let mut state = SCHEDULER.state.lock();
        state.timers.push_back(TimerEntry {
            wake_at,
            task: Some(task),
            worker: None,
        });
        drop(state);
    }

    /// 注册定时器：在指定时间后唤醒一个工作线程
    #[allow(dead_code)]
    fn register_timer_for_worker(duration: Duration, worker: thread::Thread) {
        let wake_at = Instant::now() + duration;
        let mut state = SCHEDULER.state.lock();
        state.timers.push_back(TimerEntry {
            wake_at,
            task: None,
            worker: Some(worker),
        });
        drop(state);
    }

    /// 创建 goroutine（FnOnce），放入任务队列
    pub fn go<F>(f: F) -> Arc<G>
    where
        F: FnOnce() + Send + 'static,
    {
        let id = SCHEDULER.next_g_id.fetch_add(1, Ordering::Relaxed);
        let g = Arc::new(G::new(id));
        let completed = g.completed_flag();

        // 包装任务：执行、panic处理、完成标记
        let task = Box::new(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            if let Err(e) = result {
                log::error!("[Goroutine {}] panicked: {:?}", id, e);
            }
            completed.store(true, Ordering::Release);
            Runtime::untrack_goroutine();
        }) as Task;

        Runtime::track_goroutine();

        let mut state = SCHEDULER.state.lock();
        state.task_queue.push_back(task);

        // 唤醒空闲工作线程
        if let Some(worker) = state.idle_workers.pop_front() {
            drop(state);
            worker.unpark();
        } else {
            // 通知后台线程创建器
            drop(state);
            SCHEDULER.creator_signal.store(true, Ordering::Release);
        }

        g
    }

    /// 创建可让出的 goroutine（FnMut() -> bool）
    pub fn go_task<F>(f: F) -> Arc<G>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let id = SCHEDULER.next_g_id.fetch_add(1, Ordering::Relaxed);
        let mut f = f;
        let g = Arc::new(G::new(id));
        let completed = g.completed_flag();

        // 对于 FnMut，包装成循环：反复调用直到返回 true
        let task = Box::new(move || {
            loop {
                if f() {
                    break;
                }
                // 让出 CPU 给其他线程
                thread::yield_now();
            }
            completed.store(true, Ordering::Release);
            Runtime::untrack_goroutine();
        }) as Task;

        Runtime::track_goroutine();

        let mut state = SCHEDULER.state.lock();
        state.task_queue.push_back(task);

        if let Some(worker) = state.idle_workers.pop_front() {
            drop(state);
            worker.unpark();
        } else {
            drop(state);
            SCHEDULER.creator_signal.store(true, Ordering::Release);
        }

        g
    }

    pub fn shutdown() {
        let mut state = SCHEDULER.state.lock();
        state.shutdown = true;
        // 唤醒所有空闲工作线程，让它们退出
        let workers: Vec<_> = state.idle_workers.drain(..).collect();
        // 唤醒所有定时器中的工作线程
        for entry in state.timers.iter_mut() {
            if let Some(worker) = entry.worker.take() {
                worker.unpark();
            }
        }
        drop(state);

        for worker in workers {
            worker.unpark();
        }

        crate::timer::shutdown_timer();
        crate::netpoller::stop();
    }

    pub fn yield_now() {
        thread::yield_now();
    }

    #[allow(dead_code)]
    pub fn is_running() -> bool {
        !SCHEDULER.state.lock().shutdown
    }

    pub fn pending_goroutines() -> usize {
        Runtime::active_goroutines()
    }

    // ============== 兼容 API（保留供外部 crate 使用） ==============
    #[allow(dead_code)]
    pub fn current_g() -> Option<Arc<G>> {
        None
    }

    #[allow(dead_code)]
    pub fn set_current_g(_g: Option<Arc<G>>) {}

    #[inline]
    pub fn yield_goroutine() {
        thread::yield_now();
    }

    #[inline]
    pub fn yield_to_scheduler() {
        thread::yield_now();
    }

    #[inline]
    #[allow(dead_code)]
    pub fn yield_to_scheduler_with_g(_g: &Arc<G>) {
        thread::yield_now();
    }

    #[inline]
    pub fn is_yield_requested() -> bool {
        false
    }

    #[inline]
    pub fn clear_yield_flag() {}

    pub fn wake_g(_g: Arc<G>) {}

    #[allow(dead_code)]
    pub fn wake_g_batch(_gs: Vec<Arc<G>>) {}
}

// ============== 后台线程创建器 ==============
fn creator_loop(signal: Arc<AtomicBool>) {
    loop {
        // 等待信号
        if !signal.swap(false, Ordering::Acquire) {
            thread::park();
            continue;
        }

        // 检查是否需要创建线程
        loop {
            let (task_count, idle_count, shutdown) = {
                let state = SCHEDULER.state.lock();
                if state.shutdown {
                    return;
                }
                let idle = state.idle_workers.len();
                let queue = state.task_queue.len();
                (queue, idle, state.shutdown)
            };

            if shutdown {
                return;
            }

            // 如果队列中的任务少于空闲线程，不需要创建
            if task_count <= idle_count {
                break;
            }

            let needed = task_count - idle_count;
            let batch = needed.min(BATCH_CREATE_SIZE);

            let mut state = SCHEDULER.state.lock();
            state.worker_count += batch;
            drop(state);

            spawn_worker_threads(batch);
        }

        // 检查是否有新信号（避免丢失信号）
        if signal.load(Ordering::Acquire) {
            signal.store(false, Ordering::Release);
        }
    }
}

// ============== 定时器线程 ==============
fn timer_loop() {
    loop {
        // 1. 找到最近的下一个唤醒时间
        let next_wake = {
            let state = SCHEDULER.state.lock();
            if state.shutdown {
                return;
            }
            state.timers.iter().map(|t| t.wake_at).min()
        };

        // 2. 休眠直到下一个唤醒时间
        let now = Instant::now();
        let sleep_dur = match next_wake {
            Some(wake_at) if wake_at > now => wake_at - now,
            Some(_) => Duration::ZERO,  // 有已到期的定时器，立即处理
            None => Duration::from_millis(1), // 没有定时器，休眠 1ms 后检查
        };

        if sleep_dur > Duration::ZERO {
            thread::park_timeout(sleep_dur);
        }

        // 3. 获取所有到期的定时器
        let now = Instant::now();
        let mut state = SCHEDULER.state.lock();
        if state.shutdown {
            return;
        }

        let mut expired = Vec::new();
        let mut remaining = VecDeque::new();
        while let Some(entry) = state.timers.pop_front() {
            if entry.wake_at <= now {
                expired.push(entry);
            } else {
                remaining.push_back(entry);
            }
        }
        state.timers = remaining;

        if expired.is_empty() {
            continue;
        }

        // 4. 批量处理所有到期定时器（避免反复释放/获取锁）
        let mut need_creator = false;
        for entry in expired {
            if let Some(mut task) = entry.task {
                let task_box: Task = Box::new(move || { task(); });
                state.task_queue.push_back(task_box);
                if let Some(worker) = state.idle_workers.pop_front() {
                    worker.unpark();
                } else {
                    need_creator = true;
                }
            } else if let Some(worker) = entry.worker {
                // 工作线程定时器到期：直接唤醒
                worker.unpark();
            }
        }

        if need_creator {
            drop(state);
            SCHEDULER.creator_signal.store(true, Ordering::Release);
        }
    }
}

// ============== 工作线程循环 ==============
fn worker_loop() {
    loop {
        // 尝试获取任务
        let task = {
            let mut state = SCHEDULER.state.lock();
            if let Some(task) = state.task_queue.pop_front() {
                drop(state);
                task
            } else if state.shutdown {
                return;
            } else {
                // 没有任务，休眠等待
                state.idle_workers.push_back(thread::current());
                drop(state);
                thread::park();
                continue;
            }
        };

        // 执行任务
        task();
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

pub fn yield_to_scheduler() {
    Scheduler::yield_to_scheduler()
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