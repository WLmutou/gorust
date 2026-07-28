// src/scheduler.rs
// M:N 协程调度器
// 使用 x86_64 汇编上下文切换实现用户态协程
// 少量 OS 线程（GOMAXPROCS）调度大量用户态协程

use crate::context::{Context, switch_context};
use crate::go_runtime::Runtime;
use lazy_static::lazy_static;
use log::debug;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

// ============== 常量 ==============

/// 协程栈大小：16KB
/// Go 的初始栈为 2KB 但可动态扩容，Rust 使用固定栈
/// 16KB 在大多数场景下足够（包括 HTTP 请求），同时大幅减少内存分配开销
const G_STACK_SIZE: usize = 16 * 1024;

// ============== G (Goroutine) 状态 ==============

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GStatus {
    Idle = 0,
    Runnable = 1,
    Running = 2,
    Waiting = 3,
    Dead = 4,
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

// ============== G (Goroutine) ==============

/// 用户态协程
pub struct G {
    pub id: usize,
    status: AtomicU8,
    pub ctx: Context,
    stack: Option<Vec<u8>>,
    closure: Option<Box<dyn FnOnce() + Send + 'static>>,
    /// 内联 completed 标志，避免额外的 Arc 分配
    pub completed: AtomicBool,
}

unsafe impl Send for G {}
unsafe impl Sync for G {}

impl G {
    fn new(id: usize, closure: Box<dyn FnOnce() + Send + 'static>) -> Self {
        // 避免零初始化栈内存（16KB 零初始化在大量创建时是瓶颈）
        let mut stack: Vec<u8> = Vec::with_capacity(G_STACK_SIZE);
        unsafe { stack.set_len(G_STACK_SIZE); }
        let stack_top = unsafe { stack.as_mut_ptr().add(G_STACK_SIZE) };

        let ctx = Context::make_initial(stack_top, goroutine_trampoline);

        G {
            id,
            status: AtomicU8::new(GStatus::Runnable as u8),
            ctx,
            stack: Some(stack),
            closure: Some(closure),
            completed: AtomicBool::new(false),
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
}

impl Drop for G {
    fn drop(&mut self) {
        // 不需要打印调试信息，去掉 created_at 字段以减少创建开销
    }
}

/// 从 &G 引用安全创建一个 Arc<G> 克隆
/// 用于定时器模块保存协程引用
impl G {
    pub fn arc_clone(&self) -> Arc<Self> {
        let ptr = self as *const Self as *mut Self;
        unsafe {
            Arc::increment_strong_count(ptr);
            Arc::from_raw(ptr)
        }
    }
}

// ============== 线程本地存储 ==============

thread_local! {
    /// 当前工作线程的调度器上下文
    /// 当协程切换回来时，恢复此上下文以继续调度循环
    static SCHEDULER_CTX: UnsafeCell<Context> = UnsafeCell::new(Context::new_empty());
    /// 当前正在运行的协程指针
    static CURRENT_G: UnsafeCell<*mut G> = UnsafeCell::new(std::ptr::null_mut());
    /// 协程创建批处理缓冲区（减少锁竞争）
    static GO_BATCH: UnsafeCell<Vec<Arc<G>>> = UnsafeCell::new(Vec::with_capacity(128));
}

// ============== 调度器状态 ==============

struct SchedulerState {
    run_queue: VecDeque<Arc<G>>,
    worker_count: usize,
    shutdown: bool,
}

lazy_static! {
    static ref SCHEDULER: Scheduler = Scheduler::new();
    /// 调度器状态 + 条件变量
    /// 使用 std::sync::Mutex 以便与 Condvar 配合
    static ref SCHED_STATE: (std::sync::Mutex<SchedulerState>, std::sync::Condvar) = {
        let state = SchedulerState {
            run_queue: VecDeque::new(),
            worker_count: 0,
            shutdown: false,
        };
        (std::sync::Mutex::new(state), std::sync::Condvar::new())
    };
    /// 工作线程句柄，用于 shutdown 时等待线程退出
    static ref WORKER_HANDLES: std::sync::Mutex<Vec<thread::JoinHandle<()>>> = std::sync::Mutex::new(Vec::new());
}

// ============== 协程入口蹦床函数 ==============

/// 协程入口函数（extern "C" 以匹配 make_initial 的签名）
/// 每个协程首次运行时从这里开始：
/// 1. 从 TLS 获取当前 G 指针并保存为本地变量
/// 2. 执行 G 的闭包
/// 3. 标记 G 为 Dead
/// 4. 切换回调度器
///
/// 重要：必须使用本地保存的 g_ptr，不能再次读取 CURRENT_G。
/// 因为 trampoline 是通过 `ret` 进入的（不是 `call`），栈上没有有效的返回地址，
/// 所以绝不能用普通 `return` 退出，必须始终通过 switch_context 回到调度器。
#[inline(never)]
extern "C" fn goroutine_trampoline() {
    // 1. 从 TLS 获取当前 G 指针并保存为本地变量
    let g_ptr = CURRENT_G.with(|cg| unsafe { *cg.get() });
    if g_ptr.is_null() {
        // 不应该发生，但万一发生，只能返回（会崩溃，但至少不会静默地继续执行）
        return;
    }

    unsafe {
        let g = &mut *g_ptr;
        // 执行协程闭包
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(closure) = g.closure.take() {
                closure();
            }
        }));
        if let Err(e) = result {
            log::error!("[Goroutine {}] panicked: {:?}", g.id, e);
        }
        // 标记完成
        g.completed.store(true, Ordering::Release);
        g.set_status(GStatus::Dead);
    }

    Runtime::untrack_goroutine();

    // 2. 切换回调度器
    // 使用本地保存的 g_ptr，绝不重新读取 CURRENT_G
    let g = unsafe { &mut *g_ptr };
    let sched_ctx = SCHEDULER_CTX.with(|sc| unsafe { (*sc.get()).clone() });
    unsafe {
        switch_context(&mut g.ctx, &sched_ctx);
    }
    // switch_context 恢复调度器上下文后，不会再回到这里
    // unreachable_unchecked() 告诉编译器这是不可达代码
    unsafe { std::hint::unreachable_unchecked(); }
}

// ============== 调度器 ==============

pub(crate) struct Scheduler {
    next_g_id: AtomicUsize,
    initialized: AtomicBool,
}

impl Scheduler {
    fn new() -> Self {
        Scheduler {
            next_g_id: AtomicUsize::new(1),
            initialized: AtomicBool::new(false),
        }
    }

    pub fn init() {
        if SCHEDULER.initialized.swap(true, Ordering::SeqCst) {
            return;
        }

        let num_workers = num_cpus::get().max(2);
        debug!("   GoRust M:N scheduler initialized ({} workers)", num_workers);

        {
            let (lock, _) = &*SCHED_STATE;
            let mut state = lock.lock().unwrap();
            state.worker_count = num_workers;
        }

        // 启动工作线程
        let mut handles = WORKER_HANDLES.lock().unwrap();
        for i in 0..num_workers {
            let handle = thread::Builder::new()
                .name(format!("worker-{}", i))
                .stack_size(256 * 1024) // 工作线程栈 256KB
                .spawn(worker_loop)
                .ok();
            if let Some(h) = handle {
                handles.push(h);
            }
        }
        drop(handles);

        crate::netpoller::start();
    }

    /// 创建一个新的 goroutine 并加入调度队列
    pub fn go<F>(f: F) -> Arc<G>
    where
        F: FnOnce() + Send + 'static,
    {
        let id = SCHEDULER.next_g_id.fetch_add(1, Ordering::Relaxed);
        let g = Arc::new(G::new(id, Box::new(f)));

        Runtime::track_goroutine();

        // 先加入线程本地批处理缓冲区
        // 缓冲区满时再批量刷新到全局队列，减少锁竞争
        GO_BATCH.with(|batch| {
            let batch = unsafe { &mut *batch.get() };
            batch.push(g.clone());
            if batch.len() >= 128 {
                Self::flush_go_batch_inner(batch);
            }
        });

        g
    }

    /// 刷新线程本地的协程创建缓冲区到全局队列
    fn flush_go_batch_inner(batch: &mut Vec<Arc<G>>) {
        if batch.is_empty() {
            return;
        }
        let (lock, cvar) = &*SCHED_STATE;
        let mut state = lock.lock().unwrap();
        let was_empty = state.run_queue.is_empty();
        let count = batch.len();
        for g in batch.drain(..) {
            state.run_queue.push_back(g);
        }
        // 仅当队列之前为空时才通知，避免批量创建时的惊群效应
        if was_empty && count > 0 {
            // 通知多个 worker
            let worker_count = state.worker_count.max(1);
            for _ in 0..worker_count.min(count) {
                cvar.notify_one();
            }
        }
    }

    /// 刷新当前线程的协程创建缓冲区
    /// 在 wait_for_all 前调用，确保所有协程都已加入调度队列
    pub fn flush_go_batch() {
        GO_BATCH.with(|batch| {
            let batch = unsafe { &mut *batch.get() };
            if !batch.is_empty() {
                Self::flush_go_batch_inner(batch);
            }
        });
    }

    /// 创建一个可让出的 goroutine（FnMut）
    pub fn go_task<F>(f: F) -> Arc<G>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let mut f = f;
        // 包装成循环：反复调用直到返回 true
        Scheduler::go(move || {
            loop {
                if f() {
                    break;
                }
                // 让出 CPU
                Scheduler::yield_now();
            }
        })
    }

    /// 直接切换到调度器，不修改协程状态
    /// 用于 sleep 等场景，协程状态已在调用前设置好
    pub fn yield_to_scheduler_direct() {
        let g_ptr = CURRENT_G.with(|cg| unsafe { *cg.get() });
        if g_ptr.is_null() {
            return;
        }
        let g = unsafe { &mut *g_ptr };
        // 直接切换到调度器
        let sched_ctx = SCHEDULER_CTX.with(|sc| unsafe { (*sc.get()).clone() });
        unsafe {
            switch_context(&mut g.ctx, &sched_ctx);
        }
    }

    /// 让出当前协程的执行权
    pub fn yield_now() {
        let g_ptr = CURRENT_G.with(|cg| unsafe { *cg.get() });
        if g_ptr.is_null() {
            thread::yield_now();
            return;
        }

        let g = unsafe { &mut *g_ptr };
        // 如果已经是 Waiting 状态（如 sleep 中），保持 Waiting 让定时器唤醒
        // 否则设置为 Runnable，让调度器重新调度
        let current_status = g.status();
        if current_status == GStatus::Running || current_status == GStatus::Runnable {
            g.set_status(GStatus::Runnable);
        }
        // 如果 status 是 Waiting，保持不动，让定时器线程来唤醒
        // 获取调度器上下文
        let sched_ctx = SCHEDULER_CTX.with(|sc| unsafe { (*sc.get()).clone() });
        // 切换回调度器
        unsafe {
            switch_context(&mut g.ctx, &sched_ctx);
        }
        // 当协程重新被调度时，从这里继续执行
    }

    pub fn shutdown() {
        // 设置关闭标志并唤醒所有工作线程
        {
            let (lock, cvar) = &*SCHED_STATE;
            let mut state = lock.lock().unwrap();
            state.shutdown = true;
            cvar.notify_all();
        }

        // 等待所有工作线程退出
        let handles = std::mem::take(&mut *WORKER_HANDLES.lock().unwrap());
        for handle in handles {
            let _ = handle.join();
        }

        crate::timer::shutdown_timer();
        crate::netpoller::stop();
    }

    #[allow(dead_code)]
    pub fn is_running() -> bool {
        let (lock, _) = &*SCHED_STATE;
        let state = lock.lock().unwrap();
        !state.shutdown
    }

    pub fn pending_goroutines() -> usize {
        let (lock, _) = &*SCHED_STATE;
        let state = lock.lock().unwrap();
        state.run_queue.len()
    }

    // ============== 兼容 API ==============
    #[allow(dead_code)]
    pub fn current_g() -> Option<Arc<G>> {
        None
    }

    #[allow(dead_code)]
    pub fn set_current_g(_g: Option<Arc<G>>) {}

    #[inline]
    pub fn yield_goroutine() {
        Scheduler::yield_now();
    }

    #[inline]
    pub fn yield_to_scheduler() {
        Scheduler::yield_now();
    }

    #[inline]
    #[allow(dead_code)]
    pub fn yield_to_scheduler_with_g(_g: &Arc<G>) {
        Scheduler::yield_now();
    }

    #[inline]
    pub fn is_yield_requested() -> bool {
        false
    }

    #[inline]
    pub fn clear_yield_flag() {}

    pub fn wake_g(g: Arc<G>) {
        let (lock, cvar) = &*SCHED_STATE;
        let mut state = lock.lock().unwrap();
        if !state.shutdown && g.status() == GStatus::Waiting {
            g.set_status(GStatus::Runnable);
            state.run_queue.push_back(g);
            cvar.notify_one();
        }
    }

    pub fn wake_g_batch(gs: Vec<Arc<G>>) {
        let (lock, cvar) = &*SCHED_STATE;
        let mut state = lock.lock().unwrap();
        if !state.shutdown {
            let count = gs.len();
            for g in gs {
                if g.status() == GStatus::Waiting {
                    g.set_status(GStatus::Runnable);
                    state.run_queue.push_back(g);
                }
            }
            if !state.run_queue.is_empty() {
                // 通知多个 worker 处理新任务
                // 最多通知 worker 数量，避免惊群效应
                let worker_count = state.worker_count.max(1);
                for _ in 0..worker_count.min(count) {
                    cvar.notify_one();
                }
            }
        }
    }
}

// ============== 工作线程循环 ==============

fn worker_loop() {
    let (lock, cvar) = &*SCHED_STATE;

    loop {
        // 1. 从全局队列获取一个可运行的 G
        let g = {
            let mut state = lock.lock().unwrap();

            loop {
                if state.shutdown {
                    return;
                }
                if let Some(g) = state.run_queue.pop_front() {
                    // 如果队列中还有更多任务，唤醒其他 worker
                    if !state.run_queue.is_empty() {
                        cvar.notify_one();
                    }
                    break g;
                }
                // 队列为空，等待
                state = cvar.wait(state).unwrap();
            }
        };

        // 2. 设置当前 G 到 TLS
        let g_ptr: *mut G = Arc::as_ptr(&g) as *mut G;
        CURRENT_G.with(|cg| unsafe { *cg.get() = g_ptr });
        g.set_status(GStatus::Running);

        // 3. 保存调度器上下文，切换到协程
        let sched_ctx = SCHEDULER_CTX.with(|sc| unsafe { &mut *sc.get() });
        unsafe {
            switch_context(sched_ctx, &g.ctx);
        }
        // 当协程让出或完成时，会回到这里

        // 4. 检查协程状态
        let status = g.status();
        if status == GStatus::Dead {
            // 协程已完成，释放资源
            CURRENT_G.with(|cg| unsafe { *cg.get() = std::ptr::null_mut() });
        } else if status == GStatus::Runnable {
            // 协程让出了（如 yield_now），重新加入调度队列
            let mut state = lock.lock().unwrap();
            if !state.shutdown {
                state.run_queue.push_back(g);
            }
        } else if status == GStatus::Waiting {
            // 协程正在等待（如 sleep），由定时器线程唤醒
            // 清除 CURRENT_G，允许 worker 处理其他协程
            CURRENT_G.with(|cg| unsafe { *cg.get() = std::ptr::null_mut() });
        }
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

/// 直接切换到调度器，不修改协程状态
/// 用于 sleep 等场景，协程状态已在调用前设置好
pub fn yield_to_scheduler_direct() {
    Scheduler::yield_to_scheduler_direct()
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

/// 刷新当前线程的协程创建缓冲区
/// 确保所有通过 go() 创建的协程都已加入调度队列
pub fn flush_go_batch() {
    Scheduler::flush_go_batch()
}

pub fn wake_g(g: Arc<G>) {
    Scheduler::wake_g(g)
}

pub fn wake_g_batch(gs: Vec<Arc<G>>) {
    Scheduler::wake_g_batch(gs)
}

/// 获取当前协程的原始指针（供定时器模块使用）
/// 如果不在协程上下文中，返回 null
pub fn current_g_for_timer() -> *mut G {
    CURRENT_G.with(|cg| unsafe { *cg.get() })
}