use crate::scheduler;
use lazy_static::lazy_static;
use log::debug;
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

lazy_static! {
    static ref RUNTIME_STATE: Arc<RuntimeState> = Arc::new(RuntimeState::new());
    /// 用于通知 wait_for_all 有 goroutine 完成
    static ref COMPLETION: (Mutex<()>, Condvar) = (Mutex::new(()), Condvar::new());
}

static ACTIVE_GOROUTINES: AtomicUsize = AtomicUsize::new(0);

struct RuntimeState {
    active_goroutines: AtomicUsize,
    shutdown: AtomicBool,
    start_time: Instant,
    total_goroutines: AtomicUsize,
}

impl RuntimeState {
    fn new() -> Self {
        RuntimeState {
            active_goroutines: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
            start_time: Instant::now(),
            total_goroutines: AtomicUsize::new(0),
        }
    }

    #[allow(dead_code)]
    fn inc_goroutine(&self) {
        self.active_goroutines.fetch_add(1, Ordering::Relaxed);
        self.total_goroutines.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    #[inline]
    fn dec_goroutine(&self) {
        self.active_goroutines.fetch_sub(1, Ordering::Relaxed);
    }

    fn _active_count(&self) -> usize {
        self.active_goroutines.load(Ordering::Relaxed)
    }

    fn total_count(&self) -> usize {
        self.total_goroutines.load(Ordering::Relaxed)
    }
}

pub struct Runtime;

impl Runtime {
    /// 初始化运行时
    pub fn init() {
        debug!("🚀 GoRust Runtime v0.2.0 initialized");
        debug!("   GOMAXPROCS={}", num_cpus::get());
        scheduler::Scheduler::init();

        // 注册 Ctrl-C 信号处理
        Self::setup_signal_handler();
    }

    /// 设置 Ctrl-C 信号处理
    fn setup_signal_handler() {
        let _ = ctrlc::set_handler(|| {
            debug!("📡 Received Ctrl-C signal, initiating shutdown...");
            scheduler::shutdown();
            std::process::exit(0);
        });
    }

    /// 等待所有 goroutine 完成
    pub fn wait_for_all() {
        debug!("⏳ Waiting for all goroutines to complete...");

        let start = Instant::now();
        let (lock, cvar) = &*COMPLETION;

        let mut guard = lock.lock();
        while Self::active_goroutines() > 0 {
            // 等待 Condvar 通知（goroutine 完成时触发）
            cvar.wait(&mut guard);
        }
        drop(guard);

        debug!("✅ All goroutines completed in {:?}", start.elapsed());
    }

    /// 关闭运行时
    pub fn shutdown() {
        debug!("🛑 GoRust Runtime shutting down");
        debug!(
            "   Total goroutines spawned: {}",
            RUNTIME_STATE.total_count()
        );
        debug!("   Uptime: {:?}", RUNTIME_STATE.start_time.elapsed());

        RUNTIME_STATE.shutdown.store(true, Ordering::Relaxed);
        debug!("   Scheduler stats: {} pending goroutines", scheduler::pending_goroutines());
    }

    /// 是否正在关闭
    pub fn is_shutting_down() -> bool {
        RUNTIME_STATE.shutdown.load(Ordering::Relaxed)
    }

    // /// 记录新创建的 goroutine
    // #[inline]
    // pub(crate) fn track_goroutine() {
    //     RUNTIME_STATE.inc_goroutine();
    // }

    /// goroutine 完成时调用
    // #[inline]
    // pub(crate) fn untrack_goroutine() {
    //     RUNTIME_STATE.dec_goroutine();
    // }

    /// 获取当前活跃的 goroutine 数量
    // pub fn active_goroutines() -> usize {
    //     RUNTIME_STATE.active_count()
    // }

    /// 获取总共创建的 goroutine 数量
    pub fn total_goroutines() -> usize {
        RUNTIME_STATE.total_count()
    }

    /// 获取运行时长
    pub fn uptime() -> Duration {
        RUNTIME_STATE.start_time.elapsed()
    }

    #[inline]
    pub fn track_goroutine() {
        ACTIVE_GOROUTINES.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn untrack_goroutine() {
        let prev = ACTIVE_GOROUTINES.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            // 最后一个 goroutine 完成，通知 wait_for_all
            let (lock, cvar) = &*COMPLETION;
            let _guard = lock.lock();
            cvar.notify_all();
        }
    }

    #[inline]
    pub fn active_goroutines() -> usize {
        ACTIVE_GOROUTINES.load(Ordering::Acquire)
    }

    /// 等待所有 goroutine 完成并关闭调度器
    pub fn wait_and_shutdown() {
        debug!("⏳ Waiting for all goroutines to complete and queues to drain...");
        let (lock, cvar) = &*COMPLETION;
        let mut guard = lock.lock();
        while Self::active_goroutines() > 0 {
            cvar.wait(&mut guard);
        }
        drop(guard);
        
        // 关闭调度器
        scheduler::shutdown();
    }
}
