use crate::scheduler;
use lazy_static::lazy_static;
use log::debug;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

lazy_static! {
    static ref RUNTIME_STATE: Arc<RuntimeState> = Arc::new(RuntimeState::new());
}

static ACTIVE_GOROUTINES: AtomicUsize = AtomicUsize::new(0);

/// 用于 wait_for_all 的 Condvar，避免 busy-wait
lazy_static! {
    static ref COMPLETION_COND: (Mutex<()>, Condvar) = (Mutex::new(()), Condvar::new());
}

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
        if ACTIVE_GOROUTINES.fetch_sub(1, Ordering::Release) == 1 {
            // 最后一个 goroutine 完成，通知 wait_for_all
            let (lock, cvar) = &*COMPLETION_COND;
            let _guard = lock.lock().unwrap();
            cvar.notify_one();
        }
    }

    #[inline]
    pub fn active_goroutines() -> usize {
        ACTIVE_GOROUTINES.load(Ordering::Acquire)
    }

    /// 等待所有 goroutine 完成
    /// 使用 Condvar 等待，避免 busy-wait
    pub fn wait_for_all() {
        debug!("⏳ Waiting for all goroutines to complete...");

        // 刷新当前线程的协程创建缓冲区，确保所有协程已加入调度队列
        scheduler::flush_go_batch();

        let start = Instant::now();
        let (lock, cvar) = &*COMPLETION_COND;

        // 先快速检查
        if Self::active_goroutines() == 0 {
            debug!("✅ All goroutines completed in {:?}", start.elapsed());
            return;
        }

        // 使用 Condvar 等待，每次被唤醒后重新检查
        let mut guard = lock.lock().unwrap();
        while Self::active_goroutines() > 0 {
            guard = cvar.wait_timeout(guard, Duration::from_millis(10)).unwrap().0;
        }

        debug!("✅ All goroutines completed in {:?}", start.elapsed());
    }
    pub fn wait_and_shutdown() {
        debug!("⏳ Waiting for all goroutines to complete and queues to drain...");
        Self::wait_for_all();
        // 关闭调度器
        scheduler::shutdown();
    }
}
