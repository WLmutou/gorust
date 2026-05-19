use crate::scheduler;
use lazy_static::lazy_static;
use log::debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

lazy_static! {
    static ref RUNTIME_STATE: Arc<RuntimeState> = Arc::new(RuntimeState::new());
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
        let mut last_count = Self::active_goroutines();

        loop {
            let active = Self::active_goroutines();
            let pending = scheduler::Scheduler::pending_goroutines();
            
            if active == 0 && pending == 0 {
                break;
            }
            
            if active != last_count {
                last_count = active;
                if cfg!(debug_assertions) {
                    debug!("   Active goroutines: {}, pending: {}", active, pending);
                }
            }
            std::thread::sleep(Duration::from_millis(10));
            scheduler::yield_now();
        }

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
        scheduler::print_scheduler_stats();
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
        ACTIVE_GOROUTINES.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn active_goroutines() -> usize {
        ACTIVE_GOROUTINES.load(Ordering::Relaxed)
    }

    /// 等待所有 goroutine 完成并关闭调度器
    pub fn wait_and_shutdown() {
        debug!("⏳ Waiting for all goroutines to complete and queues to drain...");
        // 等待所有 goroutine 完成且调度队列为空
        let mut iterations = 0;
        loop {
            let active = Self::active_goroutines();
            let pending = scheduler::Scheduler::pending_goroutines();
            if active == 0 && pending == 0 {
                debug!("✅ All goroutines completed and queues drained in {} iterations", iterations);
                break;
            }
            if iterations % 100 == 0 {
                debug!("   Waiting... active: {}, pending: {}", active, pending);
            }
            iterations += 1;
            std::thread::yield_now();
        }
        
        // 关闭调度器
        scheduler::shutdown();
    }
}
