use crate::scheduler;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use lazy_static::lazy_static;
use std::time::{Duration, Instant};
use log::debug;

lazy_static! {
    static ref RUNTIME_STATE: Arc<RuntimeState> = Arc::new(RuntimeState::new());
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
    
    #[inline]
    fn inc_goroutine(&self) {
        self.active_goroutines.fetch_add(1, Ordering::Relaxed);
        self.total_goroutines.fetch_add(1, Ordering::Relaxed);
    }
    
    #[inline]
    fn dec_goroutine(&self) {
        self.active_goroutines.fetch_sub(1, Ordering::Relaxed);
    }
    
    fn active_count(&self) -> usize {
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
    
    /// 等待所有 goroutine 完成
    pub fn wait_for_all() {
        debug!("⏳ Waiting for all goroutines to complete...");
        
        let start = Instant::now();
        let mut last_count = RUNTIME_STATE.active_count();
        
        while RUNTIME_STATE.active_count() > 0 {
            let current_count = RUNTIME_STATE.active_count();
            if current_count != last_count {
                last_count = current_count;
                if cfg!(debug_assertions) {
                    debug!("   Active goroutines: {}", current_count);
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
        debug!("   Total goroutines spawned: {}", RUNTIME_STATE.total_count());
        debug!("   Uptime: {:?}", RUNTIME_STATE.start_time.elapsed());
        
        RUNTIME_STATE.shutdown.store(true, Ordering::Relaxed);
        scheduler::print_scheduler_stats();
    }
    
    /// 是否正在关闭
    pub fn is_shutting_down() -> bool {
        RUNTIME_STATE.shutdown.load(Ordering::Relaxed)
    }
    
    /// 记录新创建的 goroutine
    #[inline]
    pub(crate) fn track_goroutine() {
        RUNTIME_STATE.inc_goroutine();
    }
    
    /// goroutine 完成时调用
    #[inline]
    pub(crate) fn untrack_goroutine() {
        RUNTIME_STATE.dec_goroutine();
    }
    
    /// 获取当前活跃的 goroutine 数量
    pub fn active_goroutines() -> usize {
        RUNTIME_STATE.active_count()
    }
    
    /// 获取总共创建的 goroutine 数量
    pub fn total_goroutines() -> usize {
        RUNTIME_STATE.total_count()
    }
    
    /// 获取运行时长
    pub fn uptime() -> Duration {
        RUNTIME_STATE.start_time.elapsed()
    }
}