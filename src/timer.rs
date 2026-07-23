// src/timer.rs
// 使用 std::thread::sleep 实现睡眠
// 跨平台兼容，无需定时器堆和定时器线程

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

static TIMER_ENTRY_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn init_timer_thread() {
    // 在线程模式下，不需要独立的定时器线程
}

pub fn sleep(duration: Duration) {
    if duration.is_zero() {
        thread::yield_now();
        return;
    }
    // 使用 park_timeout 替代 thread::sleep
    // park_timeout 基于 futex/condvar 实现，比 nanosleep 更轻量
    thread::park_timeout(duration);
}

pub fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms));
}

pub fn shutdown_timer() {
    // 在线程模式下，不需要清理
}

pub fn pending_timer_count() -> usize {
    TIMER_ENTRY_COUNT.load(Ordering::Relaxed)
}