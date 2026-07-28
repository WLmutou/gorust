// src/timer.rs
// M:N 协程模型下的定时器实现
// 使用独立的定时器线程管理睡眠任务
// 协程调用 sleep 时，注册定时器并让出 CPU，定时器到期后重新加入调度队列

use crate::scheduler;
use lazy_static::lazy_static;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ============== 定时器条目 ==============

struct TimerEntry {
    wake_at: Instant,
    g: Arc<scheduler::G>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.wake_at == other.wake_at
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    // 反转排序，使 BinaryHeap 成为最小堆（最早到期的优先）
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other.wake_at.cmp(&self.wake_at)
    }
}

// ============== 定时器状态 ==============

struct TimerState {
    entries: BinaryHeap<TimerEntry>,
    shutdown: bool,
}

lazy_static! {
    static ref TIMER_STATE: (Mutex<TimerState>, Condvar) = {
        let state = TimerState {
            entries: BinaryHeap::new(),
            shutdown: false,
        };
        (Mutex::new(state), Condvar::new())
    };
    static ref TIMER_INIT: AtomicBool = AtomicBool::new(false);
    static ref TIMER_ENTRY_COUNT: AtomicUsize = AtomicUsize::new(0);
}

// ============== 定时器线程 ==============

fn timer_thread_loop() {
    // 预分配批量唤醒缓冲区，避免频繁分配
    let mut batch = Vec::with_capacity(64);
    let (lock, cvar) = &*TIMER_STATE;

    loop {
        let mut state = lock.lock().unwrap();

        if state.shutdown {
            return;
        }

        // 批量收集所有到期的定时器
        let now = Instant::now();
        while let Some(entry) = state.entries.peek() {
            if entry.wake_at <= now {
                let entry = state.entries.pop().unwrap();
                TIMER_ENTRY_COUNT.fetch_sub(1, Ordering::Relaxed);
                batch.push(entry.g);
            } else {
                break;
            }
        }

        if !batch.is_empty() {
            // 释放锁后批量唤醒，避免死锁
            drop(state);
            scheduler::wake_g_batch(std::mem::take(&mut batch));
            continue;
        }

        // 没有到期定时器，等待下一个到期或被通知
        let next_wake = state.entries.peek().map(|e| e.wake_at);
        match next_wake {
            None => {
                state = cvar.wait(state).unwrap();
            }
            Some(wake_at) => {
                let timeout = wake_at - Instant::now();
                if timeout > Duration::ZERO {
                    state = cvar.wait_timeout(state, timeout).unwrap().0;
                }
            }
        }
    }
}

// ============== 公共 API ==============

/// 初始化定时器线程
pub fn init_timer_thread() {
    if TIMER_INIT.swap(true, Ordering::SeqCst) {
        return;
    }

    thread::Builder::new()
        .name("timer-thread".to_string())
        .stack_size(16 * 1024)
        .spawn(timer_thread_loop)
        .ok();
}

/// 协程睡眠指定时长
pub fn sleep(duration: Duration) {
    if duration.is_zero() {
        scheduler::yield_now();
        return;
    }

    // 确保定时器线程已启动
    init_timer_thread();

    let g_ptr = scheduler::current_g_for_timer();
    if g_ptr.is_null() {
        // 不在协程上下文中，直接线程睡眠
        thread::sleep(duration);
        return;
    }

    unsafe {
        let g = &*g_ptr;

        // 先设置状态为 Waiting，再注册定时器
        // 避免竞态条件：定时器线程在状态设为 Waiting 之前触发，导致协程永远无法被唤醒
        g.set_status(scheduler::GStatus::Waiting);

        let wake_at = Instant::now() + duration;
        let entry = TimerEntry {
            wake_at,
            g: g.arc_clone(),
        };

        TIMER_ENTRY_COUNT.fetch_add(1, Ordering::Relaxed);

        let (lock, cvar) = &*TIMER_STATE;
        let mut state = lock.lock().unwrap();
        state.entries.push(entry);
        // 通知定时器线程有新定时器加入
        cvar.notify_one();
        drop(state);

        // 直接切换到调度器，不经过 yield_now（避免 re-queue 逻辑）
        scheduler::yield_to_scheduler_direct();
    }
}

/// 协程睡眠指定毫秒
pub fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms));
}

/// 关闭定时器
pub fn shutdown_timer() {
    let (lock, cvar) = &*TIMER_STATE;
    let mut state = lock.lock().unwrap();
    state.shutdown = true;
    cvar.notify_all();
    drop(state);
}

/// 获取待处理的定时器数量
pub fn pending_timer_count() -> usize {
    TIMER_ENTRY_COUNT.load(Ordering::Relaxed)
}