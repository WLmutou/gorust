// src/timer.rs
use crate::scheduler::{G, GStatus, Scheduler};
use lazy_static::lazy_static;
use parking_lot::Mutex;
use std::collections::binary_heap::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};

// ============== 定时器任务 ==============
struct TimerEntry {
    wake_time: Instant,
    g_id: usize,
    g: Arc<G>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.g_id == other.g_id
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.wake_time.cmp(&other.wake_time) {
            std::cmp::Ordering::Equal => self.g_id.cmp(&other.g_id),
            ord => ord.reverse(),
        }
    }
}

// ============== 全局定时器 ==============
lazy_static! {
    static ref TIMER: Mutex<BinaryHeap<TimerEntry>> = Mutex::new(BinaryHeap::new());
    static ref TIMER_THREAD_RUNNING: AtomicBool = AtomicBool::new(false);
}

/// 初始化定时器线程
pub fn init_timer_thread() {
    if TIMER_THREAD_RUNNING.swap(true, Ordering::SeqCst) {
        return;
    }

    thread::Builder::new()
        .name("gorust-timer".to_string())
        .spawn(|| {
            timer_worker();
        })
        .unwrap();
}

/// 定时器工作线程
fn timer_worker() {
    loop {
        let now = Instant::now();
        let mut to_wake = Vec::new();

        {
            let mut timer = TIMER.lock();
            while let Some(entry) = timer.peek() {
                if entry.wake_time <= now {
                    let entry = timer.pop().unwrap();
                    to_wake.push(entry.g);
                } else {
                    break;
                }
            }
        }

        // 唤醒到期的 G
        for g in to_wake {
            let status = g.status();
            if status == GStatus::Waiting {
                if cfg!(debug_assertions) {
                    log::debug!("[Timer] Waking G{}", g.id);
                }
                g.set_status(GStatus::Runnable);
                Scheduler::wake_g(g);
            }
        }

        // 检查是否应该退出
        if !Scheduler::is_running() && TIMER.lock().is_empty() {
            break;
        }

        // 计算下次唤醒时间
        let next_wake = {
            let timer = TIMER.lock();
            timer.peek().map(|entry| entry.wake_time)
        };

        match next_wake {
            Some(wake_time) => {
                let now = Instant::now();
                if wake_time > now {
                    let sleep_dur = wake_time - now;
                    thread::sleep(sleep_dur.min(Duration::from_millis(10)));
                }
            }
            None => {
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    TIMER_THREAD_RUNNING.store(false, Ordering::SeqCst);
}

/// 让当前 Goroutine 睡眠
pub fn sleep(duration: Duration) {
    if duration.is_zero() {
        Scheduler::yield_now();
        return;
    }

    let wake_time = Instant::now() + duration;

    match Scheduler::current_g() {
        Some(g) => {
            let g_id = g.id;

            if cfg!(debug_assertions) {
                log::debug!(
                    "[G{}] Sleeping for {:?}, will wake at {:?}",
                    g_id,
                    duration,
                    wake_time
                );
            }

            g.set_status(GStatus::Waiting);

            let entry = TimerEntry {
                wake_time,
                g_id,
                g: g.clone(),
            };

            TIMER.lock().push(entry);

            if cfg!(debug_assertions) {
                log::debug!("[G{}] Registered in timer, pending: {}", g_id, TIMER.lock().len());
            }

            // 等待被唤醒
            while g.status() == GStatus::Waiting {
                Scheduler::yield_now();
                thread::yield_now();
            }

            if cfg!(debug_assertions) {
                log::debug!("[G{}] Woken up, new status: {:?}", g_id, g.status());
            }
        }
        None => {
            // 不在 goroutine 上下文中，使用系统 sleep
            if cfg!(debug_assertions) {
                log::debug!("Sleep called outside goroutine context, using system sleep");
            }
            thread::sleep(duration);
        }
    }
}

/// 睡眠指定毫秒
pub fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms));
}

/// 清理定时器
pub fn shutdown_timer() {
    TIMER.lock().clear();
    TIMER_THREAD_RUNNING.store(false, Ordering::SeqCst);
}

/// 获取待处理任务数
pub fn pending_timer_count() -> usize {
    TIMER.lock().len()
}