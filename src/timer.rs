use crate::scheduler::{G, GStatus, Scheduler};
use lazy_static::lazy_static;
use parking_lot::Mutex;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const TIMER_BUCKET_COUNT: usize = 64;
const TIMER_TICK_MS: u64 = 10;

struct TimerEntry {
    wake_time: Instant,
    g: Arc<G>,
    bucket_id: usize,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.g, &other.g)
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
        other.wake_time.cmp(&self.wake_time)
    }
}

struct TimerBucket {
    heap: Mutex<BinaryHeap<TimerEntry>>,
}

impl TimerBucket {
    fn new() -> Self {
        TimerBucket {
            heap: Mutex::new(BinaryHeap::new()),
        }
    }
}

lazy_static! {
    static ref TIMER_BUCKETS: Vec<TimerBucket> = {
        (0..TIMER_BUCKET_COUNT)
            .map(|_| TimerBucket::new())
            .collect()
    };
    static ref TIMER_THREAD_RUNNING: AtomicBool = AtomicBool::new(false);
    static ref TIMER_ENTRY_COUNT: AtomicUsize = AtomicUsize::new(0);
}

fn get_bucket_id(wake_time: Instant) -> usize {
    let millis = wake_time.duration_since(Instant::now()).as_millis() as usize;
    (millis / TIMER_TICK_MS as usize) % TIMER_BUCKET_COUNT
}

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

fn timer_worker() {
    let mut current_bucket = 0;

    loop {
        let now = Instant::now();
        let mut to_wake = Vec::new();

        let bucket = &TIMER_BUCKETS[current_bucket];
        let mut heap = bucket.heap.lock();

        while let Some(entry) = heap.peek() {
            if entry.wake_time <= now {
                let entry = heap.pop().unwrap();
                TIMER_ENTRY_COUNT.fetch_sub(1, Ordering::Relaxed);
                to_wake.push(entry.g);
            } else {
                break;
            }
        }

        drop(heap);

        for g in to_wake {
            let status = g.status();
            if status == GStatus::Waiting {
                g.set_status(GStatus::Runnable);
                Scheduler::wake_g(g);
            }
        }

        current_bucket = (current_bucket + 1) % TIMER_BUCKET_COUNT;

        if !Scheduler::is_running() {
            let mut total = 0;
            for bucket in TIMER_BUCKETS.iter() {
                total += bucket.heap.lock().len();
            }
            if total == 0 {
                break;
            }
        }

        thread::sleep(Duration::from_millis(TIMER_TICK_MS));
    }

    TIMER_THREAD_RUNNING.store(false, Ordering::SeqCst);
}

pub fn sleep(duration: Duration) {
    if duration.is_zero() {
        Scheduler::yield_now();
        return;
    }

    let wake_time = Instant::now() + duration;
    let bucket_id = get_bucket_id(wake_time);

    match Scheduler::current_g() {
        Some(g) => {
            let entry = TimerEntry {
                wake_time,
                g: g.clone(),
                bucket_id,
            };

            g.set_status(GStatus::Waiting);
            TIMER_ENTRY_COUNT.fetch_add(1, Ordering::Relaxed);

            let bucket = &TIMER_BUCKETS[bucket_id];
            bucket.heap.lock().push(entry);

            while g.status() == GStatus::Waiting {
                Scheduler::yield_now();
                thread::yield_now();
            }
        }
        None => {
            thread::sleep(duration);
        }
    }
}

pub fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms));
}

pub fn shutdown_timer() {
    for bucket in TIMER_BUCKETS.iter() {
        bucket.heap.lock().clear();
    }
    TIMER_ENTRY_COUNT.store(0, Ordering::Relaxed);
    TIMER_THREAD_RUNNING.store(false, Ordering::SeqCst);
}

pub fn pending_timer_count() -> usize {
    TIMER_ENTRY_COUNT.load(Ordering::Relaxed)
}
