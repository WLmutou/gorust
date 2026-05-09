// examples/sleep_demo.rs
use gorust::{go, runtime, timer};
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[runtime]
fn main() {
    // 初始化日志
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();
    
    let start = Instant::now();
    let completed = Arc::new(AtomicUsize::new(0));
    
    println!("Starting 5 goroutines with different sleep times...\n");
    
    for i in 1..=5 {
        let delay = i;
        let completed_clone = completed.clone();
        let task_start = Instant::now();
        
        go(move || {
            let before_sleep = Instant::now();
            println!("[G{}] 🛌 Going to sleep for {}s...", delay, delay);
            
            timer::sleep_ms(delay * 1000);
            
            let after_sleep = Instant::now();
            let actual_sleep = after_sleep - before_sleep;
            
            println!(
                "[G{}] ✅ Woke up after {}s (actual sleep: {:.3}s, time since spawn: {:.3}s)", 
                delay,
                delay,
                actual_sleep.as_secs_f64(),
                task_start.elapsed().as_secs_f64()
            );
            
            completed_clone.fetch_add(1, Ordering::Relaxed);
        });
    }
    
    println!("Main thread waiting for all goroutines...\n");
    
    // 使用一个简单的循环等待，而不是 sleep
    while completed.load(Ordering::Relaxed) < 5 {
        // 短暂让出 CPU，避免忙等待
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    
    let total_time = start.elapsed();
    println!("\n📊 Results:");
    println!("   Total time: {:.3}s", total_time.as_secs_f64());
    println!("   Expected sequential time: 15s");
    println!("   Expected concurrent time: ~5s");
    
    if total_time.as_secs_f64() < 6.0 {
        println!("   ✅ Success! True concurrent execution achieved!");
    } else if total_time.as_secs_f64() < 10.0 {
        println!("   ⚠️  Partial concurrency with overhead");
    } else {
        println!("   ❌ Poor concurrency");
    }
}