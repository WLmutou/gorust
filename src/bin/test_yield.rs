/// 测试 yield_to_scheduler 是否正常工作
use gorust::{go, Runtime};
use std::time::Instant;
use std::io::Write;

fn main() {
    println!("Starting test_yield...");
    std::io::stdout().flush().ok();
    Runtime::init();

    let start = Instant::now();

    for i in 0..3 {
        go(move || {
            println!("Goroutine {}: before yield_to_scheduler (GID:{:?})", i, gorust::scheduler::current_g().map(|g| g.id));
            std::io::stdout().flush().ok();
            // 直接调用 yield_to_scheduler
            gorust::scheduler::yield_to_scheduler();
            println!("Goroutine {}: after yield_to_scheduler (GID:{:?})", i, gorust::scheduler::current_g().map(|g| g.id));
            std::io::stdout().flush().ok();
        });
    }

    // 使用超时，避免无限等待
    let timeout = std::time::Duration::from_secs(5);
    let wait_start = Instant::now();
    loop {
        let active = gorust::Runtime::active_goroutines();
        let pending = gorust::scheduler::pending_goroutines();
        if active == 0 && pending == 0 {
            break;
        }
        if wait_start.elapsed() > timeout {
            println!("TIMEOUT after {:?}: active={}, pending={}", wait_start.elapsed(), active, pending);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    let elapsed = start.elapsed();
    println!("All goroutines completed in {:?}", elapsed);
}