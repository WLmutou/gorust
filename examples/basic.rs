// examples/basic.rs
use gorust::{go,runtime, Runtime, yield_now};
use gorust::sync::WaitGroup;


#[runtime]
fn main() {
    println!("=== Basic Goroutine Example ===");

    // 简单的 goroutine
    go(|| {
        println!("Hello from goroutine 1!");
    });

    // 带参数的 goroutine
    for i in 0..5 {
        go(move || {
            println!("Goroutine {} is running", i);
            yield_now(); // 主动让出 CPU
            println!("Goroutine {} done", i);
        });
    }

    // 使用 WaitGroup
    let wg = WaitGroup::new();
    for i in 0..3 {
        wg.add(1);
        let wg_clone = wg.clone();
        go(move || {
            println!("Task {} starting", i);
            std::thread::sleep(std::time::Duration::from_millis(100 * i));
            println!("Task {} finished", i);
            wg_clone.done();
        });
    }

    wg.wait();
    println!("All tasks completed!");

    // 打印统计信息
    println!("\n=== Runtime Statistics ===");
    println!("Active goroutines: {}", Runtime::active_goroutines());
}
