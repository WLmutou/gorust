// examples/basic.rs
use gorust::go;
use gorust::runtime;
use gorust::sync::WaitGroup;
use gorust::{Runtime, yield_now};

fn mytestfnc() {
    println!("Hello from mytestfnc!");
}

fn istart(i: i32) {
    println!("Task {} starting", i);
}
fn ifinish(i: i32) {
    println!("Task {} finished", i);
}

#[runtime]
fn main() {
    println!("=== Basic Goroutine Example ===");

    // 简单的 goroutine
    go(|| {
        println!("Hello from goroutine 1!");
        mytestfnc();
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
    for i in 0..300 {
        wg.add(1);
        let wg_clone = wg.clone();
        go(move || {
            istart(i);
            std::thread::sleep(std::time::Duration::from_millis(100));
            ifinish(i);
            wg_clone.done();
        });
    }

    wg.wait();
    println!("All tasks completed!");

    // 打印统计信息
    println!("\n=== Runtime Statistics ===");
    println!("Active goroutines: {}", Runtime::active_goroutines());
}
