use gorust::{go,runtime, Runtime, yield_now, sleep};
use gorust::sync::WaitGroup;

#[runtime]
fn main() {
    println!("=== Basic Goroutine Example ===");

    //  goroutine
    go(|| {
        println!("Hello from goroutine 1!");
    });

    //  goroutine
    for i in 0..5 {
        go(move || {
            println!("Goroutine {} is running", i);
            yield_now(); // 
            println!("Goroutine {} done", i);
        });
    }

    let wg = WaitGroup::new();
    for i in 0..3 {
        wg.add(1);
        let wg_clone = wg.clone();
        go(move || {
            println!("Task {} starting", i);
            sleep(std::time::Duration::from_secs(i));
            println!("Task {} finished", i);
            wg_clone.done();
        });
    }

    wg.wait();
    println!("All tasks completed!");

    println!("\n=== Runtime Statistics ===");
    println!("Active goroutines: {}", Runtime::active_goroutines());
}