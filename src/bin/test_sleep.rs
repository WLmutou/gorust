use gorust::{go, Runtime, sleep_ms};
use std::time::Instant;

fn main() {
    println!("Starting test_sleep...");
    Runtime::init();

    let start = Instant::now();

    for i in 0..5 {
        go(move || {
            println!("Goroutine {} started", i);
            sleep_ms(50);
            println!("Goroutine {} finished after 50ms", i);
        });
    }

    Runtime::wait_for_all();
    let elapsed = start.elapsed();
    println!("All goroutines completed in {:?}", elapsed);
}