# gorust
Go-style concurrency in Rust

## Overview

`gorust` is an experimental library that brings Go-style concurrency patterns to Rust. The primary goal is to provide Rust developers with familiar concurrency primitives similar to those found in Go, particularly Goroutines and channels, enabling lightweight concurrent programming experiences.

This library aims to bridge the gap between Rust's native concurrency mechanisms and Go's elegant concurrency model, potentially reducing the learning curve for developers transitioning between these languages or wanting to leverage Go-style concurrency patterns in Rust.

## Features

- Go-style goroutines implementation in Rust
- Channel communication between concurrent tasks
- Lightweight concurrency similar to Go's approach
- Familiar API for developers experienced with Go concurrency

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
gorust = { git = "https://github.com/WLmutou/gorust.git" } # Replace with actual repository
```

Or when it's published on crates.io:

```toml
[dependencies]
gorust = "0.1.2" # Replace with actual version
```

## Usage

Here's a simple example demonstrating how to use gorust:

```rust
use gorust::{runtime, go, channel};

#[runtime]
fn main() {
    let (tx, rx) = channel::new();
    
    go(move || {
        // Simulate some work
        std::thread::sleep(std::time::Duration::from_millis(100));
        tx.send("Hello from goroutine!".to_string()).unwrap();
    });
    
    let message = rx.recv().unwrap();
    println!("{}", message);
}
```

```rust
use gorust::{go,runtime, Runtime, yield_now};
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
            std::thread::sleep(std::time::Duration::from_millis(100 * i));
            println!("Task {} finished", i);
            wg_clone.done();
        });
    }

    wg.wait();
    println!("All tasks completed!");

    println!("\n=== Runtime Statistics ===");
    println!("Active goroutines: {}", Runtime::active_goroutines());
}

```

## Roadmap

- [ ] Implement basic goroutine functionality
- [ ] Add channel communication primitives
- [ ] Provide select-like functionality similar to Go
- [ ] Add comprehensive examples
- [ ] Document the API thoroughly
- [ ] Write tests and benchmarks

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, open an issue first to discuss what you would like to change.

## License

MIT License. See [LICENSE](./LICENSE) for more details.