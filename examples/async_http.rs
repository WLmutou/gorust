// examples/async_http.rs
use gorust::{go, runtime, sleep};
use gorust::net::AsyncTcpStream;
use std::time::Instant;

#[runtime]
fn main() {
    // 启动 netpoller
    gorust::netpoller::start();
    
    let start = Instant::now();
    
    // 并发发起多个 HTTP 请求
    for i in 0..10 {
        go(async move {
            println!("Request {} starting", i);
            
            // 模拟异步 HTTP 请求
            let stream = AsyncTcpStream::connect("93.184.216.34:80".parse().unwrap()).await;
            
            match stream {
                Ok(_) => {
                    println!("Request {} connected", i);
                    sleep(std::time::Duration::from_millis(100 * i));
                    println!("Request {} completed", i);
                }
                Err(e) => {
                    println!("Request {} failed: {}", i, e);
                }
            }
        });
    }
    
    // 等待所有请求完成
    sleep(std::time::Duration::from_secs(5));
    
    println!("Total time: {:?}", start.elapsed());
    gorust::netpoller::stop();
}