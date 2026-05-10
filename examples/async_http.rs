use gorust::{go, runtime, sleep};
use gorust::net::AsyncTcpStream;
use std::time::Instant;

#[runtime]
fn main() {
    gorust::netpoller::start();

    let start = Instant::now();

    for i in 0..10 {
        go(move || {
            println!("Request {} starting", i);

            let stream = AsyncTcpStream::connect("93.184.216.34:80".parse().unwrap());

            match stream {
                Ok(_) => {
                    println!("Request {} connected", i);
                    sleep(std::time::Duration::from_millis(100 * i as u64));
                    println!("Request {} completed", i);
                }
                Err(e) => {
                    println!("Request {} failed: {}", i, e);
                }
            }
        });
    }

    sleep(std::time::Duration::from_secs(5));

    println!("Total time: {:?}", start.elapsed());
    gorust::netpoller::stop();
}