use gorust::{go, runtime, sleep};
use gorust::net::{AsyncTcpListener, AsyncTcpStream};
use std::time::Instant;

#[runtime]
fn main() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug"))
        .is_test(true)
        .try_init();
    
    gorust::netpoller::start();

    let start = Instant::now();

    go(move || {
        let listener = AsyncTcpListener::bind("127.0.0.1:8080".parse().unwrap()).unwrap();
        println!("Server listening on :8080");

        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    println!("Accepted connection from {}", addr);
                    go(move || {
                        handle_connection(stream);
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    });

    go(move || {
        sleep(std::time::Duration::from_millis(100));

        let stream = AsyncTcpStream::connect("127.0.0.1:8080".parse().unwrap()).unwrap();
        println!("Client connected");

        let msg = b"Hello from client!";
        stream.write_all(msg).unwrap();
        println!("Client sent: {:?}", String::from_utf8_lossy(msg));

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).unwrap();
        println!("Client received: {:?}", String::from_utf8_lossy(&buf[..n]));
    });

    sleep(std::time::Duration::from_secs(5));
    gorust::netpoller::stop();
    println!("Total time: {:?}", start.elapsed());
}

fn handle_connection(stream: AsyncTcpStream) {
    let mut buf = [0u8; 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                println!("Server echoing {} bytes", n);
                let _ = stream.write_all(&buf[..n]);
            }
            Err(e) => {
                eprintln!("Server error: {}", e);
                break;
            }
        }
    }
}