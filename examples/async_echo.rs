// examples/async_echo.rs
use gorust::{go, runtime, sleep};
use gorust::net::{AsyncTcpListener, AsyncTcpStream};
use std::time::Instant;

#[runtime]
fn main() {
    // 启动 netpoller
    gorust::netpoller::start();
    
    let start = Instant::now();
    
    // 启动服务器
    go(async {
        let listener = AsyncTcpListener::bind("127.0.0.1:8080".parse().unwrap()).unwrap();
        println!("Server listening on :8080");
        
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    println!("Accepted connection from {}", addr);
                    go(async move {
                        handle_connection(stream).await;
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    });
    
    // 启动客户端
    go(async {
        sleep(std::time::Duration::from_millis(100)).await;
        
        let mut stream = AsyncTcpStream::connect("127.0.0.1:8080".parse().unwrap()).await.unwrap();
        println!("Client connected");
        
        let msg = b"Hello from client!";
        stream.write_all(msg).await.unwrap();
        println!("Client sent: {:?}", String::from_utf8_lossy(msg));
        
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        println!("Client received: {:?}", String::from_utf8_lossy(&buf[..n]));
    });
    
    // 保持运行
    sleep(std::time::Duration::from_secs(5));
    gorust::netpoller::stop();
    println!("Total time: {:?}", start.elapsed());
}

async fn handle_connection(mut stream: AsyncTcpStream) {
    let mut buf = [0u8; 1024];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                println!("Server echoing {} bytes", n);
                let _ = stream.write_all(&buf[..n]).await;
            }
            Err(e) => {
                eprintln!("Server error: {}", e);
                break;
            }
        }
    }
}