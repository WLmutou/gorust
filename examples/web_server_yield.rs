// examples/web_server_yield_now.rs - 修复 Content-Length
use gorust::go;
use gorust::runtime;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

const BODY: &[u8] = b"<html><body>\
                       <h1>Hello from GoRust</h1>\
                       <p>Served by goroutine</p>\
                       </body></html>";

// 自动计算 Content-Length
const CONTENT_LENGTH: usize = BODY.len();

const RESPONSE_HEADER: &[u8] = b"HTTP/1.1 200 OK\r\n\
                                Content-Type: text/html\r\n\
                                Connection: close\r\n";

// 构建完整响应（编译时）
const FULL_RESPONSE: &[u8] = {
    let header_len = RESPONSE_HEADER.len();
    // let content_len_str = format!("Content-Length: {}\r\n\r\n", CONTENT_LENGTH);
    // let content_len_bytes = content_len_str.as_bytes();

    // 这个方法在 const 上下文中不工作，所以运行时构建
    b""
};

// 改用运行时构建（更简单）
fn build_response() -> Vec<u8> {
    let mut response = Vec::with_capacity(512);
    response.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
    response.extend_from_slice(b"Content-Type: text/html\r\n");
    response.extend_from_slice(format!("Content-Length: {}\r\n", BODY.len()).as_bytes());
    response.extend_from_slice(b"Connection: close\r\n");
    response.extend_from_slice(b"\r\n");
    response.extend_from_slice(BODY);
    response
}

lazy_static::lazy_static! {
    static ref RESPONSE: Vec<u8> = build_response();
}

#[runtime]
fn main() -> std::io::Result<()> {
    println!("=== GoRust Web Server (Non-blocking) on :8080 ===");
    println!("Content-Length: {}", BODY.len());

    let listener = TcpListener::bind("127.0.0.1:8080")?;
    listener.set_nonblocking(true)?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                go(move || {
                    handle_connection_nonblocking(stream);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_micros(100));
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_connection_nonblocking(mut stream: TcpStream) {
    let _ = stream.set_nonblocking(true);

    let mut buffer = [0; 1024];
    let mut wait_us = 10;

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(_) => {
                let _ = stream.write_all(&RESPONSE);
                let _ = stream.flush();
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // 指数退避，最多等待 1ms
                std::thread::sleep(std::time::Duration::from_micros(wait_us));
                wait_us = (wait_us * 2).min(500);
                gorust::yield_now();
            }
            Err(_) => return,
        }
    }
}
