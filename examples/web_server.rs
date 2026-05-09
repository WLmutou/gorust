// examples/web_server.rs
use gorust::go;
use gorust::runtime;
use gorust::sync::WaitGroup;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

#[runtime]
fn main() -> std::io::Result<()> {
    println!("=== Simple Web Server on :8080 ===");

    let listener = TcpListener::bind("127.0.0.1:8080")?;

    go(move || {
        let wg = WaitGroup::new();
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    wg.add(1);
                    let wg_clone = wg.clone();
                    go(move || {
                        handle_connection(stream);
                        wg_clone.done();
                    });
                }
                Err(e) => {
                    println!("Connection failed: {}", e);
                }
            }
        }
        wg.wait();
        println!("Server stopped accepting connections");
    });

    println!("Press Ctrl+C to stop...");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer).unwrap();

    let response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n\
                   <html><body>\
                   <h1>Hello from GoRust</h1>\
                   <p>Served by goroutine</p>\
                   </body></html>";

    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}
