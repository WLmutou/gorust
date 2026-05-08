// examples/web_server_mio_rgo.rs
use gorust::runtime;
use gorust::go;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::io::{Read, Write};
use std::collections::HashMap;
use std::sync::Mutex;
use lazy_static::lazy_static;

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\n\
                          Content-Type: text/html\r\n\
                          Content-Length: 94\r\n\
                          Connection: close\r\n\
                          \r\n\
                          <html><body>\
                          <h1>Hello from GoRust + Mio</h1>\
                          <p>Served by goroutine</p>\
                          </body></html>";

// 全局 Reactor
lazy_static! {
    static ref REACTOR: Mutex<Reactor> = Mutex::new(Reactor::new().unwrap());
}

struct Reactor {
    poll: Poll,
    events: Events,
    streams: HashMap<Token, TcpStream>,
    next_token: usize,
}

impl Reactor {
    fn new() -> std::io::Result<Self> {
        Ok(Reactor {
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            streams: HashMap::new(),
            next_token: 1,
        })
    }
    
    fn register(&mut self, mut stream: TcpStream) -> Token {
        let token = Token(self.next_token);
        self.next_token += 1;
        
        self.poll.registry()
            .register(&mut stream, token, Interest::READABLE)
            .unwrap();
        
        self.streams.insert(token, stream);
        token
    }
    
    fn wait_events(&mut self) -> Vec<Token> {
        self.poll.poll(&mut self.events, None).unwrap();
        
        self.events.iter()
            .filter(|e| e.is_readable())
            .map(|e| e.token())
            .collect()
    }
    
    fn remove_stream(&mut self, token: Token) -> Option<TcpStream> {
        self.streams.remove(&token)
    }
}

#[runtime]
fn main() -> std::io::Result<()> {
    println!("=== GoRust + Mio Async Server on :8080 ===");
    
    let mut listener = TcpListener::bind("127.0.0.1:8080".parse().unwrap())?;
    let mut reactor = REACTOR.lock().unwrap();
    let listener_token = Token(0);
    reactor.poll.registry().register(&mut listener, listener_token, Interest::READABLE)?;
    
    // 启动 Reactor 循环
    go(move || {
        reactor_loop(listener);
    });
    
    // 保持运行
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn reactor_loop(listener: TcpListener) {
    loop {
        // 等待事件
        let tokens = {
            let mut reactor = REACTOR.lock().unwrap();
            reactor.wait_events()
        };
        
        for token in tokens {
            if token == Token(0) {
                // 接受新连接
                let mut reactor = REACTOR.lock().unwrap();
                   
                while let Ok((stream, _)) = listener.accept() {
                    let token = reactor.register(stream);
                    println!("New connection: {:?}", token);
                }
            } else {
                // 数据就绪，在 rgo 协程中处理
                let stream = {
                    let mut reactor = REACTOR.lock().unwrap();
                    reactor.remove_stream(token)
                };
                
                if let Some(stream) = stream {
                    go(move || {
                        handle_connection_rgo(stream);
                    });
                }
            }
        }
    }
}

fn handle_connection_rgo(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    
    // 读取请求
    match stream.read(&mut buffer) {
        Ok(_) => {
            let _ = stream.write_all(RESPONSE);
            let _ = stream.flush();
        }
        Err(e) => {
            eprintln!("Error: {}", e);
        }
    }
}