// src/netpoller.rs
use mio::{Events, Poll, Token, Waker};
use mio::net::TcpStream as MioTcpStream;
use std::collections::HashMap;
use std::io;
use std::os::fd::FromRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use parking_lot::Mutex;
use crossbeam::channel::{unbounded, Sender, Receiver};
use lazy_static::lazy_static;

pub use mio::Interest;

// Token 分配
const TOKEN_START: usize = 1;
const WAKER_TOKEN: Token = Token(0);

/// 事件类型（兼容层）
pub type EventType = mio::Interest;

/// 事件回调
type EventCallback = Box<dyn FnOnce() + Send + 'static>;

/// 命令类型
#[allow(dead_code)]
enum Command {
    Register { fd: std::os::unix::io::RawFd, interests: Interest, callback: EventCallback },
    Unregister { token: Token },
    Shutdown,
}

/// Netpoller 核心
pub struct Netpoller {
    poll: Poll,
    events: Events,
    waker: Waker,
    pending: HashMap<Token, (EventCallback, std::os::unix::io::RawFd)>,
    next_token: AtomicUsize,
    running: Arc<AtomicBool>,
    cmd_tx: Sender<Command>,
    cmd_rx: Receiver<Command>,
}

lazy_static! {
    static ref NETPOLLER: Mutex<Netpoller> = Mutex::new(Netpoller::new().unwrap());
}

impl Netpoller {
    fn new() -> io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Waker::new(poll.registry(), WAKER_TOKEN)?;
        let (cmd_tx, cmd_rx) = unbounded();
        
        Ok(Netpoller {
            poll,
            events: Events::with_capacity(1024),
            waker,
            pending: HashMap::new(),
            next_token: AtomicUsize::new(TOKEN_START),
            running: Arc::new(AtomicBool::new(false)),
            cmd_tx,
            cmd_rx,
        })
    }
    
    /// 启动 netpoller 线程
    pub fn start() {
        let np = NETPOLLER.lock();
        if np.running.load(Ordering::Relaxed) {
            return;
        }
        np.running.store(true, Ordering::Relaxed);
        
        let cmd_rx = np.cmd_rx.clone();
        let running = np.running.clone();
        
        std::thread::spawn(move || {
            Self::event_loop(cmd_rx, running);
        });
    }
    
    fn event_loop(cmd_rx: Receiver<Command>, running: Arc<AtomicBool>) {
        let mut local_events = Events::with_capacity(1024);
        
        while running.load(Ordering::Relaxed) {
            {
                let mut np = NETPOLLER.lock();
                
                while let Ok(cmd) = cmd_rx.try_recv() {
                    match cmd {
                        Command::Register { fd, interests, callback } => {
                            let token = Token(np.next_token.fetch_add(1, Ordering::Relaxed));
                            np.pending.insert(token, (callback, fd));
                            
                            unsafe {
                                let mut stream = MioTcpStream::from_std(std::net::TcpStream::from_raw_fd(fd));
                                if let Err(e) = np.poll.registry().register(&mut stream, token, interests) {
                                    eprintln!("Failed to register fd {}: {}", fd, e);
                                }
                                std::mem::forget(stream);
                            }
                        }
                        Command::Unregister { token } => {
                            if let Some((_, fd)) = np.pending.remove(&token) {
                                unsafe {
                                    let _ = np.poll.registry().deregister(&mut MioTcpStream::from_std(std::net::TcpStream::from_raw_fd(fd)));
                                }
                            }
                        }
                        Command::Shutdown => {
                            return;
                        }
                    }
                }
                
                std::mem::swap(&mut np.events, &mut local_events);
                let poll_result = np.poll.poll(&mut local_events, Some(Duration::from_millis(100)));
                std::mem::swap(&mut np.events, &mut local_events);
                
                let mut callbacks: Vec<EventCallback> = Vec::new();
                match poll_result {
                    Ok(_) => {
                        for event in local_events.iter() {
                            match event.token() {
                                WAKER_TOKEN => continue,
                                token => {
                                    if let Some((callback, _fd)) = np.pending.remove(&token) {
                                        callbacks.push(callback);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => eprintln!("Poll error: {}", e),
                }
                
                for callback in callbacks {
                    callback();
                }
            }
        }
    }
    
    /// 注册事件（公开 API）
    pub fn register(fd: std::os::unix::io::RawFd, interests: Interest, callback: EventCallback) {
        let np = NETPOLLER.lock();
        let _ = np.cmd_tx.send(Command::Register { fd, interests, callback });
    }
    
    /// 唤醒事件循环
    pub fn wake() {
        let np = NETPOLLER.lock();
        let _ = np.waker.wake();
    }
    
    /// 停止 netpoller
    pub fn stop() {
        let np = NETPOLLER.lock();
        if np.running.load(Ordering::Relaxed) {
            np.running.store(false, Ordering::Relaxed);
            let _ = np.cmd_tx.send(Command::Shutdown);
            let _ = np.waker.wake();
        }
    }
}

pub fn register(fd: std::os::unix::io::RawFd, interests: Interest, callback: EventCallback) {
    Netpoller::register(fd, interests, callback)
}

pub fn start() {
    Netpoller::start();
}

pub fn stop() {
    Netpoller::stop();
}