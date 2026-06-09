// src/netpoller.rs
pub use mio::{Events, Interest, Poll, Token, Waker};
use mio::unix::SourceFd;
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use parking_lot::Mutex;
use crate::channel::{unbounded, UnboundedSender, UnboundedReceiver};
use lazy_static::lazy_static;

const TOKEN_START: usize = 1;
const WAKER_TOKEN: Token = Token(0);

pub type EventType = Interest;

type EventCallback = Box<dyn FnOnce() + Send + 'static>;

#[allow(dead_code)]
enum Command {
    Register { fd: RawFd, interests: Interest, callback: EventCallback },
    Unregister { token: Token },
    Shutdown,
}

struct NetpollerInner {
    poll: Option<Poll>,
    cmd_rx: Option<UnboundedReceiver<Command>>,
    waker: Waker,
    pending: HashMap<Token, (EventCallback, RawFd)>,
    next_token: AtomicUsize,
    running: Arc<AtomicBool>,
    cmd_tx: UnboundedSender<Command>,
}

lazy_static! {
    static ref NETPOLLER: Mutex<NetpollerInner> = Mutex::new(NetpollerInner::new().unwrap());
}

impl NetpollerInner {
    fn new() -> io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Waker::new(poll.registry(), WAKER_TOKEN)?;
        let (cmd_tx, cmd_rx) = unbounded();

        Ok(NetpollerInner {
            poll: Some(poll),
            cmd_rx: Some(cmd_rx),
            waker,
            pending: HashMap::new(),
            next_token: AtomicUsize::new(TOKEN_START),
            running: Arc::new(AtomicBool::new(false)),
            cmd_tx,
        })
    }

    fn event_loop(mut poll: Poll, cmd_rx: UnboundedReceiver<Command>, running: Arc<AtomicBool>) {
        let mut events = Events::with_capacity(1024);

        while running.load(Ordering::Relaxed) {
            {
                let mut np = NETPOLLER.lock();

                while let Ok(cmd) = cmd_rx.try_recv() {
                    match cmd {
                        Command::Register { fd, interests, callback } => {
                            let token = Token(np.next_token.fetch_add(1, Ordering::Relaxed));
                            np.pending.insert(token, (callback, fd));

                            let mut source_fd = SourceFd(&fd);
                            // 先尝试 reregister（FD 已注册时更新 interest），
                            // 失败则说明是新 FD，用 register
                            if let Err(e) = poll.registry().reregister(&mut source_fd, token, interests) {
                                if e.kind() == io::ErrorKind::NotFound {
                                    if let Err(e) = poll.registry().register(&mut source_fd, token, interests) {
                                        eprintln!("NETPOLLER: Failed to register fd {}: {}", fd, e);
                                    }
                                } else {
                                    eprintln!("NETPOLLER: Failed to reregister fd {}: {}", fd, e);
                                }
                            }
                        }
                        Command::Unregister { token } => {
                            if let Some((_, fd)) = np.pending.remove(&token) {
                                let mut source_fd = SourceFd(&fd);
                                let _ = poll.registry().deregister(&mut source_fd);
                            }
                        }
                        Command::Shutdown => {
                            return;
                        }
                    }
                }
            }

            match poll.poll(&mut events, Some(Duration::from_millis(1))) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    eprintln!("NETPOLLER: Poll error: {}", e);
                    continue;
                }
            }

            let mut callbacks: Vec<EventCallback> = Vec::new();
            {
                let mut np = NETPOLLER.lock();
                for event in events.iter() {
                    match event.token() {
                        WAKER_TOKEN => continue,
                        token => {
                            if let Some((callback, fd)) = np.pending.remove(&token) {
                                callbacks.push(callback);
                                let mut source_fd = SourceFd(&fd);
                                let _ = poll.registry().deregister(&mut source_fd);
                            }
                        }
                    }
                }
            }

            for callback in callbacks {
                callback();
            }
        }
    }

    pub fn start() {
        let mut np = NETPOLLER.lock();
        if np.running.load(Ordering::Relaxed) {
            return;
        }
        np.running.store(true, Ordering::Relaxed);

        let poll = np.poll.take().expect("Netpoller already started");
        let cmd_rx = np.cmd_rx.take().expect("Netpoller already started");
        let running = np.running.clone();

        std::thread::spawn(move || {
            Self::event_loop(poll, cmd_rx, running);
        });
    }

    pub fn register(fd: RawFd, interests: Interest, callback: EventCallback) {
        let np = NETPOLLER.lock();
        let _ = np.cmd_tx.send(Command::Register { fd, interests, callback });
        let _ = np.waker.wake();
    }

    pub fn stop() {
        let np = NETPOLLER.lock();
        if np.running.load(Ordering::Relaxed) {
            np.running.store(false, Ordering::Relaxed);
            let _ = np.cmd_tx.send(Command::Shutdown);
            let _ = np.waker.wake();
        }
    }
}

pub fn register(fd: RawFd, interests: Interest, callback: EventCallback) {
    NetpollerInner::register(fd, interests, callback)
}

pub fn start() {
    NetpollerInner::start();
}

pub fn stop() {
    NetpollerInner::stop();
}
