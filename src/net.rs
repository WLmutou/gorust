// src/net.rs
use crate::netpoller::{self, Interest};
use crate::scheduler;
use std::io::{self, Read, Write};
use std::net::{TcpStream, TcpListener, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use parking_lot::Mutex;
use crate::channel::unbounded;

pub struct AsyncTcpStream {
    inner: Arc<Mutex<TcpStream>>,
    #[allow(dead_code)]
    connected: Arc<AtomicBool>,
    fd: RawFd,
}

impl AsyncTcpStream {
    pub fn connect(addr: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        let fd = stream.as_raw_fd();

        match stream.take_error()? {
            Some(e) => return Err(e),
            None => {}
        }

        let stream = Arc::new(Mutex::new(stream));
        let connected = Arc::new(AtomicBool::new(true));

        Ok(AsyncTcpStream {
            inner: stream,
            connected,
            fd,
        })
    }

    pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let result = self.inner.lock().read(buf);
            match result {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_readable();
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let result = self.inner.lock().write(buf);
            match result {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_writable();
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.write(buf)?;
            buf = &buf[n..];
        }
        Ok(())
    }

    fn wait_readable(&self) {
        let (tx, rx) = unbounded();

        netpoller::register(
            self.fd,
            Interest::READABLE,
            Box::new(move || {
                let _ = tx.send(());
            }),
        );

        while rx.try_recv().is_err() {
            scheduler::yield_now();
        }
    }

    fn wait_writable(&self) {
        let (tx, rx) = unbounded();

        netpoller::register(
            self.fd,
            Interest::WRITABLE,
            Box::new(move || {
                let _ = tx.send(());
            }),
        );

        while rx.try_recv().is_err() {
            scheduler::yield_now();
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().peer_addr()
    }
}

pub struct AsyncTcpListener {
    inner: Arc<Mutex<TcpListener>>,
    fd: RawFd,
}

impl AsyncTcpListener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let fd = listener.as_raw_fd();

        Ok(AsyncTcpListener {
            inner: Arc::new(Mutex::new(listener)),
            fd,
        })
    }

    pub fn accept(&self) -> io::Result<(AsyncTcpStream, SocketAddr)> {
        loop {
            let result = self.inner.lock().accept();
            match result {
                Ok((stream, addr)) => {
                    stream.set_nonblocking(true)?;
                    let fd = stream.as_raw_fd();
                    return Ok((
                        AsyncTcpStream {
                            inner: Arc::new(Mutex::new(stream)),
                            connected: Arc::new(AtomicBool::new(true)),
                            fd,
                        },
                        addr,
                    ));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_readable();
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn wait_readable(&self) {
        let (tx, rx) = unbounded();

        netpoller::register(
            self.fd,
            Interest::READABLE,
            Box::new(move || {
                let _ = tx.send(());
            }),
        );

        while rx.try_recv().is_err() {
            scheduler::yield_now();
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().local_addr()
    }
}

#[macro_export]
macro_rules! async_fn {
    ($name:ident($($arg:ident: $ty:ty),*) $body:block) => {
        fn $name($($arg: $ty),*) -> impl std::future::Future<Output = ()> {
            async move $body
        }
    };
}
