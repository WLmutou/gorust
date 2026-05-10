// src/net.rs
use crate::netpoller::{self, Interest};
use crate::scheduler;
use std::io::{self, Read, Write};
use std::net::{TcpStream, TcpListener, SocketAddr};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use parking_lot::Mutex;
use crate::channel::unbounded;

/// 异步 TCP 流
pub struct AsyncTcpStream {
    inner: Arc<Mutex<TcpStream>>,
    #[allow(dead_code)]
    connected: Arc<AtomicBool>,
    #[allow(dead_code)]
    fd: usize,
}

impl AsyncTcpStream {
    /// 异步连接到远程地址
    pub fn connect(addr: SocketAddr) -> io::Result<Self> {
        // 创建标准 TCP 流
        let stream = TcpStream::connect(addr)?;
        
        // 设置为非阻塞模式（纯 Rust 方法）
        stream.set_nonblocking(true)?;
        
        let fd = stream.as_raw_fd() as usize;
        
        // 检查连接是否立即完成
        match stream.take_error()? {
            Some(e) => return Err(e),
            None => {
                // 连接可能还在进行中
            }
        }
        
        let stream = Arc::new(Mutex::new(stream));
        let connected = Arc::new(AtomicBool::new(true));
        
        Ok(AsyncTcpStream {
            inner: stream,
            connected,
            fd,
        })
    }
    
    /// 异步读取
    pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.inner.lock().read(buf) {
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
            match self.inner.lock().write(buf) {
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
        let fd = self.inner.lock().as_raw_fd();
        
        // 注册可读事件
        netpoller::register(
            fd,
            Interest::READABLE,
            Box::new(move || {
                let _ = tx.send(());
            }),
        );
        
        // 让出 CPU，等待事件
        while rx.try_recv().is_err() {
            scheduler::yield_now();
        }
    }
    
    /// 等待可写
    fn wait_writable(&self) {
        let (tx, rx) = unbounded();
        let fd = self.inner.lock().as_raw_fd();
        
        netpoller::register(
            fd,
            Interest::WRITABLE,
            Box::new(move || {
                let _ = tx.send(());
            }),
        );
        
        while rx.try_recv().is_err() {
            scheduler::yield_now();
        }
    }
    
    /// 获取本地地址
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().local_addr()
    }
    
    /// 获取远程地址
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().peer_addr()
    }
}

/// 异步 TCP 监听器
pub struct AsyncTcpListener {
    inner: Arc<Mutex<TcpListener>>,
    fd: usize,
}

impl AsyncTcpListener {
    /// 绑定并监听地址
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let fd = listener.as_raw_fd() as usize;
        
        Ok(AsyncTcpListener {
            inner: Arc::new(Mutex::new(listener)),
            fd,
        })
    }
    
    /// 异步接受连接
    pub fn accept(&self) -> io::Result<(AsyncTcpStream, SocketAddr)> {
        loop {
            match self.inner.lock().accept() {
                Ok((stream, addr)) => {
                    stream.set_nonblocking(true)?;
                    return Ok((
                        AsyncTcpStream {
                            inner: Arc::new(Mutex::new(stream)),
                            connected: Arc::new(AtomicBool::new(true)),
                            fd: self.fd,
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
        let fd = self.inner.lock().as_raw_fd();
        
        netpoller::register(
            fd,
            Interest::READABLE,
            Box::new(move || {
                let _ = tx.send(());
            }),
        );
        
        while rx.try_recv().is_err() {
            scheduler::yield_now();
        }
    }
    
    /// 获取本地地址
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().local_addr()
    }
}

/// 简化的异步函数宏（类似 async/await 语法糖）
#[macro_export]
macro_rules! async_fn {
    ($name:ident($($arg:ident: $ty:ty),*) $body:block) => {
        fn $name($($arg: $ty),*) -> impl std::future::Future<Output = ()> {
            async move $body
        }
    };
}