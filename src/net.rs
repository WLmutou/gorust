// src/net.rs
use crate::netpoller::{self, EventType, Interest};
use crate::scheduler;
use std::io::{self, Read, Write};
use std::net::{TcpStream, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use parking_lot::Mutex;
use crossbeam::channel::{unbounded, Sender, Receiver};

/// 异步 TCP 流
pub struct AsyncTcpStream {
    inner: Arc<Mutex<TcpStream>>,
    connected: Arc<AtomicBool>,
    fd: usize, // 用于调试的 ID
}

impl AsyncTcpStream {
    /// 异步连接到远程地址
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        // 创建标准 TCP 流
        let mut stream = TcpStream::connect(addr)?;
        
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
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            // 尝试非阻塞读取
            match self.inner.lock().read(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // 需要等待可读事件
                    self.wait_readable().await;
                    // 继续循环，再次尝试读取
                }
                Err(e) => return Err(e),
            }
        }
    }
    
    /// 异步写入
    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            match self.inner.lock().write(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_writable().await;
                }
                Err(e) => return Err(e),
            }
        }
    }
    
    /// 异步写入全部数据
    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.write(buf).await?;
            buf = &buf[n..];
        }
        Ok(())
    }
    
    /// 等待可读
    async fn wait_readable(&self) {
        let (tx, rx) = unbounded();
        let fd = self.inner.lock().as_raw_fd();
        
        // 注册可读事件
        netpoller::register(
            fd,
            Interest::READABLE,
            Box::new(move |_| {
                let _ = tx.send(());
            }),
        );
        
        // 让出 CPU，等待事件
        while rx.try_recv().is_err() {
            scheduler::yield_now();
        }
    }
    
    /// 等待可写
    async fn wait_writable(&self) {
        let (tx, rx) = unbounded();
        let fd = self.inner.lock().as_raw_fd();
        
        netpoller::register(
            fd,
            Interest::WRITABLE,
            Box::new(move |_| {
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
    pub async fn accept(&self) -> io::Result<(AsyncTcpStream, SocketAddr)> {
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
                    self.wait_readable().await;
                }
                Err(e) => return Err(e),
            }
        }
    }
    
    /// 等待可读
    async fn wait_readable(&self) {
        let (tx, rx) = unbounded();
        let fd = self.inner.lock().as_raw_fd();
        
        netpoller::register(
            fd,
            Interest::READABLE,
            Box::new(move |_| {
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