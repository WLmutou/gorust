// src/net.rs
use crate::netpoller::{self, Interest};
use crate::channel::unbounded;
use std::io::{self, Read, Write};
use std::net::{TcpStream, TcpListener, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::time::Duration;

pub struct AsyncTcpStream {
    stream: TcpStream,
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

        Ok(AsyncTcpStream { stream, fd })
    }

    // ============ 原始 fd I/O（避免 &mut self 约束，无 Mutex 开销）============

    /// 阻塞读取。
    ///
    /// WouldBlock 时注册 FD 到 netpoller，park 当前线程。
    /// FD 就绪后 netpoller 回调 unpark 线程，继续读取。
    /// 用于数据库 I/O 等需要在 goroutine 中透明阻塞的场景。
    pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let ret = unsafe {
                libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
            };
            if ret >= 0 {
                return Ok(ret as usize);
            }
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                self.wait_readable_blocking();
                continue;
            }
            return Err(err);
        }
    }

    /// 非阻塞读取。
    ///
    /// 直接返回 WouldBlock 错误，不 park 线程，不 yield。
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let ret = unsafe {
            libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
        };
        if ret >= 0 {
            return Ok(ret as usize);
        }
        Err(io::Error::last_os_error())
    }

    /// 阻塞写入。
    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let ret = unsafe {
                libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len())
            };
            if ret >= 0 {
                return Ok(ret as usize);
            }
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                self.wait_writable_blocking();
                continue;
            }
            return Err(err);
        }
    }

    /// 非阻塞写入。
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        let ret = unsafe {
            libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len())
        };
        if ret >= 0 {
            return Ok(ret as usize);
        }
        Err(io::Error::last_os_error())
    }

    /// 阻塞写入全部数据。
    pub fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write(buf) {
                Ok(0) => return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                )),
                Ok(n) => buf = &buf[n..],
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    // ============ 阻塞等待（park/unpark）============

    /// 注册 FD 为可读，park 当前线程，等待 FD 就绪。
    fn wait_readable_blocking(&self) {
        let thread = std::thread::current();
        netpoller::register(
            self.fd,
            Interest::READABLE,
            Box::new(move || {
                thread.unpark();
            }),
        );
        std::thread::park();
    }

    /// 注册 FD 为可写，park 当前线程，等待 FD 就绪。
    fn wait_writable_blocking(&self) {
        let thread = std::thread::current();
        netpoller::register(
            self.fd,
            Interest::WRITABLE,
            Box::new(move || {
                thread.unpark();
            }),
        );
        std::thread::park();
    }

    // ============ 协程让出（用于状态机）============

    /// 注册 FD 为可读，让出当前 goroutine。
    pub fn wait_readable_yield(&self) {
        if let Some(g) = crate::scheduler::current_g() {
            netpoller::register(
                self.fd,
                Interest::READABLE,
                Box::new(move || {
                    crate::scheduler::wake_g(g.clone());
                }),
            );
            crate::scheduler::yield_goroutine();
        } else {
            let (tx, rx) = unbounded();
            netpoller::register(
                self.fd,
                Interest::READABLE,
                Box::new(move || {
                    let _ = tx.send(());
                }),
            );
            while rx.try_recv().is_err() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    /// 注册 FD 为可写，让出当前 goroutine。
    pub fn wait_writable_yield(&self) {
        if let Some(g) = crate::scheduler::current_g() {
            netpoller::register(
                self.fd,
                Interest::WRITABLE,
                Box::new(move || {
                    crate::scheduler::wake_g(g.clone());
                }),
            );
            crate::scheduler::yield_goroutine();
        } else {
            let (tx, rx) = unbounded();
            netpoller::register(
                self.fd,
                Interest::WRITABLE,
                Box::new(move || {
                    let _ = tx.send(());
                }),
            );
            while rx.try_recv().is_err() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.stream.set_nodelay(nodelay)
    }

    pub fn try_clone(&self) -> io::Result<AsyncTcpStream> {
        let cloned = self.stream.try_clone()?;
        let fd = cloned.as_raw_fd();
        Ok(AsyncTcpStream { stream: cloned, fd })
    }

    pub fn fd(&self) -> RawFd {
        self.fd
    }
}

unsafe impl Send for AsyncTcpStream {}
unsafe impl Sync for AsyncTcpStream {}

// ============ TcpListener (同步) ============

pub struct AsyncTcpListener {
    fd: RawFd,
}

impl AsyncTcpListener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let fd = listener.as_raw_fd();
        std::mem::forget(listener); // 只保留 fd，避免 drop 时关闭 fd
        Ok(AsyncTcpListener { fd })
    }

    /// 同步阻塞 accept：park 线程等待新连接，由 netpoller 回调 unpark
    pub fn accept(&self) -> io::Result<(AsyncTcpStream, SocketAddr)> {
        loop {
            let ret = unsafe {
                libc::accept4(self.fd, std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_NONBLOCK)
            };
            if ret >= 0 {
                let stream = unsafe { TcpStream::from_raw_fd(ret) };
                let addr = stream.peer_addr().ok().unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
                let fd = ret;
                return Ok((AsyncTcpStream { stream, fd }, addr));
            }
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                let thread = std::thread::current();
                netpoller::register(
                    self.fd,
                    Interest::READABLE,
                    Box::new(move || {
                        thread.unpark();
                    }),
                );
                std::thread::park();
                continue;
            }
            return Err(err);
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // Use getsockname
        let mut addr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
        let mut addrlen: libc::socklen_t = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        let ret = unsafe {
            libc::getsockname(self.fd, &mut addr as *mut _ as *mut libc::sockaddr, &mut addrlen)
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        let port = u16::from_be(addr.sin_port);
        let ip_bytes = addr.sin_addr.s_addr.to_ne_bytes();
        let ip = std::net::Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]);
        Ok(SocketAddr::from((ip, port)))
    }

    pub fn fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for AsyncTcpListener {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

// ============ Read/Write trait 实现（供 WebSocket 等使用）============

impl Read for AsyncTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let ret = unsafe {
            libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
        };
        if ret >= 0 {
            return Ok(ret as usize);
        }
        Err(io::Error::last_os_error())
    }
}

impl Write for AsyncTcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let ret = unsafe {
            libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len())
        };
        if ret >= 0 {
            return Ok(ret as usize);
        }
        Err(io::Error::last_os_error())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}