use crate::scheduler::{Scheduler, G, GStatus};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    Disconnected(T),
    Full(T),
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Disconnected(_) => write!(f, "channel disconnected"),
            SendError::Full(_) => write!(f, "channel full"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    Disconnected,
    Empty,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Disconnected => write!(f, "channel disconnected"),
            RecvError::Empty => write!(f, "channel empty"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "channel empty"),
            TryRecvError::Disconnected => write!(f, "channel disconnected"),
        }
    }
}

struct ChannelInner<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    closed: bool,
    send_waiters: VecDeque<Arc<G>>,
    recv_waiters: VecDeque<Arc<G>>,
    pending_value: Option<T>,
}

pub struct Sender<T> {
    inner: Arc<Mutex<ChannelInner<T>>>,
}

pub struct Receiver<T> {
    inner: Arc<Mutex<ChannelInner<T>>>,
}

pub struct Channel<T> {
    inner: Arc<Mutex<ChannelInner<T>>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Channel {
            inner: self.inner.clone(),
        }
    }
}

pub fn new<T: Send + 'static>() -> (Sender<T>, Receiver<T>) {
    _new(0)
}

pub fn new_with_capacity<T: Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    _new(capacity)
}

pub fn unbounded<T: Send + 'static>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let inner = Arc::new(Mutex::new(UnboundedInner {
        buffer: VecDeque::new(),
        closed: false,
        recv_waiters: VecDeque::new(),
    }));
    (
        UnboundedSender { inner: inner.clone() },
        UnboundedReceiver { inner },
    )
}

fn _new<T: Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(ChannelInner {
        buffer: VecDeque::with_capacity(capacity),
        capacity,
        closed: false,
        send_waiters: VecDeque::new(),
        recv_waiters: VecDeque::new(),
        pending_value: None,
    }));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T: Send + 'static> Sender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        loop {
            let mut inner = self.inner.lock();

            if inner.closed {
                return Err(SendError::Disconnected(value));
            }

            if inner.capacity > 0 && inner.buffer.len() < inner.capacity {
                inner.buffer.push_back(value);
                if let Some(waiter) = inner.recv_waiters.pop_front() {
                    if waiter.status() == GStatus::Waiting {
                        waiter.set_status(GStatus::Runnable);
                        Scheduler::wake_g(waiter);
                    }
                }
                return Ok(());
            } else if inner.capacity == 0 {
                if let Some(waiter) = inner.recv_waiters.pop_front() {
                    inner.pending_value = Some(value);
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                    return Ok(());
                }

                if let Some(current_g) = Scheduler::current_g() {
                    // 无缓冲 channel：sender 等待时，把值存到 pending_value
                    inner.pending_value = Some(value);
                    current_g.set_status(GStatus::Waiting);
                    inner.send_waiters.push_back(current_g.clone());
                    drop(inner);
                    while current_g.status() == GStatus::Waiting {
                        Scheduler::yield_now();
                    }
                    // 被唤醒后，值已经被 receiver 取走了
                    return Ok(());
                } else {
                    return Ok(());
                }
            } else {
                return Err(SendError::Full(value));
            }
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), SendError<T>> {
        let mut inner = self.inner.lock();

        if inner.closed {
            return Err(SendError::Disconnected(value));
        }

        if inner.capacity > 0 && inner.buffer.len() < inner.capacity {
            inner.buffer.push_back(value);
            if let Some(waiter) = inner.recv_waiters.pop_front() {
                if waiter.status() == GStatus::Waiting {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
            }
            Ok(())
        } else {
            Err(SendError::Full(value))
        }
    }

    pub fn close(&self) {
        let mut inner = self.inner.lock();
        if !inner.closed {
            inner.closed = true;
            while let Some(waiter) = inner.recv_waiters.pop_front() {
                if waiter.status() == GStatus::Waiting {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }
}

impl<T: Send + 'static> Receiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            let mut inner = self.inner.lock();

            if let Some(value) = inner.buffer.pop_front() {
                if let Some(waiter) = inner.send_waiters.pop_front() {
                    if waiter.status() == GStatus::Waiting {
                        waiter.set_status(GStatus::Runnable);
                        Scheduler::wake_g(waiter);
                    }
                }
                return Ok(value);
            }

            if inner.closed {
                return Err(RecvError::Disconnected);
            }

            // 无缓冲 channel：如果有 sender 等待，直接从 sender 接收值
            if inner.capacity == 0 {
                if let Some(waiter) = inner.send_waiters.pop_front() {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                    drop(inner);
                    std::thread::yield_now();
                    let mut inner2 = self.inner.lock();
                    if let Some(value) = inner2.pending_value.take() {
                        return Ok(value);
                    }
                    continue;
                }
            }

            // 检查是否有 pending_value（sender 已经发送但 receiver 还没开始等待）
            if let Some(value) = inner.pending_value.take() {
                if let Some(waiter) = inner.send_waiters.pop_front() {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
                return Ok(value);
            }

            if let Some(current_g) = Scheduler::current_g() {
                current_g.set_status(GStatus::Waiting);
                inner.recv_waiters.push_back(current_g.clone());
                drop(inner);
                while current_g.status() == GStatus::Waiting {
                    Scheduler::yield_now();
                }
                // 醒来后检查 pending_value
                let mut inner = self.inner.lock();
                if let Some(value) = inner.pending_value.take() {
                    return Ok(value);
                }
            } else {
                drop(inner);
                std::thread::yield_now();
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut inner = self.inner.lock();

        if let Some(value) = inner.buffer.pop_front() {
            if let Some(waiter) = inner.send_waiters.pop_front() {
                if waiter.status() == GStatus::Waiting {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
            }
            return Ok(value);
        }

        if inner.closed {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }

    pub fn has_data(&self) -> bool {
        !self.inner.lock().buffer.is_empty()
    }

    pub fn iter(&self) -> ReceiverIter<'_, T> {
        ReceiverIter { receiver: self }
    }
}

impl<T: Send + 'static> Channel<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        let inner = Arc::new(Mutex::new(ChannelInner {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
            closed: false,
            send_waiters: VecDeque::new(),
            recv_waiters: VecDeque::new(),
            pending_value: None,
        }));
        Arc::new(Channel { inner })
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        loop {
            let mut inner = self.inner.lock();

            if inner.closed {
                return Err(SendError::Disconnected(value));
            }

            if inner.capacity > 0 && inner.buffer.len() < inner.capacity {
                inner.buffer.push_back(value);
                if let Some(waiter) = inner.recv_waiters.pop_front() {
                    if waiter.status() == GStatus::Waiting {
                        waiter.set_status(GStatus::Runnable);
                        Scheduler::wake_g(waiter);
                    }
                }
                return Ok(());
            } else if inner.capacity == 0 {
                if let Some(waiter) = inner.recv_waiters.pop_front() {
                    if waiter.status() == GStatus::Waiting {
                        waiter.set_status(GStatus::Runnable);
                        Scheduler::wake_g(waiter);
                        return Ok(());
                    }
                }

                if let Some(current_g) = Scheduler::current_g() {
                    current_g.set_status(GStatus::Waiting);
                    inner.send_waiters.push_back(current_g.clone());
                    drop(inner);
                    while current_g.status() == GStatus::Waiting {
                        Scheduler::yield_now();
                    }
                    continue;
                } else {
                    return Ok(());
                }
            } else {
                return Err(SendError::Full(value));
            }
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), SendError<T>> {
        let mut inner = self.inner.lock();

        if inner.closed {
            return Err(SendError::Disconnected(value));
        }

        if inner.capacity > 0 && inner.buffer.len() < inner.capacity {
            inner.buffer.push_back(value);
            if let Some(waiter) = inner.recv_waiters.pop_front() {
                if waiter.status() == GStatus::Waiting {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
            }
            Ok(())
        } else {
            Err(SendError::Full(value))
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            let mut inner = self.inner.lock();

            if let Some(value) = inner.buffer.pop_front() {
                if let Some(waiter) = inner.send_waiters.pop_front() {
                    if waiter.status() == GStatus::Waiting {
                        waiter.set_status(GStatus::Runnable);
                        Scheduler::wake_g(waiter);
                    }
                }
                return Ok(value);
            }

            if inner.closed {
                return Err(RecvError::Disconnected);
            }

            if let Some(current_g) = Scheduler::current_g() {
                current_g.set_status(GStatus::Waiting);
                inner.recv_waiters.push_back(current_g.clone());
                drop(inner);
                while current_g.status() == GStatus::Waiting {
                    Scheduler::yield_now();
                }
            } else {
                drop(inner);
                std::thread::yield_now();
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut inner = self.inner.lock();

        if let Some(value) = inner.buffer.pop_front() {
            if let Some(waiter) = inner.send_waiters.pop_front() {
                if waiter.status() == GStatus::Waiting {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
            }
            return Ok(value);
        }

        if inner.closed {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub fn close(&self) {
        let mut inner = self.inner.lock();
        if !inner.closed {
            inner.closed = true;
            while let Some(waiter) = inner.recv_waiters.pop_front() {
                if waiter.status() == GStatus::Waiting {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }

    pub fn has_data(&self) -> bool {
        !self.inner.lock().buffer.is_empty()
    }

    pub fn iter(&self) -> ChannelIter<'_, T> {
        ChannelIter { channel: self }
    }
}

pub struct ChannelIter<'a, T> {
    channel: &'a Channel<T>,
}

impl<'a, T: Send + 'static> Iterator for ChannelIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.channel.recv().ok()
    }
}

pub struct ReceiverIter<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T: Send + 'static> Iterator for ReceiverIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl<T> Drop for Channel<T> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock();
        if !inner.closed {
            inner.closed = true;
        }
    }
}

pub trait Selectable {
    fn can_recv(&self) -> bool;
    fn can_send(&self) -> bool;
}

impl<T: Send + 'static> Selectable for Receiver<T> {
    fn can_recv(&self) -> bool {
        self.has_data() && !self.is_closed()
    }

    fn can_send(&self) -> bool {
        !self.is_closed()
    }
}

impl<T: Send + 'static> Selectable for Channel<T> {
    fn can_recv(&self) -> bool {
        self.has_data() && !self.is_closed()
    }

    fn can_send(&self) -> bool {
        !self.is_closed()
    }
}

impl<T: Send + 'static> Selectable for Sender<T> {
    fn can_recv(&self) -> bool {
        false
    }

    fn can_send(&self) -> bool {
        !self.is_closed()
    }
}

struct UnboundedInner<T> {
    buffer: VecDeque<T>,
    closed: bool,
    recv_waiters: VecDeque<Arc<G>>,
}

pub struct UnboundedSender<T> {
    inner: Arc<Mutex<UnboundedInner<T>>>,
}

pub struct UnboundedReceiver<T> {
    inner: Arc<Mutex<UnboundedInner<T>>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        UnboundedSender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for UnboundedReceiver<T> {
    fn clone(&self) -> Self {
        UnboundedReceiver {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Send + 'static> UnboundedSender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut inner = self.inner.lock();
        if inner.closed {
            return Err(SendError::Disconnected(value));
        }
        inner.buffer.push_back(value);
        if let Some(waiter) = inner.recv_waiters.pop_front() {
            if waiter.status() == GStatus::Waiting {
                waiter.set_status(GStatus::Runnable);
                Scheduler::wake_g(waiter);
            }
        }
        Ok(())
    }

    pub fn close(&self) {
        let mut inner = self.inner.lock();
        if !inner.closed {
            inner.closed = true;
            while let Some(waiter) = inner.recv_waiters.pop_front() {
                if waiter.status() == GStatus::Waiting {
                    waiter.set_status(GStatus::Runnable);
                    Scheduler::wake_g(waiter);
                }
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }
}

impl<T: Send + 'static> UnboundedReceiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            let mut inner = self.inner.lock();

            if let Some(value) = inner.buffer.pop_front() {
                return Ok(value);
            }

            if inner.closed {
                return Err(RecvError::Disconnected);
            }

            if let Some(current_g) = Scheduler::current_g() {
                current_g.set_status(GStatus::Waiting);
                inner.recv_waiters.push_back(current_g.clone());
                drop(inner);
                while current_g.status() == GStatus::Waiting {
                    Scheduler::yield_now();
                }
            } else {
                drop(inner);
                std::thread::yield_now();
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut inner = self.inner.lock();

        if let Some(value) = inner.buffer.pop_front() {
            return Ok(value);
        }

        if inner.closed {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }

    pub fn has_data(&self) -> bool {
        !self.inner.lock().buffer.is_empty()
    }
}

pub struct BoundedQueue<T> {
    queue: Arc<Mutex<VecDeque<T>>>,
    capacity: usize,
}

impl<T> BoundedQueue<T> {
    pub fn new(capacity: usize) -> Self {
        BoundedQueue {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    pub fn push(&self, value: T) -> Result<(), T> {
        let mut q = self.queue.lock();
        if q.len() >= self.capacity {
            return Err(value);
        }
        q.push_back(value);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        self.queue.lock().pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Clone for BoundedQueue<T> {
    fn clone(&self) -> Self {
        BoundedQueue {
            queue: self.queue.clone(),
            capacity: self.capacity,
        }
    }
}
