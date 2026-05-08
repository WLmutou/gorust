// src/channel.rs
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use parking_lot::Mutex;

pub struct Channel<T> {
    sender: ChannelSender<T>,
    receiver: Arc<Mutex<ChannelReceiver<T>>>,
    closed: AtomicBool,
}

enum ChannelSender<T> {
    Unbuffered(mpsc::Sender<T>),
    Buffered(mpsc::SyncSender<T>),
}

enum ChannelReceiver<T> {
    Unbuffered(mpsc::Receiver<T>),
    Buffered(mpsc::Receiver<T>),
}

impl<T: Send + 'static> Channel<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (sender, receiver) = if capacity > 0 {
            let (sync_sender, sync_receiver) = mpsc::sync_channel(capacity);
            (ChannelSender::Buffered(sync_sender), ChannelReceiver::Buffered(sync_receiver))
        } else {
            let (sender, receiver) = mpsc::channel();
            (ChannelSender::Unbuffered(sender), ChannelReceiver::Unbuffered(receiver))
        };

        Arc::new(Channel {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            closed: AtomicBool::new(false),
        })
    }

    pub fn send(&self, value: T) -> Result<(), T> {
        if self.closed.load(Ordering::Acquire) {
            return Err(value);
        }

        match &self.sender {
            ChannelSender::Unbuffered(sender) => sender.send(value).map_err(|e| e.0),
            ChannelSender::Buffered(sender) => sender.send(value).map_err(|e| e.0),
        }
    }

    // 添加 try_send 方法
    pub fn try_send(&self, value: T) -> Result<(), T> {
        if self.closed.load(Ordering::Acquire) {
            return Err(value);
        }

        match &self.sender {
            ChannelSender::Unbuffered(_sender) => {
                // 对于无缓冲通道，我们无法真正"尝试"发送，因为它总是阻塞
                // 所以我们在这里只返回错误，表示无法立即发送
                Err(value)
            },
            ChannelSender::Buffered(sender) => {
                match sender.try_send(value) {
                    Ok(()) => Ok(()),
                    Err(mpsc::TrySendError::Full(val)) => Err(val),
                    Err(mpsc::TrySendError::Disconnected(val)) => Err(val),
                }
            },
        }
    }

    pub fn recv(&self) -> Option<T> {
        loop {
            match self.try_recv() {
                Ok(value) => return Some(value),
                Err(mpsc::TryRecvError::Empty) => {
                    if self.closed.load(Ordering::Acquire) {
                        return None;
                    }
                    std::thread::yield_now();
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.close();
                    return None;
                }
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, mpsc::TryRecvError> {
        match &*self.receiver.lock() {
            ChannelReceiver::Unbuffered(receiver) => receiver.try_recv(),
            ChannelReceiver::Buffered(receiver) => receiver.try_recv(),
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn iter(&self) -> ChannelIter<'_,T> {
        ChannelIter {
            channel: self,
        }
    }
}

pub struct ChannelIter<'a, T> {
    channel: &'a Channel<T>,
}

impl<'a, T: Send + 'static> Iterator for ChannelIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.channel.recv()
    }
}

// 为任何类型实现Drop trait，因为close方法不依赖于T的任何特定trait
impl<T> Drop for Channel<T> {
    fn drop(&mut self) {
        // 直接操作原子布尔值，而不调用需要Send trait的方法
        self.closed.store(true, Ordering::Release);
    }
}

pub trait Selectable {
    fn can_recv(&self) -> bool;
    fn can_send(&self) -> bool;
}

impl<T: Send + 'static> Selectable for Channel<T> {
    fn can_recv(&self) -> bool {
        !self.is_closed() && self.try_recv().is_ok()
    }

    fn can_send(&self) -> bool {
        !self.is_closed()
    }
}