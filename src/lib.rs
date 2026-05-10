// src/lib.rs
// 纯库版本，不包含宏定义

pub mod channel;
pub mod go_runtime;
pub mod scheduler;
pub mod sync;
pub mod timer;
pub mod stack;
pub mod netpoller;
pub mod net;

// 导出公共接口
pub use channel::{Channel, Sender, Receiver, Selectable, unbounded, UnboundedSender, UnboundedReceiver, BoundedQueue, TryRecvError};
pub use go_runtime::Runtime;
pub use scheduler::{go, yield_now};
pub use timer::{sleep_ms, sleep};


// 导出的宏定义
pub use go_macros::make_chan;
pub use go_macros::runtime;
pub use go_macros::select;

