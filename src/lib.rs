// src/lib.rs
// 纯库版本，不包含宏定义

pub mod go_runtime;
pub mod scheduler;
pub mod channel;
pub mod sync;


// 导出公共接口
pub use channel::Channel;
pub use scheduler::{go, yield_now};
pub use go_runtime::Runtime;


// 导出的宏定义
pub use go_macros::runtime;
pub use go_macros::make_chan;
pub use go_macros::select;