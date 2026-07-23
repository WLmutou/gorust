// src/lib.rs

pub mod channel;
pub mod context;
pub mod go_runtime;
pub mod scheduler;
pub mod timer;
pub mod sync;
pub mod netpoller;
pub mod net;
pub mod g_select;

pub use channel::{
    Channel, Sender, Receiver, Selectable,
    unbounded, UnboundedSender, UnboundedReceiver,
    BoundedQueue,
    SendError, RecvError, TryRecvError,
};

pub use go_runtime::Runtime;
pub use scheduler::{go, go_task, yield_now};
pub use timer::{sleep_ms, sleep};

pub use g_select::{Select, SelectOutcome, select_builder};

pub use crate::sync::{
    WaitGroup, AtomicCounter, Once,
    Mutex, RWMutex, Pool, Context,
    current_task_id,
};

pub use go_macros::make_chan;
pub use go_macros::runtime;
pub use go_macros::select;
