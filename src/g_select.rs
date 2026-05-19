use crate::channel::{Receiver, Channel};
use crate::scheduler::{GStatus, Scheduler};
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

pub struct Select {
    cases: Vec<Case>,
    has_default: bool,
}

enum Case {
    Recv {
        channel: Arc<dyn SelectableChannel>,
        case_id: usize,
    },
    Timeout {
        duration: Duration,
        case_id: usize,
    },
}

#[allow(dead_code)]
trait SelectableChannel: Send + Sync {
    fn try_receive(&self) -> Option<Box<dyn Any>>;
    fn is_closed(&self) -> bool;
    fn has_data(&self) -> bool;
}

struct TypedChannel<T: Send + 'static> {
    receiver: Receiver<T>,
}

impl<T: Send + 'static> SelectableChannel for TypedChannel<T> {
    fn try_receive(&self) -> Option<Box<dyn Any>> {
        self.receiver.try_recv().ok().map(|v| Box::new(v) as Box<dyn Any>)
    }

    fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }

    fn has_data(&self) -> bool {
        self.receiver.has_data()
    }
}

struct TypedChannelWrapper<T: Send + 'static> {
    channel: Channel<T>,
}

impl<T: Send + 'static> SelectableChannel for TypedChannelWrapper<T> {
    fn try_receive(&self) -> Option<Box<dyn Any>> {
        self.channel.try_recv().ok().map(|v| Box::new(v) as Box<dyn Any>)
    }

    fn is_closed(&self) -> bool {
        self.channel.is_closed()
    }

    fn has_data(&self) -> bool {
        self.channel.has_data()
    }
}

impl Select {
    pub fn new() -> Self {
        Select {
            cases: Vec::new(),
            has_default: false,
        }
    }

    pub fn recv<T: Send + 'static>(mut self, receiver: Receiver<T>) -> Self {
        let case_id = self.cases.len();
        self.cases.push(Case::Recv {
            channel: Arc::new(TypedChannel { receiver }),
            case_id,
        });
        self
    }

    pub fn recv_channel<T: Send + 'static>(mut self, channel: Channel<T>) -> Self {
        let case_id = self.cases.len();
        self.cases.push(Case::Recv {
            channel: Arc::new(TypedChannelWrapper { channel }),
            case_id,
        });
        self
    }

    pub fn timeout(mut self, duration: Duration) -> Self {
        let case_id = self.cases.len();
        self.cases.push(Case::Timeout {
            duration,
            case_id,
        });
        self
    }

    pub fn with_default(mut self) -> Self {
        self.has_default = true;
        self
    }

    pub fn execute(self) -> SelectOutcome {
        if self.has_default {
            self.execute_non_blocking()
        } else {
            self.execute_blocking()
        }
    }

    fn execute_non_blocking(self) -> SelectOutcome {
        for case in &self.cases {
            match case {
                Case::Recv { channel, case_id } => {
                    if channel.has_data() {
                        if let Some(value) = channel.try_receive() {
                            return SelectOutcome::Received(*case_id, value);
                        }
                    }
                }
                Case::Timeout { .. } => {
                    continue;
                }
            }
        }

        SelectOutcome::Default
    }

    fn execute_blocking(self) -> SelectOutcome {
        let timeout_cases: Vec<(usize, Duration)> = self
            .cases
            .iter()
            .filter_map(|case| {
                if let Case::Timeout { duration, case_id } = case {
                    Some((*case_id, *duration))
                } else {
                    None
                }
            })
            .collect();

        let _min_timeout = timeout_cases.iter().map(|(_, d)| *d).min();

        loop {
            for case in &self.cases {
                match case {
                    Case::Recv { channel, case_id } => {
                        if channel.has_data() {
                            if let Some(value) = channel.try_receive() {
                                return SelectOutcome::Received(*case_id, value);
                            }
                        }
                    }
                    Case::Timeout { duration: _, case_id: _ } => {
                        continue;
                    }
                }
            }

            if let Some(current_g) = Scheduler::current_g() {
                current_g.set_status(GStatus::Waiting);
                Scheduler::yield_now();
                while current_g.status() == GStatus::Waiting {
                    Scheduler::yield_now();
                }
            } else {
                std::thread::yield_now();
            }
        }
    }
}

#[derive(Debug)]
pub enum SelectOutcome {
    Received(usize, Box<dyn Any>),
    Timeout(usize),
    Default,
}

impl SelectOutcome {
    pub fn unwrap_received(self) -> (usize, Box<dyn Any>) {
        match self {
            SelectOutcome::Received(case_id, value) => (case_id, value),
            _ => panic!("called `SelectOutcome::unwrap_received()` on a non-received value"),
        }
    }

    pub fn is_received(&self) -> bool {
        matches!(self, SelectOutcome::Received(_, _))
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, SelectOutcome::Timeout(_))
    }

    pub fn is_default(&self) -> bool {
        matches!(self, SelectOutcome::Default)
    }

    pub fn get_value<T: 'static>(self) -> Option<T> {
        match self {
            SelectOutcome::Received(_, value) => {
                if let Ok(v) = value.downcast::<T>() {
                    Some(*v)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

pub fn select_builder() -> Select {
    Select::new()
}
