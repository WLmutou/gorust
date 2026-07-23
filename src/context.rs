// src/context.rs
// 使用标准库线程替代平台相关汇编上下文切换
// 跨平台兼容（x86_64, ARM, RISC-V 等所有 Rust 支持平台）

/// 上下文结构体（简化版，仅用于 API 兼容）
/// 实际上下文切换由 OS 线程管理
#[derive(Clone, Debug)]
pub struct Context {
    pub rsp: u64,
    pub rbp: u64,
    pub rbx: u64,
    pub r12: u64,
    pub r13: u64,
    pub r14: u64,
    pub r15: u64,
}

impl Context {
    /// 创建一个空的上下文
    #[inline]
    pub const fn new_empty() -> Self {
        Context {
            rsp: 0, rbp: 0, rbx: 0,
            r12: 0, r13: 0, r14: 0, r15: 0,
        }
    }

    /// 创建一个初始上下文（使用线程模式时不需要实际实现）
    #[inline]
    pub fn make_initial(_stack_top: *mut u8, _entry: extern "C" fn()) -> Self {
        Context::new_empty()
    }
}

/// 上下文切换函数（使用线程模式时为空操作）
/// 实际上下文切换由 OS 负责
#[inline]
pub fn switch_context(_current: &mut Context, _target: &Context) {
    // 在线程模式下，不需要自定义上下文切换
    // 每个 goroutine 运行在独立的 OS 线程中
}