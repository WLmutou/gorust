// src/context.rs
// x86_64 汇编上下文切换实现
// 使用内联汇编保存/恢复被调用者保存寄存器
// 支持 M:N 用户态协程模型

/// 上下文结构体，保存 x86_64 的被调用者保存寄存器
/// 必须保持 #[repr(C)] 布局，与汇编代码中的偏移量一致
#[derive(Clone, Debug)]
#[repr(C)]
pub struct Context {
    pub rsp: u64,  // 栈指针 (offset 0)
    pub rbp: u64,  // 基址指针 (offset 8)
    pub rbx: u64,  // 被调用者保存 (offset 16)
    pub r12: u64,  // 被调用者保存 (offset 24)
    pub r13: u64,  // 被调用者保存 (offset 32)
    pub r14: u64,  // 被调用者保存 (offset 40)
    pub r15: u64,  // 被调用者保存 (offset 48)
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

    /// 创建一个初始上下文，用于新协程的首次执行
    ///
    /// 栈布局（从高地址到低地址）：
    ///   [stack_top]           <- 栈顶（最高地址）
    ///   [stack_top - 8]       <- 8 字节对齐填充
    ///   [stack_top - 16]      <- entry 函数指针（ret 时会弹出并跳转）
    ///   RSP = stack_top - 16
    ///
    /// 这样设置后，当 switch_context 执行 ret 时：
    ///   - ret 弹出 entry 并跳转
    ///   - RSP 变为 stack_top - 8，满足 x86_64 ABI 的 RSP % 16 == 8 要求
    #[inline]
    pub fn make_initial(stack_top: *mut u8, entry: extern "C" fn()) -> Self {
        let stack_top_addr = stack_top as u64;
        // 确保 16 字节对齐
        let aligned_top = stack_top_addr & !15;
        let rsp = aligned_top.wrapping_sub(16);

        unsafe {
            // 在栈顶放置入口函数指针
            std::ptr::write((aligned_top as usize - 16) as *mut u64, entry as u64);
        }

        Context {
            rsp,
            rbp: 0,
            rbx: 0,
            r12: 0,
            r13: 0,
            r14: 0,
            r15: 0,
        }
    }
}

/// 上下文切换函数
///
/// 保存当前执行上下文的寄存器到 `current`，恢复 `target` 的寄存器。
/// 切换后执行 `target` 协程的代码（通过 ret 跳转到 target 的返回地址）。
///
/// 使用裸函数 (naked) 避免编译器生成 prologue/epilogue，
/// 完全由汇编代码控制栈帧。
///
/// # Safety
/// 调用者必须确保 `current` 和 `target` 指向有效的 Context 结构体。
#[unsafe(naked)]
pub unsafe extern "C" fn switch_context(current: &mut Context, target: &Context) {
    // System V ABI (x86-64):
    //   RDI = current  (第一个参数)
    //   RSI = target   (第二个参数)
    core::arch::naked_asm!(
        // 保存当前上下文到 current 结构体
        "mov [rdi], rsp",
        "mov [rdi+8], rbp",
        "mov [rdi+16], rbx",
        "mov [rdi+24], r12",
        "mov [rdi+32], r13",
        "mov [rdi+40], r14",
        "mov [rdi+48], r15",
        // 恢复 target 上下文
        "mov rsp, [rsi]",
        "mov rbp, [rsi+8]",
        "mov rbx, [rsi+16]",
        "mov r12, [rsi+24]",
        "mov r13, [rsi+32]",
        "mov r14, [rsi+40]",
        "mov r15, [rsi+48]",
        // 跳转到 target 的返回地址
        "ret",
    );
}