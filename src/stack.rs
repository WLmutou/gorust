// src/stack.rs
use std::alloc::{self, Layout};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

// 栈配置
const STACK_INITIAL_SIZE: usize = 2048; // 2KB 初始栈
const STACK_MAX_SIZE: usize = 1024 * 1024; // 1MB 最大栈
const STACK_ALIGN: usize = 4096; // 页对齐

/// Goroutine 栈
pub struct GoroutineStack {
    ptr: *mut u8,
    capacity: usize,
    used: AtomicUsize,
}

/// 栈分配器
pub struct StackAllocator {
    total_allocated: AtomicUsize,
    total_memory: AtomicUsize,
}

impl StackAllocator {
    pub const fn new() -> Self {
        StackAllocator {
            total_allocated: AtomicUsize::new(0),
            total_memory: AtomicUsize::new(0),
        }
    }

    /// 分配新栈
    pub fn alloc(&self) -> Result<GoroutineStack, String> {
        let layout = Layout::from_size_align(STACK_INITIAL_SIZE, STACK_ALIGN)
            .map_err(|e| format!("Layout error: {}", e))?;
        
        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            return Err("Failed to allocate stack".to_string());
        }

        // 初始化栈内存为 0
        unsafe {
            ptr::write_bytes(ptr, 0, STACK_INITIAL_SIZE);
        }

        self.total_allocated.fetch_add(1, Ordering::Relaxed);
        self.total_memory.fetch_add(STACK_INITIAL_SIZE, Ordering::Relaxed);
        
        Ok(GoroutineStack {
            ptr,
            capacity: STACK_INITIAL_SIZE,
            used: AtomicUsize::new(0),
        })
    }

    pub fn stats(&self) -> (usize, usize, usize) {
        (
            self.total_allocated.load(Ordering::Relaxed),
            self.total_memory.load(Ordering::Relaxed),
            self.total_memory.load(Ordering::Relaxed) / 1024 / 1024, // MB
        )
    }
}

impl GoroutineStack {
    /// 获取栈顶指针
    pub fn top(&self) -> *mut u8 {
        unsafe { self.ptr.add(self.capacity) }
    }

    /// 获取栈底指针
    pub fn bottom(&self) -> *mut u8 {
        self.ptr
    }

    /// 获取当前栈使用量
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// 更新栈使用量（在上下文切换时调用）
    pub fn update_used(&self, sp: *mut u8) {
        if sp >= self.bottom() && sp <= self.top() {
            let used = unsafe { self.top().offset_from(sp) } as usize;
            self.used.store(used, Ordering::Relaxed);
        }
    }

    /// 检查是否需要扩容
    pub fn needs_grow(&self) -> bool {
        let used = self.used();
        used + 256 >= self.capacity // 剩余不到 256 字节
    }

    /// 扩容栈
    pub fn grow(&mut self) -> Result<(), String> {
        let new_capacity = (self.capacity * 2).min(STACK_MAX_SIZE);
        if new_capacity == self.capacity {
            return Err("Stack size limit reached".to_string());
        }

        let new_layout = Layout::from_size_align(new_capacity, STACK_ALIGN)
            .map_err(|e| format!("Layout error: {}", e))?;
        
        let new_ptr = unsafe {
            alloc::realloc(self.ptr as *mut u8, new_layout, new_layout.size())
        };
        
        if new_ptr.is_null() {
            return Err("Failed to grow stack".to_string());
        }

        self.ptr = new_ptr;
        self.capacity = new_capacity;
        Ok(())
    }

    /// 收缩栈（当使用量远小于容量时）
    pub fn shrink(&mut self) -> Result<(), String> {
        let used = self.used();
        if used > self.capacity / 4 {
            return Ok(());
        }

        let new_capacity = (self.capacity / 2).max(STACK_INITIAL_SIZE);
        if new_capacity == self.capacity {
            return Ok(());
        }

        let new_layout = Layout::from_size_align(new_capacity, STACK_ALIGN)
            .map_err(|e| format!("Layout error: {}", e))?;
        
        let new_ptr = unsafe {
            alloc::realloc(self.ptr as *mut u8, new_layout, new_layout.size())
        };
        
        if new_ptr.is_null() {
            return Err("Failed to shrink stack".to_string());
        }

        self.ptr = new_ptr;
        self.capacity = new_capacity;
        Ok(())
    }
}

impl Drop for GoroutineStack {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, STACK_ALIGN).unwrap();
        unsafe {
            alloc::dealloc(self.ptr, layout);
        }
    }
}

// 栈溢出检测（使用 guard page 的 Rust 替代方案）
pub struct StackGuard {
    _limit: usize,
}

impl StackGuard {
    pub fn new(stack_bottom: *mut u8) -> Self {
        // 在栈底写入哨兵值
        unsafe {
            ptr::write_volatile(stack_bottom, 0xDEu8);
        }
        StackGuard { _limit: 0 }
    }

    pub fn check(&self, _current_sp: *mut u8, stack_bottom: *mut u8) -> bool {
        unsafe {
            let guard_value = ptr::read_volatile(stack_bottom);
            if guard_value != 0xDEu8 {
                eprintln!("Stack overflow detected!");
                return false;
            }
        }
        true
    }
}