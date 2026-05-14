#[cfg(test)]
mod channel_tests {
    use gorust::channel::{self, Channel, RecvError, SendError};
    use gorust::go;
    use gorust::sync::WaitGroup;
    use gorust::Runtime;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_unbuffered_channel() {
        Runtime::init();
        
        let (tx, rx) = channel::new::<i32>();
        let wg = WaitGroup::new();
        
        wg.add(1);
        let tx_clone = tx.clone();
        let wg_clone = wg.clone();
        go(move || {
            tx_clone.send(42).unwrap();
            wg_clone.done();
        });
        
        let value = rx.recv().unwrap();
        assert_eq!(value, 42);
        
        wg.wait();
        Runtime::wait_and_shutdown();
    }

    #[test]
    fn test_buffered_channel() {
        Runtime::init();
        
        let (tx, rx) = channel::new_with_capacity::<String>(10);
        
        tx.send("hello".to_string()).unwrap();
        tx.send("world".to_string()).unwrap();
        
        assert_eq!(rx.recv().unwrap(), "hello");
        assert_eq!(rx.recv().unwrap(), "world");
        
        Runtime::wait_and_shutdown();
    }

    #[test]
    fn test_channel_close() {
        Runtime::init();
        
        let (tx, rx) = channel::new::<i32>();
        tx.close();
        
        assert!(tx.is_closed());
        assert!(rx.is_closed());
        
        let result = tx.send(1);
        assert!(matches!(result, Err(SendError::Disconnected(_))));
        
        Runtime::wait_and_shutdown();
    }

    #[test]
    fn test_recv_from_closed_channel() {
        Runtime::init();
        
        let (tx, rx) = channel::new_with_capacity::<i32>(1);
        tx.send(1).unwrap();
        tx.close();
        
        assert_eq!(rx.recv().unwrap(), 1);
        
        let result = rx.recv();
        assert!(matches!(result, Err(RecvError::Disconnected)));
        
        Runtime::wait_and_shutdown();
    }

    #[test]
    fn test_try_send_full() {
        Runtime::init();
        
        let (tx, _rx) = channel::new_with_capacity::<i32>(1);
        tx.send(1).unwrap();
        
        let result = tx.try_send(2);
        assert!(matches!(result, Err(SendError::Full(2))));
        
        Runtime::wait_and_shutdown();
    }

    #[test]
    fn test_multiple_senders_receivers() {
        Runtime::init();
        
        let (tx, rx) = channel::new_with_capacity::<i32>(100);
        let counter = Arc::new(AtomicUsize::new(0));
        let wg = WaitGroup::new();
        
        for i in 0..10 {
            wg.add(1);
            let tx_clone = tx.clone();
            let counter_clone = counter.clone();
            let wg_clone = wg.clone();
            go(move || {
                for j in 0..10 {
                    tx_clone.send(i * 10 + j).unwrap();
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                }
                wg_clone.done();
            });
        }
        
        wg.wait();
        
        let mut received = 0;
        while received < 100 {
            if let Ok(_) = rx.try_recv() {
                received += 1;
            }
        }
        
        assert_eq!(received, 100);
        assert_eq!(counter.load(Ordering::Relaxed), 100);
        
        Runtime::wait_and_shutdown();
    }
}

#[cfg(test)]
mod sync_tests {
    use gorust::sync::{WaitGroup, Mutex, RWMutex, Pool, Context, AtomicCounter, Once};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn test_waitgroup() {
        let wg = WaitGroup::new();
        let counter = Arc::new(AtomicUsize::new(0));
        
        for i in 0..5 {
            wg.add(1);
            let counter_clone = counter.clone();
            let wg_clone = wg.clone();
            std::thread::spawn(move || {
                counter_clone.fetch_add(i, Ordering::Relaxed);
                wg_clone.done();
            });
        }
        
        wg.wait();
        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_mutex() {
        let mutex = Mutex::new(0);
        
        {
            let mut guard = mutex.lock();
            *guard += 1;
        }
        
        assert_eq!(*mutex.lock(), 1);
    }

    #[test]
    fn test_rwlock() {
        let rwlock = RWMutex::new(vec![1, 2, 3]);
        
        {
            let read_guard = rwlock.read();
            assert_eq!(read_guard.len(), 3);
        }
        
        {
            let mut write_guard = rwlock.write();
            write_guard.push(4);
        }
        
        assert_eq!(*rwlock.read(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_pool() {
        let pool = Pool::new(|| String::from("default"), 10);
        
        let s1 = pool.get();
        assert_eq!(s1, "default");
        pool.put(s1);
        
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_context_background() {
        let ctx = Context::background();
        assert!(!ctx.done());
        assert!(ctx.err().is_none());
        assert!(ctx.deadline().is_none());
    }

    #[test]
    fn test_context_with_cancel() {
        let parent = Context::background();
        let (child, cancel) = Context::with_cancel(parent);
        
        assert!(!child.done());
        
        cancel();
        
        assert!(child.done());
        assert_eq!(child.err().unwrap(), "context canceled");
    }

    #[test]
    fn test_context_with_timeout() {
        let parent = Context::background();
        let (child, _cancel) = Context::with_timeout(parent, Duration::from_millis(50));
        
        assert!(!child.done());
        assert!(child.deadline().is_some());
        
        std::thread::sleep(Duration::from_millis(100));
        
        assert!(child.is_expired());
    }

    #[test]
    fn test_atomic_counter() {
        let counter = AtomicCounter::new();
        
        counter.inc();
        counter.inc();
        counter.dec();
        
        assert_eq!(counter.get(), 1);
    }

    #[test]
    fn test_once() {
        let once = Arc::new(Once::new());
        let counter = Arc::new(AtomicUsize::new(0));
        
        let mut handles = vec![];
        for _ in 0..10 {
            let counter_clone = counter.clone();
            let once_clone = once.clone();
            handles.push(std::thread::spawn(move || {
                once_clone.call_once(|| {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                });
            }));
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
mod timer_tests {
    use gorust::timer::{sleep, sleep_ms};
    use gorust::Runtime;
    use std::time::Instant;

    #[test]
    fn test_sleep_ms() {
        Runtime::init();
        
        let start = Instant::now();
        sleep_ms(50);
        let elapsed = start.elapsed();
        
        assert!(elapsed.as_millis() >= 50);
        
        Runtime::wait_and_shutdown();
    }

    #[test]
    fn test_sleep_duration() {
        Runtime::init();
        
        let start = Instant::now();
        sleep(std::time::Duration::from_millis(100));
        let elapsed = start.elapsed();
        
        assert!(elapsed.as_millis() >= 100);
        
        Runtime::wait_and_shutdown();
    }
}

#[cfg(test)]
mod select_tests {
    use gorust::channel;
    use gorust::g_select::{Select, SelectOutcome};
    use gorust::Runtime;

    #[test]
    fn test_select_non_blocking() {
        Runtime::init();
        
        let (tx1, rx1) = channel::new_with_capacity::<i32>(1);
        let (_tx2, rx2) = channel::new_with_capacity::<i32>(1);
        
        tx1.send(42).unwrap();
        
        let result = Select::new()
            .recv(rx1)
            .recv(rx2)
            .with_default()
            .execute();
        
        assert!(result.is_received());
        
        Runtime::wait_and_shutdown();
    }

    #[test]
    fn test_select_default() {
        Runtime::init();
        
        let (_tx1, rx1) = channel::new_with_capacity::<i32>(1);
        let (_tx2, rx2) = channel::new_with_capacity::<i32>(1);
        
        let result = Select::new()
            .recv(rx1)
            .recv(rx2)
            .with_default()
            .execute();
        
        assert!(result.is_default());
        
        Runtime::wait_and_shutdown();
    }
}
