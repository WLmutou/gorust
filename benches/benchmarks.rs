use criterion::{criterion_group, criterion_main, Criterion};
use gorust::{channel, go, Runtime, Select};

fn benchmark_channel_send_recv(c: &mut Criterion) {
    c.bench_function("channel_unbuffered_send_recv", |b| {
        b.iter(|| {
            Runtime::init();
            let (tx, rx) = channel::new::<i32>();
            let tx_clone = tx.clone();
            
            go(move || {
                tx_clone.send(42).unwrap();
            });
            
            let _ = rx.recv();
            Runtime::wait_and_shutdown();
        });
    });

    c.bench_function("channel_buffered_send_recv", |b| {
        b.iter(|| {
            Runtime::init();
            let (tx, rx) = channel::new_with_capacity::<i32>(100);
            
            for i in 0..100 {
                tx.send(i).unwrap();
            }
            
            for _ in 0..100 {
                let _ = rx.recv();
            }
            
            Runtime::wait_and_shutdown();
        });
    });
}

fn benchmark_goroutine_creation(c: &mut Criterion) {
    c.bench_function("create_1000_goroutines", |b| {
        b.iter(|| {
            Runtime::init();
            for _ in 0..1000 {
                go(|| {});
            }
            Runtime::wait_and_shutdown();
        });
    });
}

fn benchmark_select(c: &mut Criterion) {
    c.bench_function("select_with_data", |b| {
        b.iter(|| {
            Runtime::init();
            let (tx, rx) = channel::new_with_capacity::<i32>(1);
            tx.send(42).unwrap();
            
            let _result = Select::new()
                .recv(rx)
                .with_default()
                .execute();
            
            Runtime::wait_and_shutdown();
        });
    });
}

criterion_group!(
    benches,
    benchmark_channel_send_recv,
    benchmark_goroutine_creation,
    benchmark_select
);
criterion_main!(benches);
