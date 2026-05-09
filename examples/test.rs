use gorust::{Channel, Runtime, go};

fn main() {
    // 初始化运行时
    Runtime::init();

    // 测试 goroutine
    go(|| {
        println!("Hello from goroutine 1!");
    });

    go(|| {
        println!("Hello from goroutine 2!");
    });

    // 测试无缓冲通道
    let ch = Channel::<i32>::new(0);
    let ch_snd = ch.clone();
    go(move || {
        println!("Sending value 42...");
        ch_snd.send(42).unwrap();
        println!("Value sent!");
    });

    let ch_rcv = ch.clone();
    go(move || {
        let value = ch_rcv.recv().unwrap();
        println!("Received value: {}", value);
    });

    // 测试有缓冲通道
    let ch2 = Channel::<String>::new(10);
    ch2.send("Hello".to_string()).unwrap();
    let received = ch2.recv().unwrap();
    println!("Buffered channel test: {}", received);

    println!("All tests scheduled!");

    // 等待所有 goroutine 完成
    Runtime::wait_for_all();

    // 清理运行时
    Runtime::shutdown();
}
