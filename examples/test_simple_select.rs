use gorust::{make_chan, runtime, select};

#[runtime]
fn main() {
    // 创建一个通道用于测试
    let ch1 = make_chan!(i32, 1);
    let ch2 = make_chan!(i32, 1);

    // 发送一些数据到通道
    ch1.send(42).unwrap();
    ch2.send(99).unwrap();

    // 简单的select测试
    select! {
        val <- ch1 => {
            println!("Received from ch1: {}", val);
        },
        val <- ch2 => {
            println!("Received from ch2: {}", val);
        },
        default => {
            println!("No operation ready - default case");
        }
    }
}
