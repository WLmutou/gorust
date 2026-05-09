use gorust::{go, make_chan, select};

#[gorust::runtime]
fn main() {
    println!("Testing simple select functionality...");

    // 创建通道
    let ch1 = make_chan!(i32, 1);
    let ch2 = make_chan!(i32, 1);

    // 在单独的goroutine中发送数据，避免所有权冲突
    go({
        let ch1_clone = ch1.clone();
        move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            ch1_clone.send(42).unwrap();
        }
    });

    go({
        let ch2_clone = ch2.clone();
        move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            ch2_clone.send(99).unwrap();
        }
    });

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

    // 再次测试
    let ch3 = make_chan!(i32, 1);
    let ch4 = make_chan!(i32, 1);

    go({
        let ch3_clone = ch3.clone();
        move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            ch3_clone.send(123).unwrap();
        }
    });

    select! {
        val <- ch3 => {
            println!("Received from ch3: {}", val);
        },
        val <- ch4 => {
            println!("Received from ch4: {}", val);
        },
        default => {
            println!("No operation ready - default case (second test)");
        }
    }

    // 测试发送操作
    let ch5 = make_chan!(i32, 1);
    select! {
        ch5.send(456) => {
            println!("Successfully sent value to ch5");
        },
        default => {
            println!("Could not send to ch5");
        }
    }

    // 验证发送的值
    if let Ok(val) = ch5.try_recv() {
        println!("Verified value in ch5: {}", val);
    }
}
