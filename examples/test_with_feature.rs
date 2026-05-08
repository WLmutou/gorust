use gorust::go;
use gorust::{runtime, make_chan};

#[runtime]
fn main() {
    println!("Testing RGo functionality with macros...");

    go(|| {    
        println!("Hello from goroutine 1!");
    });

    go(|| {
        println!("Hello from goroutine 2!");
    });

    let ch = make_chan!(i32);
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

    let ch2 = make_chan!(String, 10);
    ch2.send("Hello".to_string()).unwrap();
    let received = ch2.recv().unwrap();
    println!("Buffered channel test: {}", received);

    println!("All tests scheduled!");
}