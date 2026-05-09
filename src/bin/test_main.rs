use go_macros::make_chan;
use go_macros::runtime;
use gorust::Channel;
use gorust::go;
use log::debug;

fn mytestfnc() {
    debug!("Hello from mytestfnc!");
}

fn mytestparam(i: i32) {
    debug!("Starting goroutine {}", i);
}

#[runtime]
fn main() {
    env_logger::init(); // 初始化日志系统                                                                                                                                  

    go(|| {
        mytestfnc();
    });

    go(|| {
        mytestparam(2);
    });

    let ch = make_chan!(i32);
    let ch_snd = ch.clone();
    go(move || {
        debug!("Sending value 42...");
        ch_snd.send(42).unwrap();
        debug!("Value sent!");
    });

    let ch_rcv = ch.clone();
    go(move || {
        let value = ch_rcv.recv().unwrap();
        debug!("Received value: {}", value);
    });

    let ch2 = Channel::<String>::new(10);
    ch2.send("Hello".to_string()).unwrap();
    let received = ch2.recv().unwrap();
    debug!("Buffered channel test: {}", received);

    debug!("All tests scheduled!");
}
