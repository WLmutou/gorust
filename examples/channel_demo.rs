use gorust::go;
use gorust::yield_now;
use gorust::sync::WaitGroup;
use gorust::{runtime, make_chan};

#[runtime]
fn main() {
    println!("=== Channel Demo ===");

    let ch = make_chan!(i32, 10);
    let done = make_chan!(bool, 1);

    let ch_prod = ch.clone();
    go(move || {
        for i in 0..10 {
            println!("Sending: {}", i);
            ch_prod.send(i).unwrap();
            yield_now();
        }
        println!("Producer done");
        ch_prod.close();
    });

    let ch_cons = ch.clone();
    let done_clone = done.clone();
    go(move || {
        for value in ch_cons.iter() {
            println!("Received: {}", value);
            if value == 9 {
                done_clone.send(true).unwrap();
            }
        }
    });

    done.recv().unwrap();
    println!("Channel demo completed!");

    println!("\n=== Pipeline Example ===");
    let wg_pipeline = WaitGroup::new();
    let numbers = make_chan!(i32, 10);
    let squares = make_chan!(i32, 10);

    let wg_gen = wg_pipeline.clone();
    let nums_prod = numbers.clone();
    go(move || {
        for i in 1..=10 {
            println!("Generating: {}", i);
            nums_prod.send(i).unwrap();
            yield_now();
        }
        println!("Generator done");
        nums_prod.close();
        wg_gen.done();
    });

    let wg_square = wg_pipeline.clone();
    let nums_cons = numbers.clone();
    let squares_prod = squares.clone();
    go(move || {
        for num in nums_cons.iter() {
            let sq = num * num;
            println!("Computing: {}^2 = {}", num, sq);
            squares_prod.send(sq).unwrap();
        }
        println!("Squarer done");
        squares_prod.close();
        wg_square.done();
    });

    let wg_result = wg_pipeline.clone();
    go(move || {
        println!("Results:");
        for square in squares.iter() {
            println!("  Square: {}", square);
        }
        println!("Pipeline done!");
        wg_result.done();
    });

    wg_pipeline.wait();

    println!("\n=== Select Example ===");
    let wg_select = WaitGroup::new();
    let ch1 = make_chan!(String, 5);
    let ch2 = make_chan!(String, 5);

    let wg_ch1 = wg_select.clone();
    let ch1_send = ch1.clone();
    go(move || {
        for i in 0..3 {
            ch1_send.send(format!("Message {} from ch1", i)).unwrap();
            yield_now();
        }
        ch1_send.close();
        wg_ch1.done();
    });

    let wg_ch2 = wg_select.clone();
    let ch2_send = ch2.clone();
    go(move || {
        for i in 0..3 {
            ch2_send.send(format!("Message {} from ch2", i)).unwrap();
            yield_now();
        }
        ch2_send.close();
        wg_ch2.done();
    });

    let wg_select_print = wg_select.clone();
    let ch1_recv = ch1.clone();
    let ch2_recv = ch2.clone();
    go(move || {
        let mut received_count = 0;
        while received_count < 6 {
            if let Ok(msg) = ch1_recv.try_recv() {
                println!("From ch1: {}", msg);
                received_count += 1;
            } else if let Ok(msg) = ch2_recv.try_recv() {
                println!("From ch2: {}", msg);
                received_count += 1;
            } else {
                yield_now();
            }
        }
        wg_select_print.done();
    });

    wg_select.wait();
    println!("All examples completed!");
}