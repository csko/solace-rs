/**
Example showing how to create a solace context, session and subscribing to a queue using
the session.
*/
use std::{thread::sleep, time::Duration};

use solace_rs::{
    message::InboundMessage,
    session::{event::FlowEvent, SessionEvent},
    Context, SolaceLogLevel,
};

fn main() {
    let solace_context = Context::new(SolaceLogLevel::Warning).unwrap();
    println!("Context created");

    let on_message = move |message: InboundMessage| {
        println!("on_message handler got: {:#?} ", message);
    };

    let mut session = solace_context
        .session(
            "tcp://localhost:55554", // host
            "default",               // vpn
            "default",               // username
            "",                      // password
            Some(on_message),
            Some(|e: SessionEvent| {
                println!("on_event handler got: {}", e);
            }),
            Some(|e: FlowEvent| {
                println!("on_flow_event handler got: {}", e);
            }),
        )
        .expect("Could not create session");

    let queue_name = "default";

    session
        .subscribe_queue(queue_name)
        .expect("Could not subscribe to queue");
    println!("Subscribed to topic");

    let sleep_duration = Duration::new(10, 0);
    println!("Sleeping for {:?} before exiting", sleep_duration);
    sleep(sleep_duration);

    session
        .unsubscribe(queue_name)
        .expect("Could not unsubscribe to queue");
    println!("Unsubscribed from topic");
}
