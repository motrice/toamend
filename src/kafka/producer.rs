extern crate rdkafka;
extern crate uuid;
extern crate chrono;
extern crate serde;
extern crate serde_json;

use self::rdkafka::producer::{FutureProducer, FutureRecord};
use self::rdkafka::config::ClientConfig;

use domain::{LedgerCommand, SubscriptionEvent};
use serde_json::Value;


pub fn create_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("queue.buffering.max.ms", "0")  // Do not buffer
        .create()
        .expect("Producer creation failed")
}


//pub fn create_producer(brokers: &str) ->

pub fn produce_command(command: LedgerCommand<Value>, user_id: &str) {//tracking_id: &str, payload: &str, key: &str, action: Action) {
    let producer = create_producer("localhost:9092");

    
    let cmd = LedgerCommand {
        tracking_id: "asdfgh",
        user_id: Some(user_id),
        ..command
    };

    // Serialize it to a JSON string.
    match serde_json::to_string(&cmd) {
        Result::Ok(val) => {
/*            producer.send_copy::<String, String>(
                    "test", 
                    None, 
                    Some(&val), 
                    Some(&cmd.tracking_id), 
                    None, 
                    1000); */
            producer.send(
                FutureRecord::to("test-cmd-3part")
                    .payload(&val) 
                    .key(cmd.tracking_id),
                5000
            );
        }
        Result::Err(err) => {
              print!("called `Result::unwrap()` on an `Err` value: {:?}", err);
        }
    };
        
    
}
/*

match serde_json::to_string(&evt) {
        Result::Ok(val) => {
            producer.send(
                FutureRecord::to(events_topic)
                    .payload(&val) 
                    .key(evt.event_id),
                5000
            );
            
        }
        Result::Err(err) => {
            print!("called `Result::unwrap()` on an `Err` value: {:?}", err);
        }
    };

sClientAction::Open{conn_id, sender} => {
                        info!("EVTCHANNEL OPEN conn_id: {}", conn_id);
                        
                        producer.send(
                            FutureRecord::to("test-evt-subscriber")
                                .payload(&["connected", &conn_id].join("|")) 
                                .key(&conn_id),
                            5000
                        );
                    },
*/

pub fn produce_subscription_event(producer: &FutureProducer, subscription_event: SubscriptionEvent) {//tracking_id: &str, payload: &str, key: &str, action: Action) {

    // Serialize it to a JSON string.
    match serde_json::to_string(&subscription_event) {
        Result::Ok(val) => {
            producer.send(
                FutureRecord::to("test-evt-subscriber")
                    .payload(&val) 
                    .key("keytodo"),
                5000
            );
        }
        Result::Err(err) => {
              print!("called `Result::unwrap()` on an `Err` value: {:?}", err);
        }
    };
        
    
}
