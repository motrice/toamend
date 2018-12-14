extern crate rdkafka;

use std::sync::mpsc::{channel, Sender, Receiver};

use std::collections::HashMap;
use std::collections::HashSet;
use std::thread::JoinHandle;
use std::rc::Rc;

use ws::{Sender as WsSender};
use ws::{Builder, Settings};
use std::thread;

use self::rdkafka::producer::{FutureProducer, FutureRecord};

use kafka::producer::{create_producer, produce_subscription_event};
use kafka::{LedgerEvents, LedgerEventsConsumer};
use kafka::KAFKA_EVT_SUBSCRIBERS_CONFIG;
use domain::{LedgerEvent, SubscriptionEvent};
use serde_json::Value;

pub mod action;
pub mod handler;
pub mod factory;

use self::action::WsClientAction;
use self::factory::WsFactory;

pub struct WsContext {
    pub client_events_in: Sender<WsClientAction>,
    pub clients : HashMap<String, WsSender>,
    pub subscribers: HashMap<String, HashSet<String>>
}


impl WsContext {

    pub fn new() -> WsContext {
    let (client_events_in, client_events_out) : (Sender<WsClientAction>, Receiver<WsClientAction>) = channel();

// Receive events from clients from a mpsc channel in a thread 
        let client_events_thread = thread::Builder::new()
            .name("logger".to_owned())
            .spawn(move || {
            info!("EVENTCHANNEL start listen");

            let producer = create_producer(KAFKA_EVT_SUBSCRIBERS_CONFIG.brokers);

            while let Ok(cfg_evt) = client_events_out.recv() {
                produce_subscription_event(&producer, cfg_evt.to_subscription_event());
            }

            info!("Logger sending final message.");
            })
            .unwrap();

        WsContext {
            client_events_in: client_events_in.clone(),
            clients: HashMap::new(),
            subscribers: HashMap::new()
        }
    }

    pub fn start_server(&self) {
        Builder::new()
            .with_settings(Settings {
                max_connections: 28232,
                ..Settings::default()
            })
            .build(WsFactory{sender: self.client_events_in.clone()})
            .unwrap()
            .listen("127.0.0.1:3012")
            .unwrap();

        //let _ = self.client_events_thread.join();

    }

    pub fn send_event(&self, event:&LedgerEvent<Value>) -> Result<(), ws::Error> {
        for (client_id, ws_sender) in &self.clients {
            match serde_json::to_string(event) {
                Result::Ok(val) => {
                    ws_sender.send(val);
                }
                Result::Err(err) => {
                    print!("Error in serialization {}", err);
                }
            };
        }
        Ok(())
    }
} 

impl LedgerEvents for WsContext {
    fn on_event(&self, event: &LedgerEvent<Value>) {
        println!("Received event {} with checksum {} to WsContext", event.event_id, event.sys.payload_checksum.unwrap());
        self.send_event(event).unwrap();
    }
    
    fn on_subscription_event(&self, event: &SubscriptionEvent) {
        println!("Received event");
        match event {
            SubscriptionEvent::Open{conn_id} => println!("S Opened conn_id {}", conn_id), 
            SubscriptionEvent::Close{conn_id} => println!("S Closed conn_id {}", conn_id), 
            SubscriptionEvent::Subscribe{conn_id, topic} => println!("S Opened conn_id {} topic {}", conn_id, topic), 
            SubscriptionEvent::Unsubscribe{conn_id, topic} => println!("S Opened conn_id {} topic {}", conn_id, topic), 
        }
    }
}