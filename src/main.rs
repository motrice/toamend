extern crate pretty_env_logger;
extern crate ws;

mod toamend_ws; 

use std::collections::HashMap;
use std::collections::HashSet;

use ws::{Builder, Settings};
use std::sync::mpsc::{channel};
use std::thread;

use toamend_ws::action::WsClientAction;
use toamend_ws::factory::WsFactory;

#[macro_use] extern crate log;

fn main() {

  pretty_env_logger::init();
  let (config_event_in, config_event_out) = channel();

  let mut clients = HashMap::new(); 
  let mut subscribers : HashMap<String, HashSet<String>> = HashMap::new();

    // Receive events from clients from a mpsc channel in a thread 
  let logger = thread::Builder::new()
    .name("logger".to_owned())
    .spawn(move || {
      info!("EVENTCHANNEL start listen");
      
      while let Ok(cfg_evt) = config_event_out.recv() {
        info!("EVENTCHANNEL new incoming event");
        match cfg_evt {
          WsClientAction::Open{conn_id, sender} => {
            info!("EVTCHANNEL OPEN conn_id: {}", conn_id);
            clients.insert(conn_id.clone(), sender);
          },
          WsClientAction::Close{conn_id, ws_topics} => {
            info!("EVTCHANNEL CLOSE conn_id: {} topics: {}", conn_id, ws_topics);
            clients.remove(&conn_id);
          },
          WsClientAction::Subscribe{conn_id, ws_topic} => {
            info!("EVTCHANNEL Subscribe conn_id: {} topics: {}", conn_id, ws_topic);
            subscribers
            .entry(ws_topic.clone())
            .and_modify(|e| { 
              if e.insert(conn_id.clone()) {
                info!("unsbscribed topic {} for conn_id {}", ws_topic, conn_id);
              } 
            })
            .or_insert([conn_id.clone()].iter().cloned().collect());

            for (key, val) in &subscribers {
              info!("SUBSCRIBERS {}", key);
              for client in val {
                info!(" - {}", client);
              }
            }
          },
          WsClientAction::Unsubscribe{conn_id, ws_topic} => {
            info!("EVTCHANNEL Unsubscribe conn_id: {} topics: {}", conn_id, ws_topic);
            
            subscribers
            .entry(ws_topic.clone())
            .and_modify(|e| { 
              if e.remove(&conn_id) {
                info!("unsbscribed topic {} for conn_id {}", ws_topic, conn_id);
              } 
            });
            
          } 
        }
      }

      info!("Logger sending final message.");
    })
    .unwrap();


  Builder::new()
    .with_settings(Settings {
        max_connections: 28232,
        ..Settings::default()
    })
    .build(WsFactory{sender: config_event_in.clone()})
    .unwrap()
    .listen("127.0.0.1:3012")
    .unwrap();

  let _ = logger.join();
} 