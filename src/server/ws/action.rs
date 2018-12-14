extern crate ws;

use ws::{Sender as WsSender};
use domain::SubscriptionEvent;


pub enum WsClientAction {
  Open{conn_id: String, sender: WsSender},
  Subscribe{conn_id: String, ws_topic:String},
  Unsubscribe{conn_id: String, ws_topic: String},
  // Command(LedgerCommand) typ
  Close{conn_id: String}
}

impl WsClientAction {
  pub fn to_subscription_event(&self) -> SubscriptionEvent {
    match self {
      WsClientAction::Open{conn_id, sender:_ } => 
        SubscriptionEvent::Open{conn_id: &conn_id},
      WsClientAction::Subscribe{conn_id, ws_topic} => 
        SubscriptionEvent::Subscribe{conn_id: &conn_id, topic: &ws_topic},
      WsClientAction::Unsubscribe{conn_id, ws_topic} => 
        SubscriptionEvent::Unsubscribe{conn_id: &conn_id, topic: &ws_topic},
      WsClientAction::Close{conn_id} => 
        SubscriptionEvent::Open{conn_id: &conn_id},
    }
  }
}