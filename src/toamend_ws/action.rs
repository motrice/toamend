extern crate ws;

use ws::{Sender as WsSender};

pub enum WsClientAction {
  Open{conn_id: String, sender: WsSender},
  Subscribe{conn_id: String, ws_topic:String},
  Unsubscribe{conn_id: String, ws_topic: String},
  // Command(LedgerCommand) typ
  Close{conn_id: String, ws_topics: String}
}
