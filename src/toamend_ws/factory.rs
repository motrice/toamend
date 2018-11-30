extern crate uuid;
extern crate ws;

use ws::{Sender as WsSender, Factory};
use std::sync::mpsc::{Sender};

use toamend_ws::handler::WsHandler;
use toamend_ws::action::WsClientAction;
use self::uuid::Uuid;

pub struct WsFactory {
  pub sender: Sender<WsClientAction>
}

impl Factory for WsFactory {
    type Handler = WsHandler;
 
    fn connection_made(&mut self, ws: WsSender) -> WsHandler {
      info!("connection_made");
      WsHandler {
        out: ws,
        conn_id: Uuid::new_v4().to_hyphenated().to_string(),
        // default to server
        config_event: Some(self.sender.clone()),
      }
    }

    fn client_connected(&mut self, ws: WsSender) -> WsHandler {
      info!("client_connected");

      WsHandler {
        out: ws,
        conn_id: Uuid::new_v4().to_hyphenated().to_string(),
        config_event: None,
      }
    }

    fn server_connected(&mut self, ws: WsSender) -> WsHandler {
      info!("server_connected");

      WsHandler {
        out: ws,
        conn_id: Uuid::new_v4().to_hyphenated().to_string(),
        config_event: Some(self.sender.clone()),
      }
    }

    fn on_shutdown(&mut self) {
      info!("Shutdown");
    }

    fn connection_lost(&mut self, _: Self::Handler) {
      info!("Connection lost");
    }

}
