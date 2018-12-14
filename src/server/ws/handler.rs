
extern crate ws;

use ws::{Sender as WsSender, Handler, Result, Message, Handshake, CloseCode, Error};
use std::sync::mpsc::{Sender};

use server::ws::action::WsClientAction;

pub struct WsHandler {
    pub out: WsSender,
    pub conn_id: String,
    pub config_event: Option<Sender<WsClientAction>>
}

impl Handler for WsHandler {

    fn on_open(&mut self, _: Handshake) -> Result<()> {
      info!("Ws connection is OPENed");
      
        match &self.config_event {
          Some(sender) => {
            match sender.send(WsClientAction::Open{conn_id: self.conn_id.clone(), sender: self.out.clone()}) {
              Ok(_) => info!("WsClientAction send open ok"),
              Err(err) => {
                error!("WsClientAction send open Error {}", err)
              }
            };
            Ok(())
          },
          None => {
            debug!("No sender for {}", self.conn_id);
            Ok(())
          }
        }
    }

    fn on_message(&mut self, msg: Message) -> Result<()> {
        match msg {
          Message::Text(text) => {
            trace!("The message is text {}", text);
            match text.as_str() {
              
              "sub" => {
                self.out.send("ping")?;

                match &self.config_event {
                  Some(sender) => {
                    match sender.send(WsClientAction::Subscribe{conn_id: self.conn_id.clone(), ws_topic: text.clone()}) {
                        Ok(_) => trace!("WsClientAction send sub ok"),
                        Err(err) => {
                          error!("WsClientAction send sub Error {}", err)
                        }
                    };
                    Ok(())
                  },
                  None => {
                    debug!("No sender for {}", self.conn_id);
                    Ok(())
                  }
                }
              },
              "unsub" => {
                match &self.config_event {
                  Some(sender) => {
                    match sender.send(WsClientAction::Unsubscribe{conn_id: self.conn_id.clone(), ws_topic: text.clone()}) {
                        Ok(_) => trace!("WsClientAction send unsub ok"),
                        Err(err) => {
                          error!("WsClientAction send unsub Error {}", err)
                        }
                    };
                    Ok(())
                  },
                  None => {
                    debug!("No sender for {}", self.conn_id);
                    Ok(())
                  }
                }
              },
              "pong" => {
                 match self.out.close(CloseCode::Normal)
                         {
                            Ok(_) => info!("Client requested close. Closed."),
                            Err(err) => {
                              error!("Client requested close Error {}", err)
                            }
                          };
                          Ok(())
              },
              "close" => {
                 match self.out.close(CloseCode::Normal)
                         {
                            Ok(_) => info!("Client requested close. Closed."),
                            Err(err) => {
                              error!("Client requested close Error {}", err)
                            }
                          };
                          Ok(())
              },
              _ => {
                info!("Unknown command {}", text);
                Ok(())
              }
            }
            
          }, 
          _ => core::result::Result::Ok(())
        }
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
      match &self.config_event {
        Some(sender) => {
          match sender.send(WsClientAction::Close{conn_id: self.conn_id.clone()}) {
              Ok(_) => trace!("WsClientAction send close ok"),
              Err(err) => {
                error!("WsClientAction send close Error {}", err)
              }
          };
        },
        None => {
          debug!("No sender for {}", self.conn_id);
        }
      };
      
      match code {
          CloseCode::Normal => info!("The client is done with the connection."),
          CloseCode::Away   => info!("The client is leaving the site."),
          CloseCode::Abnormal => info!(
              "Closing handshake failed! Unable to obtain closing status from client."),
          _ => info!("The client encountered an error: {}", reason),
      }

    }

    fn on_error(&mut self, err: Error) {
        error!("The server encountered an error: {:?}", err);
    }

}
