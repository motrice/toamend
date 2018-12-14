extern crate redis;
extern crate r2d2;
extern crate r2d2_redis;
extern crate uuid;

extern crate serde_json;

use self::r2d2::{Pool};
use self::r2d2_redis::RedisConnectionManager;
use self::redis::{Commands};
use domain::{LedgerEvent};
use kafka::LedgerEvents;
use serde_json::Value;
use std::io::{Error, ErrorKind};


pub struct RedisContext {
    pub pool: Pool<RedisConnectionManager>
}

#[derive(Debug)]
pub enum RedisContextError {
    SerializationError(serde_json::Error),
    RedisError(redis::RedisError)
}

type RedisIntResult = Result<i64, RedisContextError>;
type RedisStringResult = Result<String, RedisContextError>;
type EventResult<'a> = Result<Box<LedgerEvent<'a, Value>>, Error>;

pub fn new_connection_pool() -> Option<RedisContext> {
    // TODO: config variablers pool size and redis host
    let manager = RedisConnectionManager::new("redis://localhost").unwrap();;
    let pool = r2d2::Pool::builder().max_size(15).build(manager);

    let context : Option<RedisContext> = match pool {
        Ok(p) => Some(RedisContext{pool:p}), 
        Err(_err) => None
    };
    
    return context;
}   

/*
fn conv<'a>(s: String) -> EventResult<'a> {
        let val : &'a String = s.copy();
        match serde_json::from_str(&**val) {
            Ok(event) => Result::Ok(event),
            Err(ser_err) => Result::Err(RedisContextError::SerializationError(ser_err))
        } 
}
*/

impl LedgerEvents for RedisContext {
    fn on_event(&self, event: &LedgerEvent<Value>) {
        println!("Received event {} with checksum {}", event.event_id, event.sys.payload_checksum.unwrap());
        self.append_event(event).unwrap();
    }
}

impl RedisContext {
    
    pub fn append_event(&self, evt: &LedgerEvent<Value>) -> RedisIntResult {
        let pool = self.pool.clone();
        let con = pool.get().unwrap();
        
        match serde_json::to_string(evt) {
            Ok(json_str) => { 
                match con.rpush(evt.event_id, json_str) {
                        Ok(value) => Ok(value),
                        Err(redis_err) => Result::Err(RedisContextError::RedisError(redis_err))
                }
            },
            Err(err) => {
                print!("called `Result::unwrap()` on an `Err` value: {:?}", err);
                Err(RedisContextError::SerializationError(err))
            }
        }
    }

    pub fn get_latest_checksum(&self, event_id: &str) {

    }

    pub fn get_latest(&self, event_id: &str) ->  RedisStringResult {
            let pool = self.pool.clone();
            let con = pool.get().unwrap();

            // get last element in list
            match con.lindex(event_id, -1) {
                Ok(value) => {
                    Result::Ok(value)
//                    let str : String = value;
  //                  Box::new(Result::Ok(serde_json::from_str(&str).unwrap()))
                    //   match serde_json::from_str(&tmp.to_owned()[..]) {
                    //       Ok(parsed_value) => Result::Ok(Box::new(parsed_value)),
                    //       Err(_) => Result::Err(Error::new(ErrorKind::Other, "oh no parse!"))
                    //   }
                },
                Err(err) => Result::Err(RedisContextError::RedisError(err))
            }
    }

}
/*
pub fn getLatestRange() (ctx: &RedisContext, event_id: &str) -> Result<&LedgerEvent> {
        let pool = ctx.pool.clone();
        let con = pool.get().unwrap();

        // get last element in list
        match con.lindex(evt.event_id, -1) {
            Ok(value) => value,
            Err(_err) => _err
        }

}
*/