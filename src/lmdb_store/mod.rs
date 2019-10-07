extern crate lmdb_rs;
extern crate serde_json;
extern crate ws;

use self::lmdb_rs::{Database, DbHandle, Environment, EnvBuilder, DbFlags};
use self::lmdb_rs::core::MdbError;
use std::result::Result;
use domain::{LedgerEvent};
use kafka::LedgerEvents;
use serde_json::Value;

pub struct LmdbContext {
    pub env: Environment,
    pub db_handle: DbHandle
}

pub fn create_context() -> Option<LmdbContext> {
    /*
{
 TODO: KOLLA storlek  path: LMDB_DATA_PATH,
    mapSize: 2 * 1024 * 1024 * 1024, // maximum database size
    maxDbs: 3
}
    */
        match EnvBuilder::new().open("test-lmdb", 0o777) {
            Ok(environment) => {
                match environment.get_default_db(DbFlags::empty()) {
                    Ok(db) => {
                        Some(LmdbContext{env: environment, db_handle: db})
                    },
                    Err(_) => None
                }
            },
            Err(_) => None
        }
}

impl LmdbContext {

    pub fn get(&self, key: &str) -> Result<String, MdbError> {
        match self.env.get_reader() {
            Ok(reader) => {
                match reader.bind(&self.db_handle).get::<&str>(&key) {
                    Ok(value) => Result::Ok(String::from(value)),
                    Err(err) => Err(err)
                }
            },
            Err(err) => Err(err)
        }
    }

    pub fn get_latest(&self, key: &str) -> Result<Option<LedgerEvent<Value>>, MdbError> {
        println!("get_latest {}", key);
        match self.env.get_reader() {
            Ok(reader) => {
                let db : Database = reader.bind(&self.db_handle);
                match db.get::<&str>(&key) {
                    Ok(version) => match db.get::<&str>(&version) {
                        Ok(value) => match serde_json::from_str(value) {
                                    Ok(evt) => Ok(Some(evt)),
                                    Err(err) => {
                                        // TODO fix error handling , remove option
                                        print!("Error while parsing value={} err={}", value, err);
                                        Ok(None)
                                    }
                                },
                        Err(err) => {
                            print!("Error while getting version={} err={}", version, err);
                            Err(err)
                        }
                    },
                    Err(err) => {
                            print!("Error while getting key={} err={}", key, err);
                            Err(err)
                    }
                }
            },
            Err(err) => {
                print!("Error while getting reader {}", err);
                Err(err)
            }
            }
    }

    pub fn get_previous(&self, version: &str, limit: u8) -> Result<Vec<LedgerEvent<Value>>, MdbError> {
        info!("get_previous version={} limit={}", version, limit);
        let mut result = Vec::new();
        match self.env.get_reader() {
            Ok(reader) => {
                let db : Database = reader.bind(&self.db_handle);
                match db.get::<&str>(&version) {
                    Ok(data_version) => match serde_json::from_str::<LedgerEvent<Value>>(data_version) {
                        Ok(req_version) => {
                            let mut previous_version : Option<&str> = req_version.sys.previous_version;
                            result.push(req_version);
                            let mut count : u8 = 0;
                            while count <= limit && previous_version != None {
                                match db.get::<&str>(&previous_version.unwrap()) {
                                    Ok(prev_version_str) => match serde_json::from_str::<LedgerEvent<Value>>(prev_version_str) {
                                        Ok(prev_version) => {
                                            previous_version = prev_version.sys.previous_version;
                                            result.push(prev_version);
                                        },
                                        _ => count = limit, // break loop
                                    }
                                    _ => count = limit // break loop
                                }
                                count += 1;
                            }
                            Ok(result) 
                        },
                        Err(err) => {
                            error!("Error while parsing data_version={} err={}", data_version, err);
                            Ok(result)
                        }
                    },
                    Err(err) => {
                        // TODO fix error handling , remove option
                        error!("Error while parsing version={} err={}", version, err);
                        Ok(result)
                    }
                }
            },
            Err(err) => {
                error!("Error while getting reader {}", err);
                Err(err)
            }
        }
    }

    pub fn get_latest_by_version(&self, version: &str) -> Result<Option<LedgerEvent<Value>>, MdbError> {
        info!("get_latest_by_version {}", version);
        match self.env.get_reader() {
            Ok(reader) => {
                let db : Database = reader.bind(&self.db_handle);
                match db.get::<&str>(&version) {
                    Ok(data_version) => match serde_json::from_str::<LedgerEvent<Value>>(data_version) {
                        Ok(req_version) => match db.get::<&str>(&req_version.sys.id) {
                            Ok(latest_version) => match db.get::<&str>(&latest_version) {
                                Ok(value) => match serde_json::from_str(value) {
                                    Ok(evt) => Ok(Some(evt)),
                                    Err(err) => {
                                        // TODO fix error handling , remove option
                                        error!("Error while parsing latest_version value={} err={}", value, err);
                                        Ok(None)
                                    }
                                },
                                Err(err) => {
                                    error!("Error while getting latest_version={} err={}", latest_version, err);
                                    Err(err)
                                }
                            },
                            Err(err) => {
                                error!("Error while getting req_version={} err={}", req_version, err);
                                Err(err)
                            }
                        },
                        Err(err) => {
                            error!("Error while parsing data_version={} err={}", data_version, err);
                            Ok(None)
                        }
                    },
                    Err(err) => {
                        // TODO fix error handling , remove option
                        error!("Error while parsing version={} err={}", version, err);
                        Ok(None)
                    }
                }
            },
            Err(err) => {
                error!("Error while getting reader {}", err);
                Err(err)
            }
        }
    }

    

    pub fn set(&self, key: &str, value: &str) -> Result<(), MdbError> {
        let txn = match self.env.new_transaction() {
            Ok(txn) => match txn.bind(&self.db_handle).set(&key, &value) {
                Ok(_) => Ok(txn),
                Err(err) => Err(err)
            }
            Err(err) => Err(err)
                
        };
        match txn {
            Err(err) => Err(err),
            Ok(the_txn) => the_txn.commit()
        }
    }

    pub fn set_event(&self, event: &LedgerEvent<Value>) -> Result<(), MdbError> {
        match serde_json::to_string(&event) {
            Result::Ok(value) => {
                let txn = self.env.new_transaction()?;
                {
                    let db = txn.bind(&self.db_handle);
                    db.set(&event.sys.version, &value)?;
                    match db.get::<&str>(&event.sys.id) {
                        Ok(_) => {
                            // there is a previous version linked to this id
                            println!("SET: Upd id: {} version: {}", event.sys.id, event.sys.version);
                            db.del(&event.sys.id)?;
                            db.set(&event.sys.id, &event.sys.version)?;
                            Ok(())
                        },
                        Err(err) => {
                            match err {
                                MdbError::NotFound => {
                                    println!("SET: New id: {} version: {}", event.sys.id, event.sys.version);

                                    db.set(&event.sys.id, &event.sys.version)?;
                                    Ok(())
                                },
                                _ => Err(err)
                            }
                        }
                    }?;
                }
                println!("Finsihed ok");
                txn.commit()?;
                Ok(())
                        
            }
            Result::Err(err) => {
                Ok(()) // TODO serde error 
            }
        }

    }

/*
            txn.putString(this.dbiSource, docKeyRev, JSON.stringify(value));

            // set latest
            const oldDocKeyRev = txn.getString(this.dbiSource, this.dbKey(docKey));
            // if dependency checker node
            this.storeDependencies(txn, oldDocKeyRev, keyObj, value);

            if (oldDocKeyRev) txn.del(this.dbiSource, docKey);
            txn.putString(this.dbiSource, docKey, docKeyRev);

            // set log
            let docLog = JSON.parse(txn.getString(this.dbiSource, docKeyLog));
            if (docLog) {
                docLog.push(docKeyRev);
            } else {
                docLog = [docKeyRev];
            }
            txn.putString(this.dbiSource, docKeyLog, JSON.stringify(docLog));

            if (lastOffset) {
                txn.del(this.dbiCommands, offsetKey);
            }
            txn.putString(this.dbiCommands, offsetKey, JSON.stringify(offset));

            if (value && value.dependencies) {

            }
        }
        txn.commit();
*/
}

fn keys(event: &LedgerEvent<Value>) -> (String, String) {
    let mut revision_key : String = event.event_id.to_owned();
    revision_key.push_str("|");
    revision_key.push_str(event.sys.version);

    (String::from(event.event_id), revision_key)
}


impl LedgerEvents for LmdbContext {
    fn on_event(&self, event: &LedgerEvent<Value>) {
        println!("Received event {} with checksum {} to LMDB store", event.event_id, event.sys.payload_checksum.unwrap());
        self.set_event(event).unwrap();
    }
}
