extern crate rdkafka;
extern crate serde_json;
extern crate uuid;
extern crate chrono;
extern crate crypto;

pub mod consumer;
pub mod producer;

use std::io::{Error, ErrorKind};
use std::thread;
pub use self::producer::produce_command;

use self::producer::create_producer;
use self::consumer::create_consumer;
use futures::Stream;
use self::rdkafka::Message;
use self::rdkafka::consumer::{Consumer};

use self::rdkafka::producer::{FutureProducer, FutureRecord};
use self::chrono::Utc;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use self::uuid::Uuid;

use self::consumer::LoggingConsumer;

use domain::{LedgerCommand, LedgerEvent, Action, Sys, SubscriptionEvent};
use serde_json::Value;

use lmdb_store::create_context;
use lmdb_rs::core::MdbError; // TODO remove this dependency later



#[derive(Copy, Clone)]
pub struct WorkerConfig<'a> {
    pub name: &'a str,
    pub topics: &'a [&'a str], 
    pub publish_events_topic: &'a Option<& 'a str>
}

#[derive(Copy, Clone)]
pub struct KafkaConfig<'a> {
    pub brokers: &'a str,
    pub workers: &'a [&'a WorkerConfig<'a>]
} 

const KAFKA_BROKERS : &str = "localhost:9092";

const KAFKA_CMD_CONFIG : KafkaConfig = KafkaConfig {
    brokers: KAFKA_BROKERS,
    workers: &[&WorkerConfig{name: "cmd-worker", topics: &["test-cmd"], publish_events_topic: &Some("test-evt")}]
};

const KAFKA_EVT_CONFIG : KafkaConfig = KafkaConfig {
    brokers: KAFKA_BROKERS,
    workers: &[&WorkerConfig{name: "evt-worker", topics: &["test-evt"], publish_events_topic: &Option::None}]
};

pub const KAFKA_EVT_SUBSCRIBERS_CONFIG : KafkaConfig = KafkaConfig {
    brokers: KAFKA_BROKERS,
    workers: &[&WorkerConfig{name: "evt-subscriber", topics: &["test-evt-subscriber"], publish_events_topic: &Option::None}]
};

pub trait LedgerEvents {
    fn on_event(&self, event: &LedgerEvent<Value>) {}
    fn on_subscription_event(&self, event: &SubscriptionEvent) {}
}

pub struct LedgerEventsConsumer<'a> {
    hooks: Vec<Box<&'a LedgerEvents>>,
    consumer: LoggingConsumer
}

impl<'a> LedgerEventsConsumer<'a> {
    pub fn new(group_id: &str, include_subscriber_stream: bool) -> Self {
        if include_subscriber_stream {
            Self {
                hooks: Vec::new(),
                consumer: create_consumer(KAFKA_CMD_CONFIG.brokers, "event-consumer-group", &["test-evt", "test-evt-subscriber"])
            }
        }
        else {
            Self {
                hooks: Vec::new(),
                consumer: create_consumer(KAFKA_CMD_CONFIG.brokers, "event-consumer-group", &["test-evt"])
            }
        }
    }
    
    pub fn add_events_hook<E: LedgerEvents + 'a>(&mut self, hook: &'a E) {
        self.hooks.push(Box::new(hook));
    }

    pub fn process_events(&self) {
        for message in self.consumer.start().wait() {
            println!("process command: start");

            match message {
                Err(()) => {
                    warn!("Error while reading from stream");
                }
                Ok(Err(e)) => {
                    warn!("Kafka error: {}", e);
                }
                Ok(Ok(m)) => {
                    println!("process event: ok");
                    match m.payload_view::<str>() {
                        Some(some_payload) => {
                            match some_payload {
                                Ok(payload) => {
                                    match m.topic() {
                                        "test-evt" => {
                                            let the_event : Result<LedgerEvent<Value>, serde_json::Error> = serde_json::from_str(payload);
                                            match the_event { 
                                                Ok(evt) => {
                                                    // dispatch event to observers
                                                    for hook in &self.hooks {
                                                        hook.on_event(&evt);
                                                    }
                                                    println!("Finished event: {}", evt);
                                                    if let Err(e) = self.consumer.store_offset(&m) {
                                                        warn!("Error while storing offset: {} for event_id {}", e, evt.event_id);
                                                    }   
                                                },
                                                Err(err) => {
                                                    print!("Error while parsing event={} err={}", payload, err);
                                                }
                                            };
                                        },
                                        "test-evt-subscriber" => {
                                            let subscription_event : Result<SubscriptionEvent, serde_json::Error> = serde_json::from_str(payload);
                                            match subscription_event { 
                                                Ok(sub_evt) => {
                                                    // dispatch event to observers
                                                    for hook in &self.hooks {
                                                        hook.on_subscription_event(&sub_evt);
                                                    }
                                                    if let Err(e) = self.consumer.store_offset(&m) {
                                                        warn!("Error while storing offset: {}", e);
                                                    }   
                                                },
                                                Err(err) => {
                                                    print!("Error while parsing event={} err={}", payload, err);
                                                }
                                            };
                                        }
                                        _ => println!("Unknown topic")
                                    }
                                    
                                    println!("PAYLOAD EVENT: {}", payload);
                                },
                                Err(err) => {
                                    print!("Error while reading payload err={}", err);
                                }
                            }
                        },
                        None => println!("Event with no payload key {}", "")
                    };
                }
            }
        }
    }
} 

pub fn start_cmd_workers() {
    let mut threads = Vec::new();
    for worker in KAFKA_CMD_CONFIG.workers {
        let thread_handle = thread::spawn(move || {
                println!("Start worker {}", worker.name);
                let producer = create_producer(KAFKA_EVT_CONFIG.brokers);
                let consumer = create_consumer(KAFKA_CMD_CONFIG.brokers, worker.name, worker.topics);

                start_process_commands(&producer, &consumer, worker.publish_events_topic.unwrap());
                println!("Finished worker {}", worker.name);
            });
        threads.push(thread_handle);
    }

    println!("Waiting for threads to finish");
    for thread in threads {
        thread.join().unwrap();
        println!("Waiting for next thread to finish");
    }
}

pub fn send_event(producer: &FutureProducer, events_topic: &str, evt: &LedgerEvent<Value>) {
    match evt.sys.first_published_at() {
        Some(d) => println!("==========> Parsed datetime {}", d),
        None => {println!("==========> None")}
    }
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

}

pub fn start_process_commands(producer: &FutureProducer, consumer: &LoggingConsumer, publish_events_topic: &str) {

    let lmdb_ctx = create_context().unwrap();

    for message in consumer.start().wait() {
        println!("process command: start");

        match message {
            Err(()) => {
                warn!("Error while reading from stream");
            }
            Ok(Err(e)) => {
                warn!("Kafka error: {}", e);
            }
            Ok(Ok(m)) => {
                println!("process command: ok");
                let gen_content_id = &Uuid::new_v4().to_hyphenated().to_string()[..];
                let new_version_id = &Uuid::new_v4().to_hyphenated().to_string()[..];

                // parse command
                let command: Option<(LedgerCommand<Value>, String)> =  match m.payload_view::<str>() {
                    Some(some_payload) => {
                        match some_payload {
                            Ok(payload) => {
                                let mut hasher = Sha256::new();
                                hasher.input_str("&payload");
                                let digest = hasher.result_str();

                                match serde_json::from_str(payload) {
                                    Ok(cmd) => Some((cmd, digest)),
                                    Err(err) => {
                                        print!("Error while parsing command cmd={} err={}", payload, err);
                                        None
                                    }
                                }
                            },
                            Err(err) => {
                                print!("Error while reading payload err={}", err);
                                None
                            }
                        }
                    },
                    None => None
                };

                // create and send event
                let create_event : Result<&str, Error> = match command {
                    Some((cmd, digest)) => {
                        // Serialize it to a JSON string.
                        let now_utc_str = &Utc::now().to_rfc3339()[..];
                        let new_event_id = &Uuid::new_v4().to_hyphenated().to_string()[..];
                        let user_str = match cmd.user_id {
                            Some(user) => Box::new(user),
                            None => Box::new("") // TODO: decide how to do
                        };
                                            
                        match cmd.action {
                            Action::CREATE{category, content_type, bucket, env} => {
                                info!("CREATE category={} content_type={} bucket={} env={}", category, content_type, bucket, env);

                                match lmdb_ctx.get(&gen_content_id) {
                                    Ok(latest_version) => {
                                        info!("Tried to create a new value with an already existing id or version {}", latest_version);
                                        Err(Error::new(ErrorKind::Other, "Tried to create a new value with an already existing id or version"))
                                    },
                                    Err(err) => match err {
                                        MdbError::NotFound => {
                                            let evt = LedgerEvent {
                                                sys: Sys {
                                                    id: &gen_content_id,
                                                    env: &env,
                                                    category: &category,
                                                    content_type: &content_type,
                                                    bucket: &bucket,
                                                    version: &new_version_id,
                                                    created_by: &user_str,
                                                    created_at: Some(now_utc_str),
                                                    updated_by: &user_str,
                                                    updated_at: Some(now_utc_str),
                                                    first_published_at: Some(now_utc_str),
                                                    published_at: None,
                                                    published_by: None,
                                                    sealed_at: None,
                                                    sealed_by: None,
                                                    previous_version: None,
                                                    published_version: None,
                                                    published_count: 0,
                                                    payload_checksum: Some(&digest)
                                                },
                                                event_id: new_event_id,
                                                action: Action::CREATE{category, content_type, bucket, env},
                                                payload: cmd.payload,
                                                
                                            };
                                            send_event(&producer, publish_events_topic, &evt);
                                            Result::Ok(&gen_content_id)
                                        },
                                        _ => Err(Error::new(ErrorKind::Other, "Could not verify that the id is new"))

                                    }
                                }
                            },
                            Action::UPDATE(revision) => {
                                info!("UPDATE id={} version={}", revision.id, revision.version);
                                match lmdb_ctx.get_latest(&revision.id) {
                                    Ok(l) => match l {
                                        Some(latest_value) => {
                                            if latest_value.sys.version != revision.version {
                                                println!("Optimistic lock error");
                                                Err(Error::new(ErrorKind::Other, "Optimistic lock error, retry change on latest data"))
                                            }
                                            else if latest_value.sys.id != revision.id {
                                                println!("Id mismatch");
                                                Err(Error::new(ErrorKind::Other,"internal id mismatch"))

                                            }
                                            else if latest_value.sys.sealed_at != None {
                                                println!("Document with id={} is sealed at {} by {}", latest_value.sys.id, latest_value.sys.sealed_at.unwrap(), latest_value.sys.sealed_by.unwrap());
                                                Err(Error::new(ErrorKind::Other, "Document is sealed. Copy data to new document."))
                                            }
                                            else {
                                                let evt = LedgerEvent {
                                                    sys: Sys {
                                                        id: &revision.id,
                                                        version: &new_version_id,
                                                        updated_by: &user_str,
                                                        updated_at: Some(now_utc_str),
                                                        previous_version: Some(&revision.version),
                                                        payload_checksum: Some(&digest),
                                                        ..latest_value.sys
                                                    },
                                                    event_id: new_event_id,
                                                    action: Action::UPDATE(revision),
                                                    payload: cmd.payload,
                                                    
                                                };
                                                send_event(&producer, publish_events_topic, &evt);
                                                Result::Ok(&new_version_id)
                                            }
                                        },
                                        None => {
                                            println!("Error cannot update unexisting value");
                                            Err(Error::new(ErrorKind::Other,"Error cannot update unexisting value"))
                                        }
                                    },
                                    Err(err) => {
                                        println!("Error could not look up previous version");
                                         Err(Error::new(ErrorKind::Other, "Error could not look up previous version"))
                                    }
                                }
                            },
                            Action::DELETE(revision) => {
                                info!("DELETE id={} version={}", revision.id, revision.version);
                                match lmdb_ctx.get_latest(&revision.id) {
                                    Ok(l) => match l {
                                        Some(latestValue) => {
                                            if latestValue.sys.version != revision.version {
                                                println!("Optimistic lock error")
                                            }
                                            if latestValue.sys.id != revision.id {
                                                println!("Id mismatch")
                                            }

                                            let evt = LedgerEvent {
                                                sys: Sys {
                                                    id: &revision.id,
                                                    version: &new_version_id,
                                                    updated_by: &user_str,
                                                    updated_at: Some(now_utc_str),
                                                    previous_version: Some(&revision.version),
                                                    payload_checksum: Some(&digest),
                                                    ..latestValue.sys
                                                },
                                                event_id: new_event_id,
                                                action: Action::DELETE(revision),
                                                payload: None,
                                                
                                            };
                                            send_event(&producer, publish_events_topic, &evt);
                                            Result::Ok(&new_version_id)
                                        },
                                        None => {
                                            println!("Error cannot update unexisting value");
                                            Err(Error::new(ErrorKind::Other,"Error cannot update unexisting value"))
                                        }
                                    },
                                    Err(err) => {
                                        println!("Error could not look up previous version err={}", err);
                                         Err(Error::new(ErrorKind::Other, "Error could not look up previous version"))
                                    }
                                }
                            },
                            Action::COPY(revision) => {
                                info!("COPY id={} version={}", revision.id, revision.version);

                                match lmdb_ctx.get_latest(&gen_content_id) {
                                     Ok(l) => match l {
                                        Some(latest_value) => {
                                            if latest_value.sys.version != revision.version {
                                                println!("Optimistic lock error");
                                                Err(Error::new(ErrorKind::Other, "Optimistic lock error, retry change on latest data"))
                                            }
                                            else if latest_value.sys.id != revision.id {
                                                println!("Id mismatch");
                                                Err(Error::new(ErrorKind::Other,"internal id mismatch"))

                                            }
                                            else if latest_value.sys.sealed_at != None {
                                                println!("Document with id={} is sealed at {} by {}", latest_value.sys.id, latest_value.sys.sealed_at.unwrap(), latest_value.sys.sealed_by.unwrap());
                                                Err(Error::new(ErrorKind::Other, "Document is sealed. Copy data to new document."))
                                            }
                                            
                                            else if latest_value.sys.payload_checksum != Some(&digest) {
                                                println!("Invalid copy (unallowed update) command of id={}", latest_value.sys.id);
                                                Err(Error::new(ErrorKind::Other, "Invalid copy (unallowed update) command"))
                                            }
                                            else { 
                                                let evt = LedgerEvent {
                                                    sys: Sys {
                                                        id: &gen_content_id,
                                                        version: &new_version_id,
                                                        updated_by: &user_str,
                                                        updated_at: Some(now_utc_str),
                                                        first_published_at: None,
                                                        published_at: None,
                                                        published_by: None,
                                                        sealed_at: None,
                                                        sealed_by: None,
                                                        previous_version: Some(latest_value.sys.version),
                                                        published_version: None,
                                                        published_count: 0,
                                                        payload_checksum: Some(&digest),
                                                        ..latest_value.sys
                                                    },
                                                    event_id: new_event_id,
                                                    action: Action::COPY(revision),
                                                    payload: cmd.payload,
                                                };    
                                                send_event(&producer, publish_events_topic, &evt);
                                                Result::Ok(&gen_content_id)
                                                }
                                        },
                                        None => {
                                            info!("Error cannot update unexisting value id={} version={}", revision.id, revision.version);
                                            Err(Error::new(ErrorKind::Other,"Error cannot update unexisting value"))
                                        }
                                    },
                                    Err(err) => {
                                         info!("Tried to copy an non existing document id={} version={}", revision.id, revision.version);
                                         Err(Error::new(ErrorKind::Other, "Tried to copy an non existing document"))
                                    }
                                }
                            },
                            Action::SEAL(revision) => {
                                info!("SEAL id={} version={}", revision.id, revision.version);
                                match lmdb_ctx.get_latest(&revision.id) {
                                    Ok(l) => match l {
                                        Some(latest_value) => {
                                            if latest_value.sys.version != revision.version {
                                                info!("Optimistic lock error");
                                                Err(Error::new(ErrorKind::Other, "Optimistic lock error, retry change on latest data"))
                                            }
                                            else if latest_value.sys.id != revision.id {
                                                info!("Id mismatch");
                                                Err(Error::new(ErrorKind::Other,"internal id mismatch"))

                                            }
                                            else if latest_value.sys.sealed_at != None {
                                                info!("Document with id={} is sealed at {} by {}", latest_value.sys.id, latest_value.sys.sealed_at.unwrap(), latest_value.sys.sealed_by.unwrap());
                                                Err(Error::new(ErrorKind::Other, "Document is sealed. Copy data to new document."))
                                            }
                                            else {
                                                let evt = LedgerEvent {
                                                    sys: Sys {
                                                        id: &revision.id,
                                                        version: &new_version_id,
                                                        updated_by: &user_str,
                                                        updated_at: Some(now_utc_str),
                                                        sealed_by: Some(&user_str),
                                                        sealed_at: Some(now_utc_str),
                                                        previous_version: Some(&revision.version),
                                                        payload_checksum: Some(&digest),
                                                        ..latest_value.sys
                                                    },
                                                    event_id: new_event_id,
                                                    action: Action::SEAL(revision),
                                                    payload: cmd.payload,
                                                    
                                                };
                                                send_event(&producer, publish_events_topic, &evt);
                                                Result::Ok(&new_version_id)
                                            }
                                        },
                                        None => {
                                            println!("Error cannot update unexisting value");
                                            Err(Error::new(ErrorKind::Other,"Error cannot update unexisting value"))
                                        }
                                    },
                                    Err(err) => {
                                        println!("Error could not look up previous version err={}", err);
                                         Err(Error::new(ErrorKind::Other, "Error could not look up previous version"))
                                    }
                                }
                            }
                        }
                    },
                    None => {
                        print!("Error while reading empty event");
                        Err(Error::new(ErrorKind::Other, "foo"))
                    }
                    
                    
                };


                match create_event {
                    Ok(id) => {
                        println!("Finished event id: {}", id);
                        if let Err(e) = consumer.store_offset(&m) {
                            warn!("Error while storing offset: {} for event_id {}", e, id);
                        }
                    }
                    Err(_) => {
                        print!("Empty event");
                    }
                }

            }
        }
        println!("process command: end");
    }
}