extern crate clap;
extern crate futures;
extern crate rdkafka;

use self::rdkafka::client::ClientContext;
use self::rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use self::rdkafka::consumer::stream_consumer::StreamConsumer;
use self::rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use self::rdkafka::topic_partition_list::Offset;
use self::rdkafka::error::KafkaResult;


// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: *mut self::rdkafka::types::RDKafkaTopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }

    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        println!("Pre rebalance kafka consumer");
        match rebalance {
            Rebalance::Assign(topic_partition_list) => {
                for e in topic_partition_list.elements() {
                    match e.offset() {
                        Offset::Beginning => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "Beginning"),
                        Offset::End => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "End"),
                        Offset::Stored => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "Stored"),
                        Offset::Invalid => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "Invalid"),
                        Offset::Offset(offset) => {
                            info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), offset);
                        }
                    };

                    match e.error() {
                        Ok(_) => info!("no error"),
                        Err(err) => error!("Kafka error in pre rebalance: {}", err)
                    }
                    
                }                
            },
            Rebalance::Revoke => {
                info!("All partitions are revoked");
            },
            Rebalance::Error(err) => {
                error!("Pre rebalance error {}", err);
            }
        }
    }

    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        println!("Post rebalance kafka consumer");
        match rebalance {
            Rebalance::Assign(topic_partition_list) => {
                for e in topic_partition_list.elements() {
                    match e.offset() {
                        Offset::Beginning => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "Beginning"),
                        Offset::End => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "End"),
                        Offset::Stored => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "Stored"),
                        Offset::Invalid => info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), "Invalid"),
                        Offset::Offset(offset) => {
                            info!("topic: {} partition: {} offset: {} ", e.topic(), e.partition(), offset);
                        }
                    };

                    match e.error() {
                        Ok(_) => info!("no error"),
                        Err(err) => error!("Kafka error in post rebalance: {}", err)
                    }
                    
                }                
            },
            Rebalance::Revoke => {
                info!("All partitions are revoked");
            },
            Rebalance::Error(err) => {
                error!("Post rebalance error {}", err);
            }
        }

    }

}

// Define a new type for convenience
pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

pub fn create_consumer(brokers: &str, group_id: &str, topics: &[&str]) -> LoggingConsumer {
    let context = LoggingConsumerContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Can't subscribe to specified topic");

    consumer
}

