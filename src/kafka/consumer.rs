use crate::guard::Guard;
use failure::Error;
use log::{info, warn};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::TopicPartitionList;

use guard;

pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
  fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
    match result {
      Ok(_) => info!("Offsets committed successfully"),
      Err(e) => warn!("Error while committing offsets: {}", e),
    };
  }
}

pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

#[allow(dead_code)]
pub fn create_consumer(
  brokers: &str,
  group_id: &str,
  topics: &[&str],
  username: &str,
  password: &str,
) -> Result<LoggingConsumer, Error> {
  let context = LoggingConsumerContext;
  
  let mut client_config = ClientConfig::new();
  client_config.set("group.id", group_id)
    .set("bootstrap.servers", brokers)
    .set("enable.partition.eof", "false")
    .set("session.timeout.ms", "6000")
    // Commit automatically every 5 seconds.
    .set("enable.auto.commit", "true")
    .set("auto.commit.interval.ms", "5000")
    // but only commit the offsets explicitly stored via `consumer.store_offset`.
    .set("enable.auto.offset.store", "false")
    .set_log_level(RDKafkaLogLevel::Debug);

    // auth
    if !username.is_empty() {
      client_config
      .set("security.protocol", "sasl_ssl")
      .set("sasl.mechanism", "PLAIN")
      .set("sasl.username", username)
      .set("sasl.password", password);
    }

  let result: KafkaResult<LoggingConsumer> = client_config.create_with_context(context);
  // let consumer = guard!(result, "Consumer creation failed");
  let consumer = result.unwrap();

  guard!(
    consumer.subscribe(topics),
    "Can't subscribe to specified topic"
  );

  Ok(consumer)
}
