use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

#[allow(dead_code)]
pub fn create_producer(brokers: &str) -> FutureProducer {
  ClientConfig::new()
    .set("bootstrap.servers", brokers)
    .set("queue.buffering.max.ms", "0") // Do not buffer
    .create()
    .expect("Producer creation failed")
}
