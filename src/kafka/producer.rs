use crate::guard::Guard;
use failure::Error;
use guard;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

#[allow(dead_code)]
pub fn create_producer(brokers: &str) -> Result<FutureProducer, Error> {
  Ok(guard!(
    ClientConfig::new()
      .set("bootstrap.servers", brokers)
      .set("queue.buffering.max.ms", "0") // Do not buffer
      .create::<FutureProducer>(),
    "Producer creation failed"
  ))
}
