use crate::guard::Guard;
use crate::kafka::consumer::{create_consumer, LoggingConsumer};
use failure::Error;
use log::{error, info, warn};
use rdkafka::Message;
use tokio::stream::StreamExt;

mod slice;

use slice::Slice;

pub struct KafkaDumper {
  // brokers: String,
  // group_id: String,
  topic: String,
  bucket_prefix: String,
  consumer: LoggingConsumer,
}

#[allow(dead_code)]
impl KafkaDumper {
  pub fn new(
    brokers: &str,
    group_id: &str,
    topic: &str,
    bucket_prefix: &str,
  ) -> Result<KafkaDumper, Error> {
    Ok(KafkaDumper {
      // brokers: brokers.into(),
      // group_id: group_id.into(),
      topic: topic.into(),
      bucket_prefix: bucket_prefix.into(),
      consumer: create_consumer(brokers, group_id, &[topic])?,
    })
  }

  pub async fn start(&self) -> Result<(), Error> {
    let mut stream = self.consumer.start();

    let time_slice: i64 = 5 * 60 * 1000;
    let mut current_slice: Option<Slice> = None;
    info!("Listenting to Topic {}", self.topic);
    while let Some(res) = stream.next().await {
      match res {
        Err(e) => error!("Kafka Error: {}", e),
        Ok(msg) => {
          let msg_time = match msg.timestamp().to_millis() {
            Some(time) => time,
            None => {
              warn!("Timestamp empty");
              // skip msg?
              continue;
            }
          };
          let time_slice = msg_time - (msg_time % time_slice);

          if let Some(slice) = &mut current_slice {
            if slice.slice < time_slice {
              guard!(
                slice.store(&self.bucket_prefix, &self.consumer).await,
                "failed to upload"
              );
              current_slice = None;
            }
          }
          if current_slice.is_none() {
            current_slice = Some(Slice::new(time_slice, msg)?);
          } else {
            current_slice = Some(current_slice.unwrap().set_msg(msg));
          }
        }
      }
    }
    Ok(())
  }
}
