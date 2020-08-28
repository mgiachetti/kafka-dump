use crate::kafka::consumer::{create_consumer, LoggingConsumer};
use chrono::{DateTime, TimeZone, Utc};
use log::{error, info, warn};
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use tokio::stream::StreamExt;

use flate2::write::GzEncoder;
use flate2::Compression;
use flate2::GzBuilder;
use std::fs::File;
use std::io::*;

use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};

pub struct KafkaDumper {
  // brokers: String,
  // group_id: String,
  topic: String,
  bucket_prefix: String,
  consumer: LoggingConsumer,
}

fn from_millis(millis: i64) -> DateTime<Utc> {
  Utc.timestamp(millis / 1000, 0)
}

#[allow(dead_code)]
impl KafkaDumper {
  pub fn new(brokers: &str, group_id: &str, topic: &str, bucket_prefix: &str) -> KafkaDumper {
    KafkaDumper {
      // brokers: brokers.into(),
      // group_id: group_id.into(),
      topic: topic.into(),
      bucket_prefix: bucket_prefix.into(),
      consumer: create_consumer(brokers, group_id, &[topic]),
    }
  }

  pub async fn start(&self) {
    let mut stream = self.consumer.start();

    let time_slice: i64 = 5 * 60 * 1000;
    let mut current_slice = 0i64;
    let mut current_file: Option<GzEncoder<File>> = None;
    let mut current_msg: Option<rdkafka::message::BorrowedMessage> = None;
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
          let slice = msg_time - (msg_time % time_slice);

          if current_slice < slice {
            if let Some(_) = current_file {
              current_file
                .unwrap()
                .try_finish()
                .expect("Could not end file");

              let timestamp = from_millis(current_slice);
              let key = format!("{}-{}.gz", timestamp.format("%H:%M:%S"), self.topic);

              let region = Region::UsEast1;
              let s3_client = S3Client::new(region);
              let bucket_name = format!(
                "{}/{}/{}",
                self.bucket_prefix,
                self.topic,
                timestamp.format("%Y-%m-%d")
              );
              let mut content: Vec<u8> = Vec::new();
              File::open(&key)
                .expect("Failed to open gziped file")
                .read_to_end(&mut content)
                .expect("Error Read file");
              let put_request = PutObjectRequest {
                bucket: bucket_name.to_owned(),
                key: key.to_owned(),
                body: Some(content.into()),
                ..Default::default()
              };

              s3_client
                .put_object(put_request)
                .await
                .expect("Failed to put test object");
              std::fs::remove_file(&key).unwrap_or(());

              info!("Pushed to AWS File {}", key);

              self
                .consumer
                .store_offset(&current_msg.unwrap())
                .expect("Error Storing offset");
            }

            current_slice = slice;
            let timestamp = from_millis(slice);
            let key = format!("{}-{}.gz", timestamp.format("%H:%M:%S"), self.topic);
            let f = File::create(&key).expect("Cound not create file");
            current_file = Some(
              GzBuilder::new()
                .filename("dump")
                .write(f, Compression::default()),
            );
            info!("Created File {}", key);
          }
          {
            let mut gz = current_file.unwrap();
            gz.write_all(&msg.payload().unwrap()).unwrap();
            // append new line
            gz.write_all(b"\n").unwrap();
            current_file = Some(gz);
            current_msg = Some(msg);
          }
        }
      }
    }
  }
}
