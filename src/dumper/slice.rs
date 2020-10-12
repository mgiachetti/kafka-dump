use crate::kafka::consumer::LoggingConsumer;
use chrono::{DateTime, TimeZone, Utc};
use failure::Error;
use log::{info, warn};
use rdkafka::consumer::Consumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

use flate2::write::GzEncoder;
use flate2::Compression;
use flate2::GzBuilder;
use std::fs::File;
use std::io::{Read, Write};

use crate::guard::Guard;
use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};

pub fn date_time_from_millis(millis: i64) -> DateTime<Utc> {
  Utc.timestamp(millis / 1000, 0)
}

pub struct Slice<'a> {
  pub slice: i64,
  pub last_msg: BorrowedMessage<'a>,
  file: GzEncoder<File>,
}

impl Slice<'_> {
  pub fn new<'a>(slice: i64, first_msg: BorrowedMessage<'a>) -> Result<Slice<'a>, Error> {
    let timestamp = date_time_from_millis(slice);
    let file_name = format!("{}-{}.gz", timestamp.format("%H:%M:%S"), first_msg.topic());
    let f = guard!(File::create(&file_name), "Cound not create file");
    let file = GzBuilder::new()
      .filename("dump")
      .write(f, Compression::default());
    info!("Created File {}", file_name);
    Ok(Slice::from_file(slice, first_msg, file))
  }

  pub fn from_file<'a>(
    slice: i64,
    first_msg: BorrowedMessage<'a>,
    mut file: GzEncoder<File>,
  ) -> Slice<'a> {
    file.write_all(first_msg.payload().unwrap()).unwrap();
    // append new line
    file.write_all(b"\n").unwrap();

    Slice {
      slice,
      file: file,
      last_msg: first_msg,
    }
  }

  pub fn set_msg<'a>(self, msg: BorrowedMessage<'a>) -> Slice<'a> {
    Slice::from_file(self.slice, msg, self.file)
  }

  async fn upload(&self, bucket: &str, key: &str, content: Vec<u8>) -> Result<(), Error> {
    let region = Region::UsEast1;
    let s3_client = S3Client::new(region);
    let put_request = PutObjectRequest {
      bucket: bucket.to_owned(),
      key: key.to_owned(),
      body: Some(content.into()),
      ..Default::default()
    };
    s3_client.put_object(put_request).await?;
    std::fs::remove_file(&key).unwrap_or(());
    Ok(())
  }

  pub async fn store(
    &mut self,
    bucket_prefix: &str,
    consumer: &LoggingConsumer,
  ) -> Result<(), Error> {
    guard!(self.file.try_finish(), "Could not end file");

    let msg = &self.last_msg;
    let topic = msg.topic();
    let timestamp = date_time_from_millis(self.slice);
    let key = format!("{}-{}.gz", timestamp.format("%H:%M:%S"), topic);

    let bucket_name = format!(
      "{}/{}/{}",
      bucket_prefix,
      topic,
      timestamp.format("%Y-%m-%d")
    );
    let mut content: Vec<u8> = Vec::new();
    let error_msg = format!("Failed to open gziped file {}", &key);
    guard!(
      guard!(File::open(&key), error_msg).read_to_end(&mut content),
      "Error Read file"
    );

    {
      let mut attempts: u8 = 1;
      loop {
        match self.upload(&bucket_name, &key, content.clone()).await {
          Ok(_) => {
            break;
          }
          Err(_) => {
            warn!("Failed to push AWS File {} on attempt {}", key, attempts);
            attempts += 1;
            if attempts > 10 {
              failure::bail!("Failed to PUSH to AWS");
            }
          }
        }
      }
    }

    info!("Pushed to AWS File {}", key);

    guard!(consumer.store_offset(msg), "Error Storing offset");
    Ok(())
  }
}
