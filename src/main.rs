use failure::Error;
use flexi_logger::opt_format;
use tokio;

#[macro_use]
mod guard;
mod dumper;
mod kafka;
mod settings;

#[tokio::main]
async fn main() -> Result<(), Error> {
  flexi_logger::Logger::with_str("info")
    .format(opt_format)
    .start()
    .unwrap();

  let settings = settings::Settings::load()?;
  let topics: Vec<&str> = settings.kafka.topics.split(',').collect();

  let mut handles = vec![];
  for topic in topics {
    let dumper = dumper::KafkaDumper::new(
      &settings.kafka.brokers,
      &settings.kafka.group,
      topic,
      &settings.s3.bucket_prefix,
    )?;
    handles.push(tokio::spawn(async move { dumper.start().await }));
  }
  futures::future::try_join_all(handles).await?;
  Ok(())
}
