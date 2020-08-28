use failure::Error;
use flexi_logger::opt_format;
use tokio;

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

  for topic in topics {
    let dumper = dumper::KafkaDumper::new(
      &settings.kafka.brokers,
      &settings.kafka.group,
      topic,
      &settings.s3.bucket_prefix,
    );
    tokio::spawn(async move {
      dumper.start().await;
    });
  }
  Ok(())
}
