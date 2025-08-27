use crate::daemon::outbox_daemon::OutboxDaemon;
use crate::kafka::kafka_consumer::AnyKafkaConsumer;
use crate::kafka::kafka_producer::AnyKafkaProducer;
use crate::setting::settings::Settings;
use crate::storage::redis_queue::RedisQueue;
use env_logger::Builder;
use log::LevelFilter;
use redis::Client as RedisClient;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tokio::signal;

mod daemon;
mod kafka;
mod models;
mod setting;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::new("config.yaml", "APP").map_err(|err| format!("Failed to load setting: {err}"))?;

    Builder::new()
        .filter_level(LevelFilter::from_str(settings.logging.log_level.as_str()).unwrap_or(LevelFilter::Info))
        .init();

    log::info!("Settings:\n{}", settings.json_pretty());

    let connection_url = format!("redis://:{}@{}:{}/", settings.redis.secret, settings.redis.host, settings.redis.port);
    let client = RedisClient::open(connection_url)?;
    let multiplexed_connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| format!("Cannot connect to Redis. Error: {err}"))?;
    log::info!("Successfully connect with Redis");

    let redis_queue = Arc::new(RedisQueue::new(multiplexed_connection, settings.redis.clone()));

    let kafka_consumer = AnyKafkaConsumer::new(redis_queue.clone(), settings.kafka.clone(), settings.redis.clone());
    let kafka_producer = AnyKafkaProducer::new(settings.kafka.clone());

    let outbox_daemon = OutboxDaemon::new(redis_queue.clone(), settings.redis.clone(), kafka_producer);

    let consumer_handle = tokio::spawn(async move {
        if let Err(err) = kafka_consumer.consume().await {
            log::error!("Kafka consumer error: {err}");
        }
    });

    let outbox_handle = tokio::spawn(async move {
        if let Err(err) = outbox_daemon.start().await {
            log::error!("Outbox daemon error: {err}");
        }
    });

    log::info!("Service started successfully. Press Ctrl+C to stop.");
    signal::ctrl_c().await?;
    log::info!("Shutdown signal received");

    consumer_handle.abort();
    outbox_handle.abort();

    Ok(())
}
