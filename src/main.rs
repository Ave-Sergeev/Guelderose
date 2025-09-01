use crate::daemon::outbox_daemon::OutboxDaemon;
use crate::kafka::kafka_consumer::AnyKafkaConsumer;
use crate::kafka::kafka_producer::AnyKafkaProducer;
use crate::setting::settings::Settings;
use crate::storage::redis_queue::RedisQueue;
use crate::storage::s3_storage::S3Storage;
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
mod utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::new("config.yaml", "APP").map_err(|err| format!("Failed to load setting: {err}"))?;

    let shared_setting = Arc::new(settings);

    Builder::new()
        .filter_level(LevelFilter::from_str(shared_setting.logging.log_level.as_str()).unwrap_or(LevelFilter::Info))
        .init();

    log::info!("Settings:\n{}", shared_setting.json_pretty());

    let _ = S3Storage::new(shared_setting.s3.clone()).await;
    log::info!("Successfully creates a new client from S3");

    let connection_url = shared_setting.redis.build_redis_connect_url();
    let client = RedisClient::open(connection_url)?;
    let multiplexed_connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| format!("Cannot connect to Redis. Error: {err}"))?;
    log::info!("Successfully connect with Redis");

    let redis_queue = Arc::new(RedisQueue::new(multiplexed_connection, shared_setting.redis.clone()));

    let kafka_consumer =
        AnyKafkaConsumer::new(redis_queue.clone(), shared_setting.redis.clone(), shared_setting.kafka.clone());
    let kafka_producer = AnyKafkaProducer::new(shared_setting.kafka.clone());

    let outbox_daemon = OutboxDaemon::new(redis_queue.clone(), shared_setting.clone(), kafka_producer);

    let consumer_handle = tokio::spawn(async move {
        log::info!("Kafka consumer task started");

        if let Err(err) = kafka_consumer.consume().await {
            log::error!("Kafka consumer error: {err}");
        }
    });

    let outbox_handle = tokio::spawn(async move {
        log::info!("Outbox daemon task started");

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
