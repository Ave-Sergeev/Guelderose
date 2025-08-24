use crate::kafka::kafka_consumer::AnyKafkaConsumer;
use crate::setting::settings::Settings;
use crate::storage::redis_queue::RedisQueue;
use env_logger::Builder;
use log::LevelFilter;
use redis::Client as RedisClient;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;

mod kafka;
mod setting;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::new("config.yaml").map_err(|err| format!("Failed to load setting: {err}"))?;

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
    let redis_queue = Arc::new(RedisQueue::new(multiplexed_connection, settings.redis.clone()));

    let kafka_consumer = AnyKafkaConsumer::new(redis_queue, settings.kafka, settings.redis.clone());
    kafka_consumer
        .consume()
        .await
        .map_err(|err| format!("Kafka consumer error: {err}"))?;

    // TODO: Реализовать поток с Outbox Daemon

    Ok(())
}
