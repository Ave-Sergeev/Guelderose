use crate::setting::settings::RedisConfig;
use anyhow::Error;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use std::time::Duration;

pub struct RedisQueue {
    connection: MultiplexedConnection,
    redis_config: RedisConfig,
}

impl RedisQueue {
    pub fn new(connection: MultiplexedConnection, redis_config: RedisConfig) -> Self {
        Self {
            connection,
            redis_config,
        }
    }

    pub async fn push(&self, queue_key: &str, message: &str) -> Result<(), Error> {
        let mut connection = self.connection.clone();

        let serialized_message = serde_json::to_string(message)?;

        let _: i64 = connection.rpush(queue_key, serialized_message).await?;
        log::debug!("Pushed item to queue: [{queue_key}]");

        Ok(())
    }

    pub async fn pop(&self, queue_key: &str) -> Result<Option<String>, Error> {
        let read_delay = Duration::from_millis(self.redis_config.read_delay_ms);
        let mut connection = self.connection.clone();

        loop {
            let result: Option<String> = connection.lpop(queue_key, None).await?;

            match result {
                Some(serialized) => match serde_json::from_str(&serialized) {
                    Ok(item) => {
                        log::debug!("Popped item from queue: [{queue_key}]");
                        return Ok(Some(item));
                    }
                    Err(err) => {
                        log::debug!("Failed to deserialize item from queue [{queue_key}]: {err}");
                        continue;
                    }
                },
                None => tokio::time::sleep(read_delay).await,
            }
        }
    }

    pub async fn check_queue(&self, queue_key: &str, poll_delay: Duration) -> Result<(), Error> {
        let mut connection = self.connection.clone();

        loop {
            let len: usize = connection.llen(queue_key).await?;
            log::debug!("Redis queue check: key='{queue_key}', length={len}");

            if len == 0 {
                return Ok(());
            } else {
                tokio::time::sleep(poll_delay).await;
            }
        }
    }
}
