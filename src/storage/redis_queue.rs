use crate::models::input_message::InputMessage;
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

    pub async fn push(&self, queue_key: &str, message: InputMessage) -> Result<(), Error> {
        let mut connection = self.connection.clone();

        let serialized_message = serde_json::to_vec(&message)?;

        let _: i64 = connection.rpush(queue_key, serialized_message).await?;

        Ok(())
    }

    pub async fn pop(&self, queue_key: &str) -> Result<Option<InputMessage>, Error> {
        let read_delay = Duration::from_millis(self.redis_config.read_delay_ms);
        let mut connection = self.connection.clone();

        loop {
            let result: Option<String> = connection.lpop(queue_key, None).await?;

            match result {
                Some(serialized_message) => match serde_json::from_slice::<InputMessage>(serialized_message.as_bytes())
                {
                    Ok(message) => return Ok(Some(message)),
                    Err(err) => {
                        log::error!("Failed to deserialize message from queue [{queue_key}]: {err}");
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
