use crate::kafka::kafka_producer::AnyKafkaProducer;
use crate::models::input_message::InputMessage;
use crate::setting::settings::Settings;
use crate::storage::redis_queue::RedisQueue;
use anyhow::Error;
use std::sync::Arc;
use std::time::Duration;

pub struct OutboxDaemon {
    redis_queue: Arc<RedisQueue>,
    producer: AnyKafkaProducer,
    config: Arc<Settings>,
}

impl OutboxDaemon {
    pub fn new(redis_queue: Arc<RedisQueue>, config: Arc<Settings>, producer: AnyKafkaProducer) -> Self {
        OutboxDaemon {
            redis_queue,
            producer,
            config,
        }
    }

    async fn process_message(&self, message: InputMessage) -> Result<(), Error> {
        let queue_key = self.config.redis.queues.outbox.as_str();

        // TODO: Добавить выгрузку результата inner_storage (S3), и загрузку в outer_storage (S3). Выгрузка и загрузка происходят перед отправкой message в Kafka.
        let result = self.producer.send(message.clone()).await;

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                log::error!(
                    "Failed to process message: {err}. Message will be returned to the queue: [{queue_key}]. MessageId: {}",
                    message.id
                );

                self.redis_queue.push(queue_key, message).await
            }
        }
    }

    async fn process_queue(&self) -> Result<(), Error> {
        let queue_key = self.config.redis.queues.inbox.as_str();
        let duration = Duration::from_millis(100);

        loop {
            let result = self.redis_queue.pop(queue_key).await?;

            match result {
                Some(message) => {
                    log::info!("Popped message from queue: [{queue_key}]. MessageId: {}", message.id);
                    self.process_message(message).await?;
                }
                None => tokio::time::sleep(duration).await,
            }
        }
    }

    pub async fn start(self) -> Result<(), Error> {
        self.process_queue().await
    }
}
