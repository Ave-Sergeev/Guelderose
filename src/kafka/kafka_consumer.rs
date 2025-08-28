use crate::models::input_message::InputMessage;
use crate::setting::settings::{KafkaConfig, RedisConfig};
use crate::storage::redis_queue::RedisQueue;
use anyhow::Error;
use futures::stream::StreamExt;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use std::sync::Arc;
use std::time::Duration;

pub struct AnyKafkaConsumer {
    redis_queue: Arc<RedisQueue>,
    consumer: StreamConsumer,
    kafka_config: KafkaConfig,
    redis_config: RedisConfig,
}

impl AnyKafkaConsumer {
    pub fn new(redis_queue: Arc<RedisQueue>, kafka_config: KafkaConfig, redis_config: RedisConfig) -> Self {
        let consumer: StreamConsumer = kafka_config
            .build_kafka_config()
            .create()
            .expect("Consumer creation failed");

        AnyKafkaConsumer {
            redis_queue,
            consumer,
            kafka_config,
            redis_config,
        }
    }

    pub async fn consume(&self) -> Result<(), Error> {
        let topic = self.kafka_config.topics.input.as_str();
        let batch_size = self.kafka_config.batch_size;

        self.consumer.subscribe(&[topic])?;

        let mut batch: Vec<BorrowedMessage> = Vec::with_capacity(batch_size);

        let mut stream = self.consumer.stream();

        while let Some(result) = stream.next().await {
            match result {
                Ok(message) => {
                    batch.push(message);

                    if batch.len() >= batch_size {
                        if let Err(err) = self.process_batch(&batch).await {
                            log::error!("Batch processing error: {err}");
                        }

                        batch.clear();
                    }
                }
                Err(err) => {
                    log::error!("Kafka error: {err}");
                }
            }
        }

        if !batch.is_empty() {
            if let Err(err) = self.process_batch(&batch).await {
                log::error!("Final batch processing error: {err}");
            }
        }

        Ok(())
    }

    async fn process_batch(&self, batch: &[BorrowedMessage<'_>]) -> Result<(), Error> {
        let topic = self.kafka_config.topics.input.as_str();
        let queue_key = self.redis_config.queues.inbox.as_str();
        let poll_delay = Duration::from_millis(self.redis_config.poll_delay_ms);

        for message in batch {
            if let Some(payload) = message.payload() {
                match serde_json::from_slice::<InputMessage>(payload) {
                    Ok(message) => {
                        self.redis_queue.push(queue_key, message.clone()).await?;
                        log::info!(
                            "Message consumed from topic: [{topic}] and pushed to queue: [{queue_key}]. MessageId: {}",
                            message.id
                        );
                    }
                    Err(err) => {
                        log::warn!("Invalid UTF-8 payload at topic [{topic}]: {err}");
                    }
                }
            }
        }

        self.redis_queue.check_queue(queue_key, poll_delay).await?;

        for message in batch {
            self.consumer.commit_message(message, CommitMode::Async)?;
        }

        Ok(())
    }
}
