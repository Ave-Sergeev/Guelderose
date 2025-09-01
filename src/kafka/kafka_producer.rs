use crate::models::input_message::InputMessage;
use crate::setting::settings::KafkaConfig;
use anyhow::Error;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub struct AnyKafkaProducer {
    producer: FutureProducer,
    kafka_config: KafkaConfig,
}

impl AnyKafkaProducer {
    pub fn new(kafka_config: KafkaConfig) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka_config.bootstrap_servers.join(","))
            .create()
            .expect("Failed to create Kafka producer");

        AnyKafkaProducer { producer, kafka_config }
    }

    pub async fn send(&self, message: InputMessage) -> Result<(), Error> {
        let topic = self.kafka_config.topics.output.as_str();

        let serialized_message: String = serde_json::to_string(&message)?;

        let record = FutureRecord::to(topic).payload(&serialized_message).key("");

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => {
                log::info!("Sending message to topic: [{topic}]. MessageId: {}", message.id);
                Ok(())
            }
            Err((err, _msg)) => Err(Error::from(err)),
        }
    }
}
