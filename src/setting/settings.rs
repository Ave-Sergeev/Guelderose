use anyhow::Result;
use config::Config;
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RedisConfig {
    pub host: String,
    pub port: String,
    pub secret: String,
    pub poll_delay_ms: u64,
    pub read_delay_ms: u64,
    pub queues: RedisQueues,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RedisQueues {
    pub inbox: String,
    pub outbox: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KafkaConfig {
    pub group_id: String,
    pub batch_size: usize,
    pub bootstrap_servers: Vec<String>,
    pub topics: KafkaTopics,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KafkaTopics {
    pub input: String,
    pub output: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Logging {
    pub log_level: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Settings {
    pub redis: RedisConfig,
    pub kafka: KafkaConfig,
    pub logging: Logging,
}

impl Settings {
    pub fn new(location: &str) -> Result<Self> {
        let mut builder = Config::builder();

        if Path::new(location).exists() {
            builder = builder.add_source(config::File::with_name(location));
        } else {
            log::warn!("Configuration file not found");
        }

        let settings = builder.build()?.try_deserialize()?;

        Ok(settings)
    }

    pub fn json_pretty(&self) -> String {
        to_string_pretty(&self).expect("Failed serialize")
    }
}
