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
    pub queue_poll_delay: u64,
    pub queue_read_delay: u64
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct KafkaConfig {
    pub topic: String,
    pub group_id: String,
    pub batch_size: usize,
    pub bootstrap_servers: Vec<String>,
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
