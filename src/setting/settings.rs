use anyhow::Result;
use config::{Config, Environment};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RedisConfig {
    pub host: String,
    pub port: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub poll_delay_ms: u64,
    pub read_delay_ms: u64,
    pub queues: RedisQueues,
}

impl RedisConfig {
    pub fn build_redis_connect_url(&self) -> String {
        let host = &self.host;
        let port = &self.port;

        let username = self.username.as_deref();
        let password = self.password.as_deref();

        match (username, password) {
            (Some(user), Some(pass)) => format!("redis://{}:{}@{}:{}/", user, pass, host, port),
            (None, Some(pass)) => format!("redis://:{}@{}:{}/", pass, host, port),
            (Some(user), None) => format!("redis://{}@{}:{}/", user, host, port),
            (None, None) => format!("redis://{}:{}/", host, port),
        }
    }
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
    pub auth: Option<KafkaAuthConfig>,
    pub topics: KafkaTopics,
}

impl KafkaConfig {
    pub fn build_kafka_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        config
            .set("group.id", &self.group_id)
            .set("bootstrap.servers", &self.bootstrap_servers.join(","));

        match &self.auth {
            Some(auth_config) => {
                config
                    .set("security.protocol", &auth_config.protocol)
                    .set("sasl.mechanism", &auth_config.mechanism)
                    .set("sasl.username", auth_config.username.clone().unwrap())
                    .set("sasl.password", auth_config.password.clone().unwrap());
            }
            None => {}
        }

        config
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaAuthConfig {
    username: Option<String>,
    password: Option<String>,
    protocol: String,
    mechanism: String,
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
    pub fn new(location: &str, env_prefix: &str) -> Result<Self> {
        let mut builder = Config::builder();

        if Path::new(location).exists() {
            builder = builder.add_source(config::File::with_name(location));
        } else {
            log::warn!("Configuration file not found");
        }

        builder = builder.add_source(
            Environment::with_prefix(env_prefix)
                .separator("__")
                .prefix_separator("__"),
        );

        let settings = builder.build()?.try_deserialize()?;

        Ok(settings)
    }

    pub fn json_pretty(&self) -> String {
        to_string_pretty(&self).expect("Failed serialize")
    }
}
