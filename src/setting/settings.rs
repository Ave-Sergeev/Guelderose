use anyhow::Result;
use config::{Config, Environment};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::path::Path;
use crate::utils::secret::Secret;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct S3Config {
    pub url: String,
    pub bucket: String,
    pub access_key: Option<Secret>,
    pub secret_key: Option<Secret>,
    pub client_connection_timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RedisConfig {
    pub host: String,
    pub port: String,
    pub username: Option<Secret>,
    pub password: Option<Secret>,
    pub poll_delay_ms: u64,
    pub read_delay_ms: u64,
    pub queues: RedisQueues,
}

impl RedisConfig {
    pub fn build_redis_connect_url(&self) -> String {
        let host = &self.host;
        let port = &self.port;

        let username = self.username.clone();
        let password = self.password.clone();

        match (username, password) {
            (Some(user), Some(pass)) => format!("redis://{}:{}@{host}:{port}/", user.reveal(), pass.reveal()),
            (None, Some(pass)) => format!("redis://:{}@{host}:{port}/", pass.reveal()),
            (Some(user), None) => format!("redis://{}@{host}:{port}/", user.reveal()),
            (None, None) => format!("redis://{host}:{port}/"),
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
            .set("bootstrap.servers", self.bootstrap_servers.join(","));

        if let Some(auth_config) = &self.auth {
            config
                .set("security.protocol", &auth_config.protocol)
                .set("sasl.mechanism", &auth_config.mechanism)
                .set("sasl.username", auth_config.username.clone().unwrap().reveal())
                .set("sasl.password", auth_config.password.clone().unwrap().reveal());
        }

        config
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaAuthConfig {
    username: Option<Secret>,
    password: Option<Secret>,
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
    pub s3: S3Config,
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
