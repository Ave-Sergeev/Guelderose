use crate::setting::settings::S3Config;
use anyhow::Error;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, ConfigLoader};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;
use std::time::Duration;

pub struct S3Storage {
    client: Client,
    config: S3Config,
}

impl S3Storage {
    pub async fn new(config: S3Config) -> Self {
        let credentials = Credentials::from_keys(&config.access_key.clone().unwrap(), &config.secret_key.clone().unwrap(), None);

        let timeout = TimeoutConfig::builder()
            .operation_timeout(Duration::from_secs(config.client_connection_timeout_seconds))
            .build();

        let sdk_config = ConfigLoader::default()
            .behavior_version(BehaviorVersion::latest())
            .region("eu-central-1")
            .endpoint_url(&config.url)
            .credentials_provider(credentials)
            .timeout_config(timeout)
            .load()
            .await;

        let client = Client::new(&sdk_config);

        S3Storage { client, config }
    }

    pub async fn upload_file(&self, key: &str, file_path: &str) -> Result<(), Error> {
        let path = Path::new(file_path);
        let body = ByteStream::from_path(path).await?;

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body)
            .send()
            .await?;
        log::debug!("Successfully uploaded file to {}/{}", &self.config.bucket, key);

        Ok(())
    }

    pub async fn download_file(&self, key: &str, output_path: &str) -> Result<(), Error> {
        let path = Path::new(output_path);
        let resp = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await?;

        let data = resp.body.collect().await?.into_bytes();
        tokio::fs::write(path, data).await?;
        log::debug!("Successfully downloaded {}/{} to {}", &self.config.bucket, key, output_path);

        Ok(())
    }

    pub async fn put_presigned_url(&self, bucket: &str, key: &str, expire_days: Duration) -> Result<String, Error> {
        self.presigned_url_with_operation(bucket, key, expire_days, S3PreSignOperation::PutObject)
            .await
    }

    pub async fn get_presigned_url(&self, bucket: &str, key: &str, expire_days: Duration) -> Result<String, Error> {
        self.presigned_url_with_operation(bucket, key, expire_days, S3PreSignOperation::GetObject)
            .await
    }

    async fn presigned_url_with_operation(
        &self,
        bucket: &str,
        key: &str,
        expire_days: Duration,
        operation: S3PreSignOperation,
    ) -> Result<String, Error> {
        let presigning_config = PresigningConfig::expires_in(expire_days)?;

        let presigned_request = match operation {
            S3PreSignOperation::GetObject => {
                self.client
                    .get_object()
                    .bucket(bucket)
                    .key(key)
                    .presigned(presigning_config)
                    .await?
            }
            S3PreSignOperation::PutObject => {
                self.client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .presigned(presigning_config)
                    .await?
            }
        };

        let presigned_url = presigned_request.uri().to_string();
        log::debug!("Successfully create url: {}", &presigned_url);

        Ok(presigned_url)
    }
}

enum S3PreSignOperation {
    GetObject,
    PutObject,
}
