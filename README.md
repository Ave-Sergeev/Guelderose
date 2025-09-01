## Guelderose

---

[Russian version](https://github.com/Ave-Sergeev/Guelderose/blob/main/README.ru.md)

### Description

This project is a kind of connector for asynchronous processing of recognition tasks.

This service performs:
- Consumes messages (recognition tasks) from `Kafka` + sends them to `inbox queue`.
- Receives results/errors from `outbox queue` and publishes them back to `Kafka`.
- Uploads/downloads files for recognition and results between file storages (`S3`).

Supported types of recognition: "TypeOne", TypeTwo".

UPD: The project is not finished, improvements will be added as soon as possible.

### Configuration

In `config.yaml`, the following fields are set:

- `S3`
  - `url` - storage address.
  - `bucket` - name of the bucket used.
  - `access_key` - access key to S3 storage (optional parameter).
  - `secret_key` - secret key for accessing S3 storage (optional parameter).
  - `client_connection_timeout_seconds` - connection lifetime.
- `Redis`
  - `host` - server host.
  - `port` - server port.
  - `username` - login for auth in Redis (optional parameter).
  - `password` - password for auth in Redis (optional parameter).
  - `poll_delay_ms` - delay (in milliseconds) between checking for messages in the queue.
  - `read_delay_ms` - delay (in milliseconds) between reading messages from the queue.
  - `queues` - queue names (keys).
    - `inbox` - input queue name (for recognition jobs).
    - `outbox` - output queue name (for processed jobs).
- `Kafka`
  - `group_id` - consumer group identifier.
  - `batch_size` - batch size (number of messages) consumed at a time.
  - `bootstrap_servers` - list of Kafka broker addresses.
  - `auth` - configuration for SASL_PLAINTEXT auth (optional structure).
    - `username` - username (optional parameter).
    - `password` - password (optional parameter).
    - `protocol` - security protocol (e.g. "SASL_PLAINTEXT").
    - `mechanism` - mechanism (e.g. "PLAIN").
  - `topics` - names of topics in Kafka.
    - `input` - topic name for recognition jobs.
    - `output` - topic name for (un)processed jobs.
- `Logging`
  - `log_level` - level of detail of logs/tracing.

***Important!***  
For S3/Redis/Kafka, override credentials via environment variables to avoid storing secrets in YAML.  
Depending on your Redis configuration, use username, username+password, or neither.  

Environment variables:
- APP__S3__ACCESS_KEY="your_s3_login"
- APP__S3__SECRET_KEY="your_secure_s3_password"
- APP__REDIS__USERNAME="your_redis_login"
- APP__REDIS__PASSWORD="your_secure_redis_password"
- APP__KAFKA__AUTH__USERNAME="your_kafka_login"
- APP__KAFKA__AUTH__PASSWORD="your_secure_kafka_password"

### Implementation details

When the service starts, the following are started and begin to run:

- `outbox_daemon`  
  This is a daemon that reads messages from the `outbox queue` and sends them to `Kafka`.  
  Work logic:  
  When the service starts, an instance is started in a separate thread.
  The daemon polls the `outbox queue` at certain intervals, and if there are messages, sends them to `Kafka`.
- `kafka_consumer`  
  This is a consumer of messages from `Kafka`.  
  Work logic:  
  When the service starts, Consumer is launched in a separate thread.
  Consumer connects to `Kafka` and starts reading messages in batches (of size N).
  All messages in the batch are pushed to the `inbox queue`.
  Then `inbox queue` is polled until all tasks from the batch are processed. Only then this batch committed, and the next one taken.

At the moment, `inbox`/`outbox` queues are implemented as `Redis Lists`, where we write to the tail of the queue, read from the beginning.
In the future, it is advisable to consider `Redis Stream` or `Apache Pulsar` - this will guarantee processing (unlike `Redis Lists`).

### Local startup

1) To install `Rust` on Unix-like systems (MacOS, Linux, ...) - run the command in the terminal.
   After the download is complete, you will get the latest stable version of Rust for your platform, as well as the latest version of Cargo.

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2) Run the following command in the terminal to verify.  
   If the installation is successful (step 1), you will see something like `cargo 1.89.0 ...`.

```shell
cargo --version
```

3) We clone the project from GitHub, open it, and execute the following commands.

Check the code to see if it can be compiled (without running it).
```shell
cargo check
```

Build + run the project (in release mode with optimizations).
```shell
cargo run --release
```

UDP: If you have Windows, see [Instructions here](https://forge.rust-lang.org/infra/other-installation-methods.html).

### Local deployment

To deploy a project locally in `Docker`, you need to:

1) Make sure `Docker daemon` is running.
2) Open a terminal in the project root, go to the `/docker` directory.
3) Run a command (for example `docker compose up -d`) - dependent services (Minio, Redis, Kafka) will start.
4) Go to the Kafka container and run the command to create topics (for example `kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_input_topic`).
5) Go to [MinIO UI](http://localhost:9001/), and create a bucket (for example, named `test`).
6) Start the service itself (instructions in the `Local launch` section).
7) Enjoy using. :wink:
