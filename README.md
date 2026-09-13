# mq-parity

Interop canary: **[`mq-protocol`](https://github.com/egao1980/mq-protocol)** + **[`mq-backend-amqp`](https://github.com/egao1980/mq-backend-amqp)** vs dockerized **RabbitMQ**, and **[`mq-backend-kafka`](https://github.com/egao1980/mq-backend-kafka)** vs dockerized **Redpanda** (librdkafka CFFI driver).

Protocol unit tests stay in the protocol/backend repos (in-memory / octet-pipe). This repo is **live interop** only.

## Run

Default `asdf:test-system` is green **without Docker** — live cases `skip` when the daemon is unreachable.

```bash
ros -e '(asdf:test-system "mq-parity")' -q
```

Live AMQP + Kafka:

```bash
docker compose up --wait
ros -e '(asdf:test-system "mq-parity")' -q
```

Force skip even if brokers are up:

```bash
PARITY=0 ros -e '(asdf:test-system "mq-parity")' -q
```

## Kafka / Redpanda

The Kafka canary produces 10 keyed messages with headers via `mq-backend-kafka`, consumes + acks (order-per-key and header round-trip), then checks nack/redelivery. It **skips** when `MQ_PARITY_KAFKA_BOOTSTRAP` is unset **and** `docker compose ps` shows no broker. If a broker is available but librdkafka cannot be loaded, it skips with that reason.

Host library (live job / local overlay):

```bash
# Debian/Ubuntu
sudo apt-get install -y librdkafka-dev
# macOS
brew install librdkafka
```

Optional absolute path: `MQ_PARITY_LIBRDKAFKA=/path/to/librdkafka.so`.

## Env

| Variable | Default | Meaning |
|----------|---------|---------|
| `PARITY` | probe | `0`/`false`/`off` skips live cases |
| `MQ_PARITY` | probe | same, AMQP+Kafka |
| `MQ_PARITY_HOST` | `127.0.0.1` | RabbitMQ host |
| `MQ_PARITY_PORT` | `5672` | AMQP port |
| `MQ_PARITY_USER` | `guest` | PLAIN login |
| `MQ_PARITY_PASSWORD` | `guest` | PLAIN password |
| `MQ_PARITY_KAFKA_BOOTSTRAP` | unset | Kafka `host:port`; skip if unset and compose has no broker |
| `MQ_PARITY_LIBRDKAFKA` | unset | optional absolute path to librdkafka |

Live AMQP runs when the TCP probe succeeds **and** neither `PARITY` nor `MQ_PARITY` is off.

## Compose pins

| Service | Image |
|---------|--------|
| RabbitMQ | `rabbitmq:3.13.7-alpine` |
| Redpanda | `redpandadata/redpanda:v24.3.11` (`--overprovisioned`, admin `:9644`) |

## License

MIT
