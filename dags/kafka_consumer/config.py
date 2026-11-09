import os
from dataclasses import dataclass

from kafka_consumer.metadata import TOPIC_METADATA


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap_servers: str
    group_id: str
    client_id: str
    security_protocol: str
    sasl_mechanism: str
    sasl_username: str
    sasl_password: str
    auto_offset_reset: str
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_max_seconds: float
    poll_timeout_seconds: float
    idle_timeout_seconds: float
    max_messages: int
    gcs_bucket: str
    gcs_timeout_seconds: float
    topics: tuple[str, ...]

    @classmethod
    def from_env(cls):
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        if not bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be configured")

        security_protocol = os.getenv(
            "KAFKA_SECURITY_PROTOCOL", "SASL_SSL"
        ).upper()
        sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN")
        sasl_username = os.getenv("KAFKA_SASL_USERNAME", "")
        sasl_password = os.getenv("KAFKA_SASL_PASSWORD", "")
        if security_protocol.startswith("SASL_") and not (
            sasl_username and sasl_password
        ):
            raise ValueError(
                "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required "
                "for SASL authentication"
            )

        auto_offset_reset = os.getenv(
            "KAFKA_AUTO_OFFSET_RESET", "earliest"
        ).lower()
        if auto_offset_reset not in {"earliest", "latest"}:
            raise ValueError(
                "KAFKA_AUTO_OFFSET_RESET must be 'earliest' or 'latest'"
            )

        configured_topics = os.getenv("KAFKA_TOPICS", "").strip()
        topics = (
            tuple(topic.strip() for topic in configured_topics.split(",") if topic.strip())
            if configured_topics
            else tuple(TOPIC_METADATA)
        )
        unknown_topics = set(topics) - set(TOPIC_METADATA)
        if unknown_topics:
            raise ValueError(
                f"Topics are missing metadata definitions: {sorted(unknown_topics)}"
            )

        return cls(
            bootstrap_servers=bootstrap_servers,
            group_id=os.getenv(
                "KAFKA_GROUP_ID", "daemon-supply-chain-consumer"
            ),
            client_id=os.getenv(
                "KAFKA_CLIENT_ID", "daemon-supply-chain-consumer"
            ),
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            auto_offset_reset=auto_offset_reset,
            max_retries=_positive_int("KAFKA_MAX_RETRIES", 5),
            retry_backoff_seconds=_positive_float(
                "KAFKA_RETRY_BACKOFF_SECONDS", 1.0
            ),
            retry_backoff_max_seconds=_positive_float(
                "KAFKA_RETRY_BACKOFF_MAX_SECONDS", 30.0
            ),
            poll_timeout_seconds=_positive_float(
                "KAFKA_POLL_TIMEOUT_SECONDS", 1.0
            ),
            idle_timeout_seconds=_positive_float(
                "KAFKA_IDLE_TIMEOUT_SECONDS", 30.0
            ),
            max_messages=_positive_int("KAFKA_MAX_MESSAGES", 500),
            gcs_bucket=os.getenv("GCS_BUCKET", "daemon-supply-bronze"),
            gcs_timeout_seconds=_positive_float(
                "GCS_REQUEST_TIMEOUT_SECONDS", 60.0
            ),
            topics=topics,
        )

    def kafka_consumer_config(self):
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": True,
            "max.poll.interval.ms": 900000,
        }
        if self.security_protocol.startswith("SASL_"):
            config.update(
                {
                    "sasl.mechanisms": self.sasl_mechanism,
                    "sasl.username": self.sasl_username,
                    "sasl.password": self.sasl_password,
                }
            )
        return config


def _positive_int(name, default):
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name, default):
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value
