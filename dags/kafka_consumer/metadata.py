from pathlib import Path

import yaml


_METADATA_PATH = Path(__file__).with_name("topics.yaml")
with _METADATA_PATH.open(encoding="utf-8") as _metadata_file:
    _METADATA = yaml.safe_load(_metadata_file)

REQUIRED_FIELDS = tuple(_METADATA["required_fields"])
TOPIC_METADATA = {
    topic: frozenset(config["event_types"])
    for topic, config in _METADATA["topics"].items()
}


def event_types_for_topic(topic):
    try:
        return TOPIC_METADATA[topic]
    except KeyError as exc:
        raise ValueError(f"Topic {topic!r} has no consumer metadata") from exc
