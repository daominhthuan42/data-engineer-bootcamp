import logging
import json
import uuid
from confluent_kafka import SerializingProducer
from config.settings import *

def create_producer(logger:logging.Logger):
    """
    Create and configure Kafka producer instance.

    This producer is responsible for sending
    streaming events to Kafka topics.
    """

    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
        "error_cb": lambda err: logger.error(f"Kafka error: {err}")
    }

    return SerializingProducer(producer_config)

def _json_serializer(obj):
    """
    Custom JSON serializer for unsupported Python objects.

    This function is used when converting Python objects
    into JSON format. Some object types such as UUID
    are not natively serializable by the standard
    json library.

    Example
    -------
    UUID('123e4567-e89b-12d3-a456-426614174000')

    becomes:

    "123e4567-e89b-12d3-a456-426614174000"
    """

    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def _delivery_report(err, msg):
    """
    Kafka delivery callback function.

    Logs whether a message was successfully
    delivered to Kafka or failed.
    """

    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to: {msg.topic() [msg.partition()]}")

def produce_data_to_kafka(producer: SerializingProducer, topic, data):
    """
    Send event data to Kafka topic.
    """

    producer.produce(topic,
                     key=str(data["id"]), 
                     value=json.dumps(data, default=_json_serializer).encode("utf-8"),
                     on_delivery=_delivery_report
                    )
    producer.flush()
