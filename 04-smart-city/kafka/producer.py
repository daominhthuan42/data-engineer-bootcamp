import logging
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
