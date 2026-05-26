import logging
from confluent_kafka import SerializingProducer
from simulator.vehicle_data import generate_vehicle_data

def simulate_journey(producer: SerializingProducer, device_id: str, 
                     logger:logging.Logger):
    """
    Simulate continuous vehicle journey.

    Generates streaming vehicle telemetry data
    and logs the events.
    """
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        logger.info(vehicle_data)
        break
