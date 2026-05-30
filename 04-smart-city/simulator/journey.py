import logging
import time
from kafka.producer import produce_data_to_kafka
from config.settings import *
from confluent_kafka import SerializingProducer
from simulator.vehicle_data import generate_vehicle_data
from simulator.gps_data import generate_gps_data
from simulator.traffic_camera import generate_traffic_camera_data
from simulator.weather_data import generate_weather_data
from simulator.emergency_incident import generate_emergency_incident_data

def simulate_journey(producer: SerializingProducer, device_id: str,
                     logger:logging.Logger):
    """
    Simulate continuous vehicle journey.

    Generates streaming vehicle telemetry data
    and logs the events.
    """
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data["timestamp"], 
                                                           vehicle_data["location"], camera_id="Nikon-Cam")
        weather_data = generate_weather_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])

        logger.debug(f"[Vehicle] {vehicle_data}")
        logger.debug(f"[GPS] {gps_data}")
        logger.debug(f"[Camera] {traffic_camera_data}")
        logger.debug(f"[Weather] {gps_data}")
        logger.debug(f"[Emergency] {gps_data}")

        if (vehicle_data["location"][0] >= BIRMINGHAM_COORDINATES["latitude"] and 
            vehicle_data["location"][0] <= BIRMINGHAM_COORDINATES["longitude"]):
            logger.info("Vehicle has reached Birmingham. Simulation ending")
            break

        produce_data_to_kafka(producer=producer, topic=VEHICLE_TOPIC, data=vehicle_data)
        produce_data_to_kafka(producer=producer, topic=GPS_TOPIC, data=gps_data)
        produce_data_to_kafka(producer=producer, topic=TRAFFIC_TOPIC, data=traffic_camera_data)
        produce_data_to_kafka(producer=producer, topic=WEATHER_TOPIC, data=weather_data)
        produce_data_to_kafka(producer=producer, topic=EMERGENCY_TOPIC, data=emergency_incident_data)

        time.sleep(1)
