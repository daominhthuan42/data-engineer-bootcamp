import random
import uuid
from datetime import datetime, timedelta
from config.settings import *

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()
random.seed(42)

LATITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["latitude"] -
    LONDON_COORDINATES["latitude"]
)

LONGITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["longitude"] -
    LONDON_COORDINATES["longitude"]
)

def _get_next_time():
    """
    Generate next event timestamp.

    Simulates real-time streaming intervals
    by adding random seconds between events.
    """

    global start_time

    # Random update frequency between 30–60 seconds
    start_time += timedelta(seconds=random.randint(30, 60)) # update frequency
    return start_time

def _simulate_vehicle_movement():
    """
    Simulate vehicle GPS movement.

    The vehicle gradually moves from London
    toward Birmingham.

    Small random noise is added to mimic
    realistic road movement.
    """

    global start_location

    # Move toward birmingham
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT

    # Add some randomness to simulate actual road_travel
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id: str):
    """
    Generate simulated vehicle telemetry event.
    """

    location = _simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": _get_next_time().isoformat(),
        "location": (location["latitude"], location["longitude"]),
        "speed": random.uniform(20, 50),
        "direction": "North-East",
        "make": "BMW",
        "year": 2024,
        "fuelType": "Hybrid"
    }
