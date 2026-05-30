import uuid
import random

random.seed(42)

def generate_gps_data(device_id, timestamp, vehicle_type = "private"):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(0, 40),
        "direction": "North East",
        "vehicleType": vehicle_type
    }
