import uuid
import random

random.seed(42)

def generate_weather_data (device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "location": location,
        "temperature": random.uniform(-5, 30),
        "weatherCondition": random.choice(["Sunny", "Cloudy", "Rain", "Snow"]),
        "precipitation": random.uniform(0, 25),
        "winSpeed": random.uniform(0, 100),
        "humidity": random.randint(0, 100), # Percentage
        "airQuantityIndex": random.uniform(0, 500) # AQL Value
    }