import uuid
import random

random.seed(42)

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "incidentId": uuid.uuid4(),
        "type": random.choice(["Accident", "Fire", "Medical", "Police", "None"]),
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "Description of the incident"
    }