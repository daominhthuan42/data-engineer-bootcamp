import uuid

def generate_traffic_camera_data (device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "camera_id": camera_id,
        "location": location,
        "snapshot": "Base64EncodedString"
    }