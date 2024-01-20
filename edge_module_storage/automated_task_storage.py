from edge_module_mqtt.mqtt_client import MQTTClient, MQTT_CONFIG
from config.firebase_config import db
import asyncio
import time
import datetime
import json

async def process_sensor_data(device_data, sensor_type, collection_name):
    user_id = device_data.get('userID')
    device_id = device_data.get('deviceID')

    mqtt_client = MQTTClient(userID=user_id, topic=f"esp32/{device_id}/pub", **MQTT_CONFIG)
    mqtt_client.connect()
    mqtt_client.start()
    
    start_time = time.time()
    while time.time() - start_time < 20:
        if not mqtt_client.messages_received.empty():
            break
        await asyncio.sleep(1)
    
    mqtt_client.stop()
    
    message_received = mqtt_client.get_messages()

    if message_received is None:
        return

    message = json.loads(message_received)
    value = message[sensor_type]

    # Preparar los datos para Firebase
    data = {
        "deviceID": device_id,
        "value": value,
        "timestamp": datetime.datetime.now(),
    }

    # Almacenar en Firebase
    db.collection(collection_name).add(data)

async def storage_task(sensor_type, collection_name):
    devices_ref = db.collection('devices')
    devices = devices_ref.where('deviceType', '==', sensor_type).get()

    tasks = []

    for device in devices:
        device_data = device.to_dict()
        task = asyncio.create_task(process_sensor_data(device_data, sensor_type, collection_name))
        tasks.append(task)
    
    await asyncio.gather(*tasks)