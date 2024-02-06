import asyncio
import datetime
import random
import json
from config.firebase_config import db, ref
from edge_module_mqtt.mqtt_client import MQTTClient, MQTT_CONFIG

async def subscribe_mqtt(device_id, user_id):
    mqtt_client = MQTTClient(userID=user_id, topic=f"esp32/{device_id}/pub", **MQTT_CONFIG)
    mqtt_client.connect()
    mqtt_client.start()
    
    start_time = datetime.datetime.now()
    while (datetime.datetime.now() - start_time).seconds < 20:
        message = mqtt_client.get_messages()
        if message:
            break
        await asyncio.sleep(1)
    
    mqtt_client.stop()
    return message

async def check_and_update_realtime_database(device_id, value):
    device_ref = ref.child('devices').child(device_id)
    if not device_ref.get():
        device_ref.set({
            'category': 'Unknown',
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'value': 'NN'
        })
    
    # Ahora que sabemos que el dispositivo existe, actualizamos los datos
    await update_realtime_database(device_id, value)


async def update_realtime_database(device_id, value):
    category = categorize_value(value, air_quality_ranges)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = {
        'category': category,
        'timestamp': timestamp,
        'value': value
    }
    ref.child('devices').child(device_id).update(data)

async def handle_device(device):
    user_id = device.get('userID')
    device_id = device.get('deviceID')
    device_type = device.get('deviceType')

    # Suscribirse a MQTT y obtener el mensaje
    message_received = await subscribe_mqtt(device_id, user_id)

    if message_received is None:
        print(f"No se recibió mensaje para el dispositivo {device_id}")
        return

    message = json.loads(message_received)
    value = message[device_type]
    await check_and_update_realtime_database(device_id, value)


async def real_time_update_data_task():
    try:
        devices_ref = db.collection('devices')
        devices = [device.to_dict() for device in devices_ref.get()]

        # Crear y ejecutar tareas para cada dispositivo
        tasks = [handle_device(device) for device in devices]
        await asyncio.gather(*tasks)

    except Exception as e:
        print(f"Error al procesar el mensaje recibido: {e}")
        
def categorize_value(value, ranges):
    for range, category in ranges.items():
        if range[0] <= value <= range[1]:
            return category
    return "Fuera de rango"

# Rangos para la categorización
air_quality_ranges = {
    (10, 300): "Peligroso",
    (300, 400): "Moderado",
    (400, 500): "Bueno",
    (500, 10000): "Insalubre",
}

humidity_ranges = {
    (0, 20): "Muy Seco",
    (21, 40): "Seco",
    (41, 60): "Moderado",
    (61, 80): "Húmedo",
    (81, 100): "Muy Húmedo"
}

temperature_ranges = {
    (-50, 0): "Muy Frío",
    (1, 10): "Frío",
    (11, 20): "Templado",
    (21, 30): "Cálido",
    (31, 50): "Muy Cálido"
}
