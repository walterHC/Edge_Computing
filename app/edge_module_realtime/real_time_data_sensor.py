import asyncio
import datetime
import random
import json
import pytz
from config.firebase_config import db, ref, messaging
from edge_module_mqtt.mqtt_client import MQTTClient, MQTT_CONFIG

last_readings = {}
PELIGROSO_THRESHOLD = 2000

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

async def send_alert_notification(device_token):
    message = messaging.Message(
        notification=messaging.Notification(
            title="Alerta de sensor",
            body="El valor del sensor se encuentra por encima del umbral permitido"
        ),
        token=device_token
    )

    response = messaging.send(message)
    print('Mensaje enviado con exito: ', response)

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
    lima_tz = pytz.timezone('America/Lima')
    timestamp = datetime.datetime.now(lima_tz).strftime('%Y-%m-%d %H:%M:%S')
    data = {
        'category': category,
        'timestamp': timestamp,
        'value': value
    }
    ref.child('devices').child(device_id).update(data)

async def handle_device(device):
    global last_readings
    user_id = device.get('userID')
    device_id = device.get('deviceID')
    device_type = device.get('deviceType')

    if device_id not in last_readings:
        last_readings[device_id] = [0, 0, 0]

    # Suscribirse a MQTT y obtener el mensaje
    message_received = await subscribe_mqtt(device_id, user_id)

    if message_received is None:
        print(f"No se recibió mensaje para el dispositivo {device_id}")
        return

    message = json.loads(message_received)
    value = message[device_type]

    last_readings[device_id].append(value)
    if len(last_readings[device_id]) > 3:
        last_readings[device_id].pop(0)

    if sum(1 for read in last_readings[device_id] if read > PELIGROSO_THRESHOLD) >= 2:
        last_readings[device_id] = [0, 0, 0]
        await send_alert_notification(device.get('deviceToken'))

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
    (1000, 1500): "Bajo",
    (1500, 2000): "Normal",
    (2000, 2500): "Peligroso",
    (2500, 10000): "Muy Peligroso",
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
