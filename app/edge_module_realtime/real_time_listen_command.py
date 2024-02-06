import asyncio
import datetime
import json
from config.firebase_config import db, ref
from edge_module_mqtt.mqtt_client import MQTTClient, MQTT_CONFIG

def publish_mqtt(device_id, user_id, command):
    mqtt_client = MQTTClient(userID=user_id, **MQTT_CONFIG)
    mqtt_client.connect()
    mqtt_client.start()

    payload = json.dumps({
        'command': command,
    })
    
    mqtt_client.publish_messages(topic=f'esp32/{device_id}/sub', payload=payload)
    
    mqtt_client.stop()

def make_command_change_callback(device_id, user_id):
    def command_change_callback(event):
        print(f'Command changed for {event.path}: {event.data}')
        publish_mqtt(device_id, user_id, event.data)
    return command_change_callback

async def handle_listener_device(device):
    user_id = device.get('userID')
    device_id = device.get('deviceID')
    
    commands_manager_ref = ref.child('commandsManager')
    device_ref = commands_manager_ref.child(device_id).child('command')
    device_ref.listen(make_command_change_callback(device_id, user_id)) 


async def real_time_listen_command_task():
    try:
        devices_ref = db.collection('devices')
        devices = [device.to_dict() for device in devices_ref.get()]

        # Crear y ejecutar tareas para cada dispositivo
        tasks = [handle_listener_device(device) for device in devices]
        await asyncio.gather(*tasks)

    except Exception as e:
        print(f'Error al procesar el mensaje recibido: {e}')
        
