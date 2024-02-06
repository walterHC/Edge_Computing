import config.firebase_config
import asyncio
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from edge_module_storage.automated_task_storage import storage_task
from edge_module_realtime.real_time_data_sensor import real_time_update_data_task
from edge_module_realtime.real_time_listen_command import real_time_listen_command_task
from functools import partial

# Definir una tarea parcial para el almacenamiento de datos
partial_storage_task = partial(storage_task, 'aire', 'data-history')

# Crear e iniciar el scheduler
scheduler = AsyncIOScheduler()
# scheduler.add_job(partial_storage_task, 'cron', hour='22,13,18,23', minute='0,0,0,0')
scheduler.add_job(partial_storage_task, 'interval', minutes=5)
scheduler.add_job(real_time_update_data_task, 'interval', seconds=30)
scheduler.start()

# Función para mantener el script ejecutándose indefinidamente
async def main_loop():
    asyncio.create_task(real_time_listen_command_task())
    # asyncio.create_task(real_time_update_data_task())
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Deteniendo el script...")

if __name__ == "__main__":
    # Iniciar el bucle de eventos de asyncio
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main_loop())
    finally:
        scheduler.shutdown()  # Detiene el scheduler de forma ordenada
        loop.close()
        print("Script detenido.")

