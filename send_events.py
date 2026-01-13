from azure.eventhub import EventHubProducerClient, EventData
import json
import time
import random
from datetime import datetime

# Configura tu Event Hub
EVENT_HUB_NAME = "iot-events"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING,
    eventhub_name=EVENT_HUB_NAME
)

def enviar_dato():
    dispositivo = f"Dispositivo_{random.randint(1,5)}"
    estados = ["ok", "error", "warning"]

    dato = {
        "device": dispositivo,
        "status": random.choice(estados),
        "timestamp": datetime.utcnow().isoformat(),
        "temperatura": round(random.uniform(20.0, 35.0), 2),
        "humedad": round(random.uniform(40.0, 80.0), 2)
    }
    return dato

print("Simulador IoT iniciado...")

while True:
    data = enviar_dato()
    with producer:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(data)))
        producer.send_batch(batch)
    print(f"Evento enviado: {data}")
    time.sleep(2)  # envía cada 2 segundos
