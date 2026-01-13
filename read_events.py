from azure.eventhub import EventHubConsumerClient
import json
import pandas as pd


CONSUMER_GROUP = "test-consumer"

# Lista para guardar eventos
eventos_guardados = []

def on_event(partition_context, event):
    data = json.loads(event.body_as_str())
    eventos_guardados.append(data)
    print(f"Evento recibido: {data}")
    # Guardar a CSV cada 5 eventos
    if len(eventos_guardados) % 5 == 0:
        df = pd.DataFrame(eventos_guardados)
        df.to_csv("iot_eventos.csv", index=False)
        print("CSV actualizado")
    partition_context.update_checkpoint(event)

client = EventHubConsumerClient.from_connection_string(
    conn_str=CONNECTION_STRING,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENT_HUB_NAME
)

print("Consumidor IoT iniciado...")

with client:
    client.receive(
        on_event=on_event,
        starting_position="-1"  # lee desde el inicio
    )
