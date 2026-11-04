
import json
import os
import time
import boto3
import paho.mqtt.client as mqtt

# Configuración del entorno y AWS SQS
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")

# Configuración MQTT del RSU (modificables por variables de entorno)
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
RSU_ID = os.getenv("RSU_ID", "RSU_1")

# Inicializar cliente de SQS
sqs = boto3.client("sqs", region_name=AWS_REGION)
QUEUE_URL = None


def enviar_a_sqs(event_data: dict):
    """Reenvía la trama recibida por MQTT directamente a la cola SQS en AWS."""
    global QUEUE_URL
    if not QUEUE_URL:
        return

    try:
        event_type = event_data.get("event_type", "position")

        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(event_data),
            MessageAttributes={
                "TipoEvento": {
                    "StringValue": event_type,
                    "DataType": "String",
                }
            },
        )
        print(
            f"🚀 [{RSU_ID}] Evento '{event_type}' de {event_data.get('source_id')} reenviado a SQS"
        )
    except Exception as e:
        print(f"❌ [{RSU_ID}] Error al reenviar a SQS: {e}")


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ [{RSU_ID}] Conectado al Broker MQTT en {MQTT_HOST}:{MQTT_PORT}")
        # Escuchar la telemetría de todos los vehículos conectados a este RSU
        client.subscribe("vehiculos/+/telemetria")
        print(f"📡 [{RSU_ID}] Suscrito a 'vehiculos/+/telemetria'")
    else:
        print(f"❌ [{RSU_ID}] Error al conectar a MQTT, código: {rc}")


def on_message(client, userdata, msg):
    """Callback ejecutado cada vez que un vehículo publica en MQTT."""
    try:
        payload_str = msg.payload.decode("utf-8")
        event_data = json.loads(payload_str)

        print(
            f"📥 [{RSU_ID}] Recibido MQTT en topic '{msg.topic}': {event_data.get('source_id')}"
        )

        # Reenviar directamente a SQS sin procesar
        enviar_a_sqs(event_data)

    except json.JSONDecodeError:
        print(f"⚠️ [{RSU_ID}] Trama no válida recibida en {msg.topic}")
    except Exception as e:
        print(f"⚠️ [{RSU_ID}] Error procesando mensaje MQTT: {e}")


def main():
    global QUEUE_URL

    # 1. Obtener URL de la cola SQS
    try:
        print(f"🔍 [{RSU_ID}] Obteniendo URL de la cola SQS '{SQS_QUEUE_NAME}'...")
        respuesta = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        QUEUE_URL = respuesta["QueueUrl"]
        print(f"🔗 [{RSU_ID}] Conectado a SQS: {QUEUE_URL}")
    except Exception as e:
        print(f"❌ [{RSU_ID}] Error obteniendo cola SQS: {e}")
        print("Asegúrate de tener configuradas las credenciales de AWS.")
        return

    # 2. Configurar e iniciar cliente MQTT
    client_id = f"RSU_Node_{RSU_ID}"
    cliente_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id)

    cliente_mqtt.on_connect = on_connect
    cliente_mqtt.on_message = on_message

    try:
        cliente_mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        cliente_mqtt.loop_start()

        print(
            f"🟢 [{RSU_ID}] RSU operativo. Presiona Ctrl+C para salir.\n"
            + "-" * 50
        )

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n🛑 [{RSU_ID}] Apagando RSU...")
    finally:
        cliente_mqtt.loop_stop()
        cliente_mqtt.disconnect()


if __name__ == "__main__":
    main()
