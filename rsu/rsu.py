from datetime import datetime, timezone
import json
import os
import time
import threading
import uuid
import queue
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


# Constantes de proximidad e inactividad
DISTANCIA_ALERTA_M = 100.0
DISTANCIA_REARME_M = 150.0
TIMEOUT_VEHICULO_S = 10.0

# Frecuencia de comprobación de vehículos inactivos
INTERVALO_LIMPIEZA_S = 1.0

# COLA DE BUFFERING PARA AWS SQS
cola_eventos = queue.Queue(maxsize=10000)


def hilo_sqs_worker():
    """
    Hilo consumidor: procesa la cola de eventos y gestiona la política de 
    buffering y reintentos en caso de desconexión con AWS SQS.
    """
    global QUEUE_URL
    print(f"[{RSU_ID}] Hilo worker de SQS iniciado y esperando eventos.")

    while not stop_event.is_set():
        try:
            # Extraemos un evento de la cola. Usamos timeout para poder 
            # chequear stop_event.is_set() periódicamente y apagar limpiamente.
            event_data = cola_eventos.get(timeout=1.0)
        except queue.Empty:
            continue

        exito = False
        # Política de retención (Buffering / Retry): 
        # Mientras no haya éxito y no nos manden apagar, seguimos reintentando.
        while not exito and not stop_event.is_set():
            if not QUEUE_URL:
                time.sleep(2)
                continue

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
                print(f"🚀 [{RSU_ID}] Evento '{event_type}' de {event_data.get('source_id')} reenviado a SQS")
                
                # Si llegamos aquí, el envío funcionó. Marcamos la tarea como hecha y salimos del bucle.
                exito = True
                cola_eventos.task_done()
                
            except Exception as e:
                print(f"⚠️ [{RSU_ID}] Fallo de conexión con AWS SQS. Buffering activo. Reintentando en 5s... Error: {e}")
                time.sleep(5)  # Esperar antes de reintentar el mismo mensaje


# ============================================================
# BASE DE DATOS LOCAL DEL RSU
# ============================================================

# Vehículos que están enviando telemetría a este RSU.
estado_coches = {}

# Alertas de proximidad actualmente activas.
alertas_proximidad_activas = []

# Evita repetir una alerta mientras la pareja siga en proximidad.
PAREJAS_NOTIFICADAS = set()

# Protege las estructuras compartidas entre los distintos hilos.
estado_lock = threading.Lock()

# Evento para detener el hilo de limpieza.
stop_event = threading.Event()


def actualizar_estado_coche(event_data: dict):
    """
    Actualiza la información de un vehículo en la base de datos local.

    Cada mensaje recibido reinicia su temporizador de actividad.
    """

    source_id = event_data.get("source_id")

    if not source_id:
        return

    location = event_data.get("location") or {}
    payload = event_data.get("payload") or {}

    if not isinstance(location, dict):
        location = {}

    if not isinstance(payload, dict):
        payload = {}

    ahora = time.monotonic()

    with estado_lock:

        anterior = estado_coches.get(source_id, {})

        estado_coches[source_id] = {
            "source_id": source_id,

            "x": float(location.get("x", anterior.get("x", 0.0))),

            "speed": payload.get(
                "speed",
                anterior.get("speed", 0.0)
            ),

            "direction": payload.get(
                "direction",
                anterior.get("direction", "ida")
            ),

            "hazard": payload.get(
                "hazard",
                anterior.get("hazard", False)
            ),

            "event_type": event_data.get(
                "event_type",
                "position"
            ),

            "last_update": datetime.now().strftime("%H:%M:%S"),

            "last_seen": ahora,

            "alerta_proximidad": False,
        }

# ============================================================
# ALERTAS MQTT DE PROXIMIDAD
# ============================================================

def enviar_alerta_proximidad(
    cliente,
    alerta: dict,
    destinatarios: list
):
    """
    Publica una alerta MQTT individual para cada vehículo
    que esté registrado en este RSU.
    """

    mensaje = {
        "message_type": "alert",
        "event_type": "proximity_alert",
        "alert_id": f"alert-{uuid.uuid4().hex[:8]}",
        "event_time": datetime.now(timezone.utc).isoformat(),

        "rsu_id": RSU_ID,

        "vehiculos_involucrados": [
            alerta["coche1"],
            alerta["coche2"]
        ],

        "distancia_m": alerta["distancia"],

        "mismo_sentido": alerta["mismo_sentido"],
    }

    mensaje_json = json.dumps(mensaje)

    for vehicle_id in destinatarios:

        topic = f"vehiculos/{vehicle_id}/alertas"

        try:
            resultado = cliente.publish(
                topic,
                mensaje_json,
                qos=1,
                retain=False
            )

            if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
                print(
                    f"[{RSU_ID}] Alerta de proximidad enviada a "
                    f"{vehicle_id}: {alerta['coche1']} - "
                    f"{alerta['coche2']} ({alerta['distancia']} m)"
                )
            else:
                print(
                    f"[{RSU_ID}] Error publicando alerta para "
                    f"{vehicle_id}: código {resultado.rc}"
                )

        except Exception as e:
            print(
                f"[{RSU_ID}] Error enviando alerta a "
                f"{vehicle_id}: {e}"
            )


# ============================================================
# DETECCIÓN DE PROXIMIDAD
# ============================================================

def verificar_proximidad(cliente):
    """
    Comprueba la distancia entre todos los vehículos registrados.

    - Menos de 100 m: activa la alerta.
    - Más de 150 m: permite volver a generar una alerta.
    - La alerta MQTT se envía una sola vez por cada episodio
      de proximidad y se rearma al superar los 150 m.
    """

    global alertas_proximidad_activas

    alertas = []
    nuevas_alertas = []

    with estado_lock:

        vehiculos = list(estado_coches.values())

        # Reiniciar las marcas de proximidad.
        for vehiculo in vehiculos:
            vehiculo["alerta_proximidad"] = False

        # Comprobar todas las parejas de vehículos.
        for i in range(len(vehiculos)):

            for j in range(i + 1, len(vehiculos)):

                v1 = vehiculos[i]
                v2 = vehiculos[j]

                distancia = abs(v1["x"] - v2["x"])

                if distancia < DISTANCIA_ALERTA_M:

                    v1["alerta_proximidad"] = True
                    v2["alerta_proximidad"] = True

                    alerta = {
                        "coche1": v1["source_id"],
                        "coche2": v2["source_id"],
                        "distancia": round(distancia, 1),
                        "mismo_sentido": (
                            v1["direction"] == v2["direction"]
                        ),
                    }

                    alertas.append(alerta)

                    pareja_key = tuple(
                        sorted([
                            v1["source_id"],
                            v2["source_id"]
                        ])
                    )

                    # Solo generar una alerta nueva al entrar
                    # en una situación de proximidad.
                    if pareja_key not in PAREJAS_NOTIFICADAS:

                        PAREJAS_NOTIFICADAS.add(pareja_key)
                        nuevas_alertas.append(alerta)

                        print(
                            f"[{RSU_ID}] AVISO DE PROXIMIDAD: "
                            f"{v1['source_id']} y {v2['source_id']} "
                            f"están a {distancia:.1f} m"
                        )

        # Rearmar parejas que se han separado suficientemente.
        parejas_a_remover = set()

        for pareja in PAREJAS_NOTIFICADAS:

            id1, id2 = pareja

            # Si uno de los vehículos ha desaparecido del RSU,
            # también se elimina el registro de la pareja.
            if id1 not in estado_coches or id2 not in estado_coches:

                parejas_a_remover.add(pareja)
                continue

            distancia = abs(
                estado_coches[id1]["x"] -
                estado_coches[id2]["x"]
            )

            if distancia > DISTANCIA_REARME_M:
                parejas_a_remover.add(pareja)

        PAREJAS_NOTIFICADAS.difference_update(parejas_a_remover)

        # Todos los coches presentes en la base de datos recibirán
        # las nuevas alertas MQTT.
        destinatarios = list(estado_coches.keys())

        alertas_proximidad_activas = alertas

    # Publicar fuera del bloqueo para no bloquear las actualizaciones
    # de telemetría mientras se envían los mensajes MQTT.
    for alerta in nuevas_alertas:

        enviar_alerta_proximidad(
            cliente,
            alerta,
            destinatarios
        )


# ============================================================
# LIMPIEZA DE VEHÍCULOS INACTIVOS
# ============================================================

def limpiar_vehiculos_inactivos(cliente):
    """
    Elimina de la base de datos local los vehículos que llevan
    más de TIMEOUT_VEHICULO_S sin enviar mensajes.
    """

    ahora = time.monotonic()
    eliminados = []

    with estado_lock:

        for vehicle_id, vehiculo in list(estado_coches.items()):

            tiempo_inactivo = ahora - vehiculo["last_seen"]

            if tiempo_inactivo > TIMEOUT_VEHICULO_S:

                eliminados.append(vehicle_id)
                del estado_coches[vehicle_id]

    for vehicle_id in eliminados:

        print(
            f"[{RSU_ID}] Vehículo {vehicle_id} eliminado: "
            f"más de {TIMEOUT_VEHICULO_S:.0f} s sin recibir telemetría"
        )

    # Al eliminar vehículos, hay que actualizar las alertas
    # y las parejas notificadas.
    if eliminados:
        verificar_proximidad(cliente)


def hilo_limpieza(cliente):
    """Comprueba periódicamente si existen vehículos inactivos."""

    while not stop_event.wait(INTERVALO_LIMPIEZA_S):

        limpiar_vehiculos_inactivos(cliente)



def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[{RSU_ID}] Conectado al Broker MQTT en {MQTT_HOST}:{MQTT_PORT}")
        # Escuchar la telemetría de todos los vehículos conectados a este RSU
        client.subscribe("vehiculos/+/telemetria")
        print(f"[{RSU_ID}] Suscrito a 'vehiculos/+/telemetria'")
    else:
        print(f"[{RSU_ID}] Error al conectar a MQTT, código: {rc}")


def on_message(client, userdata, msg):
    """Callback ejecutado cada vez que un vehículo publica en MQTT."""
    try:
        payload_str = msg.payload.decode("utf-8")
        event_data = json.loads(payload_str)

        #print(f"[{RSU_ID}] Recibido MQTT en topic '{msg.topic}': {event_data.get('source_id')}")

        # 1. Actualizar la base de datos local del RSU.
        actualizar_estado_coche(event_data)

        # 2. Comprobar posibles situaciones de proximidad.
        verificar_proximidad(client)
        
        # 3. Enviar a la cola interna de buffering en lugar de hacer la llamada web
        try:
            cola_eventos.put_nowait(event_data)
        except queue.Full:
            print(f"⚠️ [{RSU_ID}] ADVERTENCIA: La cola de buffering está LLENA. Descartando mensaje de {event_data.get('source_id')}")

    except json.JSONDecodeError:
        print(f"[{RSU_ID}] Trama no válida recibida en {msg.topic}")
    except Exception as e:
        print(f"[{RSU_ID}] Error procesando mensaje MQTT: {e}")


def main():
    global QUEUE_URL

    # 1. Obtener URL de la cola SQS
    try:
        print(f"[{RSU_ID}] Obteniendo URL de la cola SQS '{SQS_QUEUE_NAME}'...")
        respuesta = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        QUEUE_URL = respuesta["QueueUrl"]
        print(f"[{RSU_ID}] Conectado a SQS: {QUEUE_URL}")
    except Exception as e:
        print(f"[{RSU_ID}] Error obteniendo cola SQS: {e}")
        return

    # 2. Configurar e iniciar cliente MQTT
    client_id = f"RSU_Node_{RSU_ID}"
    cliente_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id)

    cliente_mqtt.on_connect = on_connect
    cliente_mqtt.on_message = on_message

    try:
        cliente_mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        cliente_mqtt.loop_start()

        
        # 3. Iniciar el hilo encargado de limpiar vehículos.
        hilo_watchdog = threading.Thread(
            target=hilo_limpieza,
            args=(cliente_mqtt,),
            daemon=True
        )
        hilo_watchdog.start()

        # 4. Iniciar el hilo de envío a SQS (Worker)
        hilo_sqs = threading.Thread(
            target=hilo_sqs_worker,
            daemon=True
        )
        hilo_sqs.start()

        
        print(
            f"[{RSU_ID}] RSU operativo. Presiona Ctrl+C para salir.\n"
            + "-" * 50
        )

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[{RSU_ID}] Apagando RSU...")
    finally:
        stop_event.set()
        cliente_mqtt.loop_stop()
        cliente_mqtt.disconnect()


if __name__ == "__main__":
    main()
