from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from urllib.request import urlopen

import json
import os
import time
import threading
import uuid
import queue

import boto3
import paho.mqtt.client as mqtt


# ============================================================
# CONFIGURACIÓN
# ============================================================

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
RSU_ID = os.getenv("RSU_ID", "RSU_1")

SNS_HTTP_PORT = int(os.getenv("SNS_HTTP_PORT", "8443"))
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")

sqs = boto3.client("sqs", region_name=AWS_REGION)
QUEUE_URL = None

DISTANCIA_ALERTA_M = 100.0
DISTANCIA_REARME_M = 150.0
TIMEOUT_VEHICULO_S = 10.0
INTERVALO_LIMPIEZA_S = 1.0

cola_eventos = queue.Queue(maxsize=10000)

estado_coches = {}
alertas_proximidad_activas = []
PAREJAS_NOTIFICADAS = set()

estado_lock = threading.Lock()
stop_event = threading.Event()

cliente_mqtt_global = None


# ============================================================
# ENVÍO DE EVENTOS A SQS
# ============================================================

def hilo_sqs_worker():
    global QUEUE_URL

    print(f"[{RSU_ID}] Hilo worker de SQS iniciado y esperando eventos.")

    while not stop_event.is_set():
        try:
            event_data = cola_eventos.get(timeout=1.0)
        except queue.Empty:
            continue

        exito = False

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

                print(
                    f"🚀 [{RSU_ID}] Evento '{event_type}' de "
                    f"{event_data.get('source_id')} reenviado a SQS"
                )

                exito = True
                cola_eventos.task_done()

            except Exception as error:
                print(
                    f"⚠️ [{RSU_ID}] Fallo de conexión con AWS SQS. "
                    f"Buffering activo. Reintentando en 5s... Error: {error}"
                )
                time.sleep(5)


# ============================================================
# BASE DE DATOS LOCAL DEL RSU
# ============================================================

def actualizar_estado_coche(event_data: dict):
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
            "speed": payload.get("speed", anterior.get("speed", 0.0)),
            "direction": payload.get(
                "direction",
                anterior.get("direction", "ida")
            ),
            "hazard": payload.get("hazard", anterior.get("hazard", False)),
            "event_type": event_data.get("event_type", "position"),
            "last_update": datetime.now().strftime("%H:%M:%S"),
            "last_seen": ahora,
            "alerta_proximidad": False,
        }


# ============================================================
# ALERTAS MQTT DE PROXIMIDAD
# ============================================================

def enviar_alerta_proximidad(cliente, alerta: dict, destinatarios: list):
    mensaje = {
        "message_type": "alert",
        "event_type": "proximity_alert",
        "alert_id": f"alert-{uuid.uuid4().hex[:8]}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "rsu_id": RSU_ID,
        "vehiculos_involucrados": [
            alerta["coche1"],
            alerta["coche2"],
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
                retain=False,
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

        except Exception as error:
            print(
                f"[{RSU_ID}] Error enviando alerta a "
                f"{vehicle_id}: {error}"
            )


# ============================================================
# DETECCIÓN LOCAL DE PROXIMIDAD
# ============================================================

def verificar_proximidad(cliente):
    global alertas_proximidad_activas

    alertas = []
    nuevas_alertas = []

    with estado_lock:
        vehiculos = list(estado_coches.values())

        for vehiculo in vehiculos:
            vehiculo["alerta_proximidad"] = False

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
                            v2["source_id"],
                        ])
                    )

                    if pareja_key not in PAREJAS_NOTIFICADAS:
                        PAREJAS_NOTIFICADAS.add(pareja_key)
                        nuevas_alertas.append(alerta)

                        print(
                            f"[{RSU_ID}] AVISO DE PROXIMIDAD: "
                            f"{v1['source_id']} y {v2['source_id']} "
                            f"están a {distancia:.1f} m"
                        )

        parejas_a_remover = set()

        for pareja in PAREJAS_NOTIFICADAS:
            id1, id2 = pareja

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

        destinatarios = list(estado_coches.keys())
        alertas_proximidad_activas = alertas

    for alerta in nuevas_alertas:
        enviar_alerta_proximidad(
            cliente,
            alerta,
            destinatarios,
        )


# ============================================================
# LIMPIEZA DE VEHÍCULOS INACTIVOS
# ============================================================

def limpiar_vehiculos_inactivos(cliente):
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

    if eliminados:
        verificar_proximidad(cliente)


def hilo_limpieza(cliente):
    while not stop_event.wait(INTERVALO_LIMPIEZA_S):
        limpiar_vehiculos_inactivos(cliente)


# ============================================================
# RECEPCIÓN DE RESPUESTAS DEL CLOUD MEDIANTE SNS
# ============================================================

def reenviar_alerta_cloud_por_mqtt(cliente, mensaje_cloud):
    if cliente is None:
        print(f"[{RSU_ID}] MQTT aún no está conectado.")
        return

    alertas = mensaje_cloud.get("alertas", [])

    for alerta in alertas:
        enviar_alerta_proximidad(
            cliente,
            {
                "coche1": alerta.get("coche1"),
                "coche2": alerta.get("coche2"),
                "distancia": alerta.get("distancia", 0.0),
                "mismo_sentido": alerta.get("mismo_sentido", False),
            },
            list(estado_coches.keys()),
        )


class ManejadorSNS(BaseHTTPRequestHandler):
    def _responder(self, codigo=200):
        self.send_response(codigo)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        ruta = urlsplit(self.path).path

        if ruta == "/health":
            self._responder(200)
            return

        self._responder(404)

    def do_POST(self):
        global cliente_mqtt_global

        ruta = urlsplit(self.path).path

        if ruta != "/sns":
            self._responder(404)
            return

        try:
            longitud = int(self.headers.get("Content-Length", 0))
            cuerpo = self.rfile.read(longitud).decode("utf-8")
            mensaje_sns = json.loads(cuerpo)

            tipo = mensaje_sns.get("Type")
            print(f"[{RSU_ID}] SNS recibido: {tipo}", flush=True)

            if tipo == "SubscriptionConfirmation":
                subscribe_url = mensaje_sns.get("SubscribeURL")

                print(
                    f"[{RSU_ID}] Confirmando suscripción SNS: "
                    f"{subscribe_url}",
                    flush=True,
                )

                if subscribe_url:
                    urlopen(subscribe_url, timeout=10)

                self._responder()
                return

            if tipo == "Notification":
                mensaje_cloud = json.loads(
                    mensaje_sns.get("Message", "{}")
                )

                print(
                    f"[{RSU_ID}] Respuesta del cloud recibida por SNS: "
                    f"{mensaje_cloud}",
                    flush=True,
                )

                if mensaje_cloud.get("tipo") == "alerta_proximidad":
                    reenviar_alerta_cloud_por_mqtt(
                        cliente_mqtt_global,
                        mensaje_cloud,
                    )

                self._responder()
                return

            self._responder()

        except Exception as error:
            print(
                f"[{RSU_ID}] Error procesando SNS: {error}",
                flush=True,
            )
            self._responder(500)

    def log_message(self, format, *args):
        return


def iniciar_servidor_sns():
    servidor = ThreadingHTTPServer(
        ("0.0.0.0", SNS_HTTP_PORT),
        ManejadorSNS,
    )

    print(
        f"[{RSU_ID}] Endpoint SNS escuchando en "
        f"puerto {SNS_HTTP_PORT}/sns",
        flush=True,
    )

    servidor.serve_forever()


# Sólo los eventos distintos de "position" viajarán al Cloud.
# Ejemplos: obra, accidente, carretera_cortada, emergencia...
def debe_enviar_a_cloud(event_data: dict) -> bool:
    return event_data.get("event_type", "position") != "position"

# ============================================================
# MQTT
# ============================================================

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(
            f"[{RSU_ID}] Conectado al Broker MQTT en "
            f"{MQTT_HOST}:{MQTT_PORT}"
        )

        client.subscribe("vehiculos/+/telemetria")
        print(f"[{RSU_ID}] Suscrito a 'vehiculos/+/telemetria'")

    else:
        print(
            f"[{RSU_ID}] Error al conectar a MQTT, código: {rc}"
        )


def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        event_data = json.loads(payload_str)

        actualizar_estado_coche(event_data)
        verificar_proximidad(client)

                # La telemetría normal se procesa sólo en el edge.
        # Sólo eventos especiales se reenvían al Cloud por SQS.
        if debe_enviar_a_cloud(event_data):
            event_data["rsu_id"] = RSU_ID

            try:
                cola_eventos.put_nowait(event_data)

                print(
                    f"☁️ [{RSU_ID}] Evento '{event_data.get('event_type')}' "
                    f"enviado al Cloud"
                )

            except queue.Full:
                print(
                    f"⚠️ [{RSU_ID}] Cola Cloud llena. Evento descartado: "
                    f"{event_data.get('event_type')}"
                )

    except json.JSONDecodeError:
        print(
            f"[{RSU_ID}] Trama no válida recibida en {msg.topic}"
        )

    except Exception as error:
        print(
            f"[{RSU_ID}] Error procesando mensaje MQTT: {error}"
        )


# ============================================================
# MAIN
# ============================================================

def main():
    global QUEUE_URL
    global cliente_mqtt_global

    try:
        print(
            f"[{RSU_ID}] Obteniendo URL de la cola SQS "
            f"'{SQS_QUEUE_NAME}'..."
        )

        respuesta = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        QUEUE_URL = respuesta["QueueUrl"]

        print(f"[{RSU_ID}] Conectado a SQS: {QUEUE_URL}")

    except Exception as error:
        print(f"[{RSU_ID}] Error obteniendo cola SQS: {error}")
        return

    cliente_mqtt = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        f"RSU_Node_{RSU_ID}",
    )

    cliente_mqtt.on_connect = on_connect
    cliente_mqtt.on_message = on_message

    cliente_mqtt_global = cliente_mqtt

    try:
        cliente_mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        cliente_mqtt.loop_start()

        hilo_sns = threading.Thread(
            target=iniciar_servidor_sns,
            daemon=True,
        )
        hilo_sns.start()

        hilo_watchdog = threading.Thread(
            target=hilo_limpieza,
            args=(cliente_mqtt,),
            daemon=True,
        )
        hilo_watchdog.start()

        hilo_sqs = threading.Thread(
            target=hilo_sqs_worker,
            daemon=True,
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
