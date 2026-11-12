from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
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
RSU_DASHBOARD_PORT = int(
    os.getenv("RSU_DASHBOARD_PORT", "5001")
)
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

sqs = boto3.client("sqs", region_name=AWS_REGION)
QUEUE_URL = None

DISTANCIA_ALERTA_M = 100.0
DISTANCIA_REARME_M = 150.0
TIMEOUT_VEHICULO_S = 10.0
INTERVALO_LIMPIEZA_S = 1.0
MAX_MENSAJES_DASHBOARD = 50

cola_eventos = queue.Queue(maxsize=10000)

estado_coches = {}
alertas_proximidad_activas = []
PAREJAS_NOTIFICADAS = set()
mensajes_vehiculos = []
mensajes_cloud = []

estado_lock = threading.Lock()
stop_event = threading.Event()

cliente_mqtt_global = None

# Incidencias que se resuelven en el edge con una alerta local inmediata.
EVENTOS_SOLO_EDGE = {
    "frenada_brusca",
    "aguaplaning",
}
DISTANCIA_AVISO_PELIGRO_M = 300.0

# alert_id de las alertas del cloud ya reenviadas (SNS puede repetir entregas).
alertas_cloud_vistas = set()

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
            # La URL de la cola se obtiene aquí (con reintentos) para que la
            # RSU pueda arrancar aunque AWS no esté disponible.
            if not QUEUE_URL:
                try:
                    QUEUE_URL = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
                    print(f"[{RSU_ID}] Conectado a SQS: {QUEUE_URL}", flush=True)
                except Exception as error:
                    print(
                        f"⚠️ [{RSU_ID}] No se pudo obtener la cola SQS. "
                        f"Buffering activo. Reintentando en 5s... Error: {error}"
                    )
                    time.sleep(5)
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
            #"hazard": payload.get("hazard", anterior.get("hazard", False)),
            "event_type": event_data.get("event_type", "position"),
            "last_update": datetime.now().strftime("%H:%M:%S"),
            "last_seen": ahora,
            "alerta_proximidad": False,
        }


def registrar_mensaje(destino: list, mensaje: dict):
    """Conserva los últimos mensajes recibidos para el dashboard de la RSU."""
    evento = mensaje.get("evento", mensaje)
    if not isinstance(evento, dict):
        evento = {}

    payload = evento.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    registro = {
        "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "source_id": evento.get("source_id", "Cloud"),
        "event_type": mensaje.get(
            "alert_type", evento.get("event_type", mensaje.get("tipo", "desconocido"))
        ),
        "descripcion": mensaje.get(
            "descripcion",
            payload.get("descripcion", payload.get("description", "Sin descripción")),
        ),
        # Se mantiene el mensaje completo para poder inspeccionarlo desde la UI.
        "contenido": mensaje,
    }

    with estado_lock:
        destino.insert(0, registro)
        del destino[MAX_MENSAJES_DASHBOARD:]


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
# ALERTA LOCAL DE FRENADA BRUSCA Y AGUAPLANING
# ============================================================

def alerta_local_peligro(cliente, event_data: dict):
    """Avisa a los coches que se acercan al punto del peligro.

    - frenada_brusca: solo a los que van detrás en el mismo sentido.
    - aguaplaning: a los que se acercan en cualquier sentido.
    """
    origen = event_data.get("source_id")
    event_type = event_data.get("event_type")
    x0 = float((event_data.get("location") or {}).get("x", 0.0))
    sentido = (event_data.get("payload") or {}).get("direction", "ida")

    with estado_lock:
        coches = list(estado_coches.values())

    destinatarios = []
    for coche in coches:
        if coche["source_id"] == origen:
            continue
        if event_type == "frenada_brusca" and coche["direction"] != sentido:
            continue

        distancia = coche["x"] - x0
        # En 'ida' se avanza hacia x creciente: se acerca quien está antes de x0.
        se_acerca = (
            (coche["direction"] == "ida" and distancia <= 0)
            or (coche["direction"] == "vuelta" and distancia >= 0)
        )
        if se_acerca and abs(distancia) <= DISTANCIA_AVISO_PELIGRO_M:
            destinatarios.append((coche["source_id"], round(abs(distancia), 1)))

    for vehicle_id, distancia in destinatarios:
        mensaje = {
            "message_type": "alert",
            "event_type": f"{event_type}_local",
            "alert_id": f"local-{event_data.get('event_id')}",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "rsu_id": RSU_ID,
            "source_id": origen,
            "posicion_x": x0,
            "distancia_m": distancia,
            "descripcion": f"{event_type} de {origen} a {distancia} m por delante",
        }
        cliente.publish(f"vehiculos/{vehicle_id}/alertas", json.dumps(mensaje), qos=1)

    print(
        f"🟠 [{RSU_ID}] {event_type} de {origen} en x={x0:.0f} m: "
        f"aviso local a {len(destinatarios)} vehículos",
        flush=True,
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

    # Alerta generada por V2X-SERVICE (incidencia o zona peligrosa)
    if mensaje_cloud.get("tipo") == "alerta_cloud":
        evento = mensaje_cloud.get("evento", {})
        alerta_evento = {
            "message_type": "alert",
            "event_type": mensaje_cloud.get("alert_type", "alerta_cloud"),
            "alert_id": mensaje_cloud.get("alert_id", f"cloud-{uuid.uuid4().hex[:8]}"),
            "event_time": mensaje_cloud.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "rsu_id": RSU_ID,
            "origen_rsu": evento.get("rsu_id", "desconocida"),
            "source_id": evento.get("source_id", "Cloud"),
            "posicion_x": (evento.get("location") or {}).get("x"),
            "descripcion": mensaje_cloud.get("descripcion", "Aviso de tráfico"),
        }
        alerta_json = json.dumps(alerta_evento)
        with estado_lock:
            vehiculos_destinatarios = list(estado_coches.keys())

        for vid in vehiculos_destinatarios:
            cliente.publish(f"vehiculos/{vid}/alertas", alerta_json, qos=1)
            print(f"[{RSU_ID}] Alerta Cloud enviada a {vid}: {alerta_evento['event_type']}", flush=True)

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

                # SNS entrega "al menos una vez": si ya se reenvió esta
                # alerta, se ignora la repetición.
                alert_id = mensaje_cloud.get("alert_id")
                if alert_id in alertas_cloud_vistas:
                    print(f"[{RSU_ID}] Alerta {alert_id} duplicada ignorada", flush=True)
                    self._responder()
                    return
                if alert_id:
                    alertas_cloud_vistas.add(alert_id)

                registrar_mensaje(mensajes_cloud, mensaje_cloud)
                print(
                    f"[{RSU_ID}] Alerta del cloud recibida por SNS: "
                    f"{mensaje_cloud.get('alert_type')} ({alert_id})",
                    flush=True,
                )
                # Reenviar la alerta a los vehículos conectados vía MQTT
                try:
                    reenviar_alerta_cloud_por_mqtt(cliente_mqtt_global, mensaje_cloud)
                except Exception as err_mqtt:
                    print(f"[{RSU_ID}] Error al reenviar alerta a coches por MQTT: {err_mqtt}", flush=True)
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

class ManejadorDashboardRSU(BaseHTTPRequestHandler):
    def _responder_archivo(self, archivo, content_type):
        try:
            contenido = archivo.read_bytes()
        except FileNotFoundError:
            self._responder_json(404, {"error": "Recurso no encontrado"})
            return

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(contenido)))
        self.end_headers()
        self.wfile.write(contenido)

    def _responder_json(self, codigo, datos):
        contenido = json.dumps(
            datos,
            ensure_ascii=False,
        ).encode("utf-8")

        self.send_response(codigo)
        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )
        self.send_header("Content-Length", str(len(contenido)))
        self.end_headers()
        self.wfile.write(contenido)

    def do_GET(self):
        ruta = urlsplit(self.path).path

        if ruta == "/api/data":
            with estado_lock:
                respuesta = {
                    "rsu_id": RSU_ID,
                    "coches": estado_coches.copy(),
                    "alertas_proximidad": list(
                        alertas_proximidad_activas
                    ),
                    "mensajes_vehiculos": list(mensajes_vehiculos),
                    "mensajes_cloud": list(mensajes_cloud),
                }

            self._responder_json(200, respuesta)
            return

        recursos = {
            "/": (STATIC_DIR / "index.html", "text/html; charset=utf-8"),
            "/index.html": (STATIC_DIR / "index.html", "text/html; charset=utf-8"),
            "/static/css/style.css": (STATIC_DIR / "css" / "style.css", "text/css; charset=utf-8"),
            "/static/js/app.js": (STATIC_DIR / "js" / "app.js", "application/javascript; charset=utf-8"),
        }
        if ruta in recursos:
            archivo, content_type = recursos[ruta]
            self._responder_archivo(archivo, content_type)
            return

        self._responder_json(
            404,
            {"error": "Ruta no encontrada"},
        )

    def log_message(self, format, *args):
        return

def iniciar_dashboard_rsu():
    servidor = ThreadingHTTPServer(
        ("0.0.0.0", RSU_DASHBOARD_PORT),
        ManejadorDashboardRSU,
    )

    print(
        f"[{RSU_ID}] Dashboard Edge escuchando en "
        f"puerto {RSU_DASHBOARD_PORT}",
        flush=True,
    )

    servidor.serve_forever()
        
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


def debe_enviar_a_cloud(event_data: dict) -> bool:
    event_type = event_data.get("event_type", "position")

    # La telemetría normal y estas incidencias se quedan en el edge.
    if event_type == "position" or event_type in EVENTOS_SOLO_EDGE:
        return False

    # accidente, obras, bache y coche_averiado irán al cloud.
    return True

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

        registrar_mensaje(mensajes_vehiculos, event_data)
        actualizar_estado_coche(event_data)
        verificar_proximidad(client)

        # Frenada brusca y aguaplaning: alerta local inmediata, sin cloud.
        if event_data.get("event_type") in EVENTOS_SOLO_EDGE:
            alerta_local_peligro(client, event_data)

        # La telemetría normal se procesa sólo en el edge.
        # Sólo eventos especiales se reenvían al Cloud por SQS.
        if debe_enviar_a_cloud(event_data):
            event_data["rsu_id"] = RSU_ID

            try:
                cola_eventos.put_nowait(event_data)

                print(
                    f"☁️ [{RSU_ID}] Evento '{event_data.get('event_type')}' "
                    f"({event_data.get('event_id')}) enviado al Cloud"
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
    global cliente_mqtt_global

    # La cola SQS la resuelve el hilo worker (con reintentos), así la RSU
    # arranca y da servicio local aunque AWS no esté disponible.
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

        hilo_dashboard = threading.Thread(
            target=iniciar_dashboard_rsu,
            daemon=True,
        )
        hilo_dashboard.start()
        
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
