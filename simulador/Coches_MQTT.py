from datetime import datetime, timezone
from pathlib import Path
import configparser
import json
import random
import sys
import threading
import time
import uuid

import paho.mqtt.client as mqtt


LONGITUD_CARRETERA_M = 5000.0  # Carretera de 5 km (0 m a 5.000 m)

# Se rellena al arrancar con las secciones [RSU_x] del archivo de configuración.
RSU_CONFIG = {}

EVENTOS_INCIDENCIA = [
    "frenada_brusca",
    "accidente",
    "aguaplaning",
    "obras",
    "bache",
    "coche_averiado",
]


# Valores por defecto de la anomalía (desactivada).
ANOMALIA = {
    "solo_incidencias": True,
    "duplicado": 0.0,
}


# Registro de las incidencias enviadas (para los experimentos).
LOG_ARCHIVO = None
LOG_LOCK = threading.Lock()


def registrar_envio(event_data: dict, estado: str):
    """Anota en el CSV cada incidencia enviada."""
    if LOG_ARCHIVO is None or event_data.get("event_type") == "position":
        return

    with LOG_LOCK:
        LOG_ARCHIVO.write(
            f"{datetime.now(timezone.utc).isoformat()},"
            f"{event_data['event_id']},"
            f"{event_data['source_id']},"
            f"{event_data['event_type']},"
            f"{estado}\n"
        )
        LOG_ARCHIVO.flush()


class Coche(threading.Thread):
    def __init__(
        self,
        vehicle_id: str,
        posicion_inicial_m: float = 0.0,
        sentido: int = 1,
        velocidad_kmh: float = 80.0,
        prob_alerta: float = 0.01,
        intervalo_s: float = 2.0,
        anomalias: dict = None,
    ):
        super().__init__()
        self.vehicle_id = vehicle_id
        self.posicion_m = max(0.0, min(posicion_inicial_m, LONGITUD_CARRETERA_M))
        self.sentido = 1 if sentido >= 0 else -1
        self.velocidad_kmh = velocidad_kmh
        self.prob_alerta = prob_alerta
        self.intervalo_s = intervalo_s
        self.anomalias = anomalias or dict(ANOMALIA)

        self.recuperando = False
        self.running = True

        self.rsu_actual = None
        self.cliente_mqtt = None

    def _conectar_a_rsu(self, id_rsu: str):
        """Desconecta del RSU anterior y se conecta al nuevo RSU."""
        self._desconectar_mqtt()

        config = RSU_CONFIG[id_rsu]
        self.rsu_actual = id_rsu

        cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{self.vehicle_id}_{id_rsu}")

        cliente.on_connect = self._crear_on_connect(id_rsu)
        cliente.on_message = self._crear_on_message(id_rsu)

        try:
            cliente.connect(config["host"], config["port"], keepalive=60)
            cliente.loop_start()
            self.cliente_mqtt = cliente
        except Exception as e:
            print(f"[{self.vehicle_id}] No se pudo conectar a {id_rsu} ({config['host']}:{config['port']}): {e}")

    def _desconectar_mqtt(self):
        if self.cliente_mqtt is None:
            return

        try:
            self.cliente_mqtt.loop_stop()
            self.cliente_mqtt.disconnect()
        except Exception:
            pass

        self.cliente_mqtt = None

    def _crear_on_connect(self, id_rsu: str):
        config = RSU_CONFIG[id_rsu]

        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code == 0:
                print(f"[{self.vehicle_id}] Conectado exitosamente a {id_rsu} ({config['host']}:{config['port']})")

                topic_alertas = f"vehiculos/{self.vehicle_id}/alertas"
                client.subscribe(topic_alertas, qos=1)
                print(f"[{self.vehicle_id}] Suscrito a alertas: {topic_alertas}")
            else:
                print(f"[{self.vehicle_id}] Error al conectar a {id_rsu}: {reason_code}")

        return on_connect

    def _crear_on_message(self, id_rsu: str):
        def on_message(client, userdata, msg):
            try:
                alerta = json.loads(msg.payload.decode("utf-8"))
                event_type = alerta.get("event_type", "alerta_desconocida")

                if event_type == "proximity_alert":
                    self._mostrar_alerta_proximidad(alerta, id_rsu)
                else:
                    self._mostrar_alerta_cloud(alerta, event_type)

            except Exception as e:
                print(f"[{self.vehicle_id}] Error procesando alerta MQTT: {e}")

        return on_message

    def _mostrar_alerta_proximidad(self, alerta: dict, id_rsu: str):
        involucrados = alerta.get("vehiculos_involucrados", [])
        distancia = alerta.get("distancia_m", 0.0)

        if self.vehicle_id in involucrados:
            otros_coches = [v for v in involucrados if v != self.vehicle_id]

            print(f"\n[{self.vehicle_id}] ALERTA DE PROXIMIDAD con {', '.join(otros_coches)} -- Distancia: {distancia} m")
        else:
            print(f"\n[{self.vehicle_id}] Aviso de proximidad recibido desde {id_rsu} -- Vehículos involucrados: {involucrados}")


    def _mostrar_alerta_cloud(self, alerta: dict, event_type: str):
        alert_id = alerta.get("alert_id", "sin-id")
        descripcion = alerta.get("descripcion", "Sin descripción")
        origen_rsu = alerta.get("origen_rsu", "Cloud")
        posicion = alerta.get("posicion_x", "desconocida")

        print(f"\n☁️🚨 [{self.vehicle_id}] ALERTA CLOUD [{event_type}]  (id={alert_id})\n   Origen: {origen_rsu} | Posición: {posicion}\n   Descripción: {descripcion}")

    def evaluar_handover(self):
        """Selecciona la RSU según la posición del vehículo."""
        target_rsu = "RSU_1" if self.posicion_m < 2500.0 else "RSU_2"

        if self.rsu_actual != target_rsu:
            print(f"{self.vehicle_id} en pos {self.posicion_m:.1f}m pasa de {self.rsu_actual} -> {target_rsu}")
            self._conectar_a_rsu(target_rsu)

    def actualizar_posicion(self, delta_t: float = 1.0):
        if self.recuperando:
            self.velocidad_kmh += 15.0 * delta_t

            if self.velocidad_kmh >= 100.0:
                self.recuperando = False
                print(f"[{self.vehicle_id}] Ha recuperado velocidad crucero ({self.velocidad_kmh:.1f} km/h).")
        else:
            self.velocidad_kmh = random.uniform(100.0, 120.0)

        velocidad_ms = self.velocidad_kmh / 3.6
        self.posicion_m += self.sentido * velocidad_ms * delta_t

        if self.posicion_m >= LONGITUD_CARRETERA_M:
            self.posicion_m = LONGITUD_CARRETERA_M
            self.sentido = -1
            print(f"[{self.vehicle_id}] Ha llegado al final (5 km) y da la vuelta.")
        elif self.posicion_m <= 0.0:
            self.posicion_m = 0.0
            self.sentido = 1
            print(f"[{self.vehicle_id}] Ha llegado al inicio (0 km) y da la vuelta.")

    def _generar_payload(self, event_type: str) -> dict:
        return {
            "event_id": f"evt-{uuid.uuid4().hex[:12]}",
            "source_id": self.vehicle_id,
            "event_type": event_type,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "location": {"x": round(self.posicion_m, 2), "y": 0.0},
            "rsu_asociado": self.rsu_actual,
            "payload": {
                "speed": round(self.velocidad_kmh, 1),
                "direction": "ida" if self.sentido == 1 else "vuelta",
            },
        }

    def _publicar(self, event_data: dict, estado: str = "normal"):
        """Publicación MQTT"""
        cliente = self.cliente_mqtt

        if cliente is None:
            return

        topic = f"vehiculos/{self.vehicle_id}/telemetria"

        try:
            cliente.publish(topic, json.dumps(event_data))
            registrar_envio(event_data, estado)
        except Exception as e:
            print(f"Error publicando mensaje desde {self.vehicle_id}: {e}")

    def _enviar_mqtt(self, event_data: dict):
        """Envía un evento; si la duplicación está activa, a veces lo envía dos veces."""
        if self.cliente_mqtt is None:
            return

        a = self.anomalias

        # Por defecto la telemetría de posición no se toca: la duplicación solo
        # afecta a las incidencias, que son las que llegan al cloud.
        if a["solo_incidencias"] and event_data["event_type"] == "position":
            self._publicar(event_data)
            return

        copias = 2 if random.random() < a["duplicado"] else 1

        for i in range(copias):
            self._publicar(event_data, "duplicado" if i == 1 else "normal")

    def run(self):
        """Bucle principal del vehículo."""
        self.evaluar_handover()

        tiempo_anterior = time.time()

        while self.running:
            inicio_ciclo = time.time()
            delta_t = inicio_ciclo - tiempo_anterior
            tiempo_anterior = inicio_ciclo

            self.actualizar_posicion(delta_t)
            self.evaluar_handover()

            self._enviar_mqtt(self._generar_payload("position"))

            if random.random() < self.prob_alerta:
                tipo_alerta = random.choice(EVENTOS_INCIDENCIA)

                if tipo_alerta == "frenada_brusca":
                    self.velocidad_kmh = random.uniform(40.0, 50.0)
                    self.recuperando = True
                    print(f"⚠️ [{self.vehicle_id}] ¡FRENADA BRUSCA!")

                self._enviar_mqtt(self._generar_payload(tipo_alerta))

            tiempo_transcurrido = time.time() - inicio_ciclo
            time.sleep(max(0.0, self.intervalo_s - tiempo_transcurrido))

    def stop(self):
        self.running = False
        self._desconectar_mqtt()



# CONFIGURACIÓN archivo .txt


def leer_config(ruta: Path) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser(inline_comment_prefixes=("#", ";"))

    if not cfg.read(ruta, encoding="utf-8"):
        raise SystemExit(f"No se encuentra el archivo de configuración: {ruta}")

    return cfg


def anomalias_de(cfg: configparser.ConfigParser, seccion: str) -> dict:

    def valor(clave, defecto, conversion=float):
        for sec in (seccion, "anomalias"):
            if cfg.has_option(sec, clave):
                return conversion(cfg.get(sec, clave))
        return defecto

    def booleano(texto):
        return texto.strip().lower() in ("1", "true", "si", "sí", "yes")

    return {
        "solo_incidencias": valor("solo_incidencias", True, booleano),
        "duplicado": valor("duplicado", 0.0),
    }


if __name__ == "__main__":
    carpeta = Path(__file__).resolve().parent

    ruta_config = Path(sys.argv[1]) if len(sys.argv) > 1 else carpeta / "config_simulador.txt"
    if not ruta_config.is_absolute() and not ruta_config.exists():
        ruta_config = carpeta / ruta_config

    cfg = leer_config(ruta_config)

    for seccion in cfg.sections():
        if seccion.startswith("RSU_"):
            RSU_CONFIG[seccion] = {
                "host": cfg.get(seccion, "host"),
                "port": cfg.getint(seccion, "port", fallback=1883),
            }

    for rsu in ("RSU_1", "RSU_2"):
        if rsu not in RSU_CONFIG:
            raise SystemExit(f"Falta la sección [{rsu}] en {ruta_config}")

    log_csv = cfg.get("general", "log_csv", fallback="").strip()
    if log_csv:
        ruta_log = Path(log_csv)
        if not ruta_log.is_absolute():
            ruta_log = carpeta / ruta_log
        LOG_ARCHIVO = open(ruta_log, "w", encoding="utf-8")
        LOG_ARCHIVO.write("hora_envio,event_id,source_id,event_type,estado\n")
        print(f"Registro de incidencias enviadas en: {ruta_log}")

    coches = []

    for seccion in cfg.sections():
        if not seccion.startswith("coche:"):
            continue

        coches.append(
            Coche(
                vehicle_id=seccion.split(":", 1)[1].strip(),
                posicion_inicial_m=cfg.getfloat(seccion, "posicion_inicial_m", fallback=0.0),
                sentido=cfg.getint(seccion, "sentido", fallback=1),
                velocidad_kmh=cfg.getfloat(seccion, "velocidad_kmh", fallback=80.0),
                prob_alerta=cfg.getfloat(seccion, "prob_alerta", fallback=0.01),
                intervalo_s=cfg.getfloat(seccion, "intervalo_s", fallback=2.0),
                anomalias=anomalias_de(cfg, seccion),
            )
        )

    if not coches:
        raise SystemExit(f"No hay ninguna sección [coche:...] en {ruta_config}")

    print(f"Iniciando simulación coches V2X ({len(coches)} coches, config: {ruta_config.name})")

    for coche in coches:
        coche.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo hilos de la simulación...")

        for coche in coches:
            coche.stop()

        for coche in coches:
            coche.join()

        if LOG_ARCHIVO is not None:
            LOG_ARCHIVO.close()

        print("Simulación finalizada limpiamente.")