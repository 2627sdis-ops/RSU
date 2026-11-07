from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit
import json
import os
import threading
import time
 
import boto3 


# Configuracion: de AWS y la cola sqs.
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")
PORT = int(os.getenv("PORT", "5000"))

sqs = boto3.client("sqs", region_name=AWS_REGION) 

############################################## Nuevo
SNS_TOPIC_ARN = os.getenv(
    "SNS_TOPIC_ARN",
    "arn:aws:sns:us-east-1:TU_ID_CUENTA:v2x-alertas"
)

sns = boto3.client("sns", region_name=AWS_REGION)
###################################################3


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# Variables globales para los coches y los eventos de proximidad
#estado_coches = {}
#alertas_proximidad_activas = []
#PAREJAS_NOTIFICADAS = set()  # Evita repetir avisos mientras siga activa la proximidad.
MAX_ALERTAS_CLOUD = 50

alertas_cloud_activas = []
alertas_cloud_lock = threading.Lock()

def verificar_proximidad():
    # Procesado en servidor para alerta de proximidad
    global alertas_proximidad_activas

    alertas = []
    vehiculos = list(estado_coches.values())

    for vehiculo in vehiculos:
        vehiculo["alerta_proximidad"] = False

    for i in range(len(vehiculos)):
        for j in range(i + 1, len(vehiculos)):
            v1 = vehiculos[i]
            v2 = vehiculos[j]
            distancia = abs(v1["x"] - v2["x"])

            if distancia < 100.0:
                v1["alerta_proximidad"] = True
                v2["alerta_proximidad"] = True

                alertas.append({
                    "coche1": v1["source_id"],
                    "coche2": v2["source_id"],
                    "distancia": round(distancia, 1),
                    "mismo_sentido": v1["direction"] == v2["direction"],
                })

                pareja_key = tuple(sorted([v1["source_id"], v2["source_id"]]))
                if pareja_key not in PAREJAS_NOTIFICADAS:
                    PAREJAS_NOTIFICADAS.add(pareja_key)
                    print(f" AVISO PROXIMIDAD: {v1['source_id']} y {v2['source_id']} estan a {distancia:.1f}m!")

    # La pareja vuelve a poder generar un aviso cuando se separa mas de 150 m.
    parejas_a_remover = set()
    for pareja in PAREJAS_NOTIFICADAS:
        if ( pareja[0] in estado_coches  and  pareja[1] in estado_coches  and  abs(estado_coches[pareja[0]]["x"] - estado_coches[pareja[1]]["x"]) > 150.0 ):
            parejas_a_remover.add(pareja)

    PAREJAS_NOTIFICADAS.difference_update(parejas_a_remover)
    alertas_proximidad_activas = alertas

    ##########################################################333################################################################
    # NUEVO: enviar las alertas al topic SNS
    publicar_alertas_sns(alertas)
    ###############################################################################################################################33

def registrar_alerta_cloud(evento: dict):
    location = evento.get("location") or {}
    payload = evento.get("payload") or {}

    alerta = {
        "event_id": evento.get("event_id"),
        "event_type": evento.get("event_type", "desconocido"),
        "source_id": evento.get("source_id", "desconocido"),
        "rsu_id": evento.get("rsu_id", "desconocida"),
        "x": location.get("x"),
        "direction": payload.get("direction", "ida"),
        "descripcion": payload.get(
            "descripcion",
            payload.get("description", "Sin descripción"),
        ),
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    }

    with alertas_cloud_lock:
        # La vista del cloud representa el estado actual de cada vehículo, no
        # su historial. Si el mismo coche vuelve a avisar, se elimina su aviso
        # anterior antes de guardar el nuevo al principio de la lista.
        source_id = evento.get("source_id")
        if source_id:
            alertas_cloud_activas[:] = [
                alerta_anterior
                for alerta_anterior in alertas_cloud_activas
                if alerta_anterior["source_id"] != source_id
            ]
        alertas_cloud_activas.insert(0, alerta)
        del alertas_cloud_activas[MAX_ALERTAS_CLOUD:]
##################################################################################################################

def consumidor_sqs(queue_url):
    # lee de la cola sqs y actualiza los coches
    print(f" Escuchando SQS en segundo plano: {queue_url}", flush=True)

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
                MessageAttributeNames=["All"],
            )

            if "Messages" in response:
                for mensaje in response["Messages"]:
                    try:
                        cuerpo = json.loads(mensaje["Body"])
                        event_type = cuerpo.get("event_type", "position")

                        # El Cloud no recibe ni procesa telemetría periódica.
                        if event_type == "position":
                            print(
                                f"Telemetría descartada en Cloud: "
                                f"{cuerpo.get('source_id')}"
                            )
                            continue
                        
                        print(
                            f"☁️ Evento recibido desde {cuerpo.get('rsu_id', 'RSU desconocida')}: "
                            f"{event_type}"
                        )
                        
                        #publicar_evento_sns(cuerpo)
                        registrar_alerta_cloud(cuerpo)
                        publicar_evento_sns(cuerpo)
                 
                        #source_id = cuerpo.get("source_id")

                        #if source_id:
                        #    location = cuerpo.get("location", {})
                        #    payload = cuerpo.get("payload", {})
                        #    event_type = cuerpo.get("event_type", "position")

                        #    if event_type != "position":
                        #        print(f" NUEVO EVENTO [{event_type.upper()}]: Vehiculo {source_id} en X={location.get('x', 0.0)}m")

                        #    estado_coches[source_id] = {
                        #        "source_id": source_id,
                        #        "x": location.get("x", 0.0),
                        #        "speed": payload.get("speed", 0.0),
                        #        "direction": payload.get("direction", "ida"),
                        #        "hazard": payload.get("hazard", False),
                        #        "event_type": event_type,
                        #        "last_update": datetime.now().strftime("%H:%M:%S"),
                        #        "alerta_proximidad": False,
                        #    }

                    except Exception as error:
                        print(f"Error procesando mensaje: {error}")
                    finally:
                        # borrar el mensaje procesado.
                        try:
                            sqs.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=mensaje["ReceiptHandle"],
                            )
                        except Exception as error:
                            print(f"Error borrando mensaje de SQS: {error}")

                #verificar_proximidad()

        except Exception as error:
            print(f"Error en recepcion SQS: {error}")

        time.sleep(0.1)


########################################### Nuevo
def publicar_evento_sns(evento):
    mensaje = {
        "tipo": "evento_cloud",
        "timestamp": datetime.now().isoformat(),
        "evento": evento,
    }

    try:
        respuesta = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(mensaje, ensure_ascii=False),
            Subject=f"Evento V2X: {evento.get('event_type', 'desconocido')}",
            MessageAttributes={
                "event_type": {
                    "DataType": "String",
                    "StringValue": evento.get("event_type", "unknown"),
                }
            }
        )

        print(
            f"Evento '{evento.get('event_type')}' publicado en SNS: "
            f"{respuesta['MessageId']}",
            flush=True,
        )

    except Exception as error:
        print(f"Error publicando evento en SNS: {error}", flush=True)
###################################################

class ServidorHTTPNativo(BaseHTTPRequestHandler):
    # Sirve la API JSON y los recursos estaticos

    def _responder(self, codigo, contenido, content_type):
        self.send_response(codigo)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(contenido)))
        self.end_headers()
        self.wfile.write(contenido)

    def do_GET(self):
        ruta = urlsplit(self.path).path

        #if ruta in ("/api/data", "/api/coches"):
            # respuesta = {
            #    "coches": estado_coches,
            #    "alertas_proximidad": alertas_proximidad_activas,
            #}
        if ruta in ("/api/data", "/api/coches"):
            with alertas_cloud_lock:
                alertas_cloud = list(alertas_cloud_activas)

            respuesta = {
                "alertas_cloud": alertas_cloud,
            }

            contenido = json.dumps(respuesta, ensure_ascii=False).encode("utf-8")
            self._responder(200, contenido, "application/json; charset=utf-8")
            return

        if ruta in ("/", "/index.html"):
            archivo = STATIC_DIR / "index.html"
            content_type = "text/html; charset=utf-8"
        elif ruta == "/static/css/style.css":
            archivo = STATIC_DIR / "css" / "style.css"
            content_type = "text/css; charset=utf-8"
        elif ruta == "/static/js/app.js":
            archivo = STATIC_DIR / "js" / "app.js"
            content_type = "application/javascript; charset=utf-8"
        else:
            self.send_error(404, "Recurso no encontrado")
            return

        try:
            contenido = archivo.read_bytes()
        except FileNotFoundError:
            self.send_error(404, "Recurso no encontrado")
            return

        self._responder(200, contenido, content_type)

    def log_message(self, format, *args):
        # Evita llenar la consola con una linea por cada consulta periodica del navegador.
        return


def main():
    # La consulta a AWS se realiza al arrancar.
    respuesta_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
    queue_url = respuesta_url["QueueUrl"]

    hilo_sqs = threading.Thread(target=consumidor_sqs, args=(queue_url,), daemon=True)
    hilo_sqs.start()

    httpd = HTTPServer(("", PORT), ServidorHTTPNativo)
    print(f" Servidor V2X con control de proximidad iniciado en: http://localhost:{PORT}\n")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()




