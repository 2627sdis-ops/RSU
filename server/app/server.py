from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from decimal import Decimal
from botocore.exceptions import ClientError
import json
import os 
import threading
import time
import uuid
import boto3

# Configuracion: de AWS y la cola sqs. 
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")
PORT = int(os.getenv("PORT", "5000"))

sqs = boto3.client("sqs", region_name=AWS_REGION)


############################################## Métricas CloudWatch
CLOUDWATCH_NAMESPACE = os.getenv("CLOUDWATCH_NAMESPACE", "V2X")
METRICAS_INTERVALO_S = float(os.getenv("METRICAS_INTERVALO_S", "10"))
METRICAS_LOTE_MAX = 500  # CloudWatch admite hasta 1000 datos por llamada
TTL_DIAS = int(os.getenv("TTL_DIAS", "7"))

cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)

metricas_pendientes = []
metricas_lock = threading.Lock()
aviso_reloj_mostrado = False


def anotar_metrica(nombre, valor, unidad, dimensiones=None):
    dato = {
        "MetricName": nombre,
        "Value": float(valor),
        "Unit": unidad,
        "Timestamp": datetime.now(timezone.utc),
    }
    if dimensiones:
        dato["Dimensions"] = [
            {"Name": clave, "Value": str(valor_dim)}
            for clave, valor_dim in dimensiones.items()
        ]
    with metricas_lock:
        metricas_pendientes.append(dato)


def enviar_metricas():
    with metricas_lock:
        pendientes = metricas_pendientes[:]
        metricas_pendientes.clear()

    for i in range(0, len(pendientes), METRICAS_LOTE_MAX):
        try:
            cloudwatch.put_metric_data(
                Namespace=CLOUDWATCH_NAMESPACE,
                MetricData=pendientes[i:i + METRICAS_LOTE_MAX],
            )
        except Exception as error:
            # Las métricas nunca deben tumbar el servicio: se descarta el lote.
            print(f"No se pudieron enviar métricas a CloudWatch: {error}", flush=True)


def bucle_envio_metricas():
    while True:
        time.sleep(METRICAS_INTERVALO_S)
        enviar_metricas()


def registrar_metricas(evento: dict, mensaje: dict, procesado: bool):
    """Anota las métricas de un mensaje ya tratado. Nunca lanza excepciones.
    procesado=False significa que el event_id ya existía (duplicado)."""
    global aviso_reloj_mostrado

    try:
        if not procesado:
            anotar_metrica("EventosDuplicados", 1, "Count")
            return

        anotar_metrica("EventosProcesados", 1, "Count")

        rsu_id = str(evento.get("rsu_id") or "desconocida")
        ahora = datetime.now(timezone.utc)

        # Latencia desde que el coche generó el evento.
        event_time = evento.get("event_time")
        if event_time:
            instante = datetime.fromisoformat(str(event_time))
            if instante.tzinfo is None:
                instante = instante.replace(tzinfo=timezone.utc)

            latencia_ms = (ahora - instante).total_seconds() * 1000.0

            if latencia_ms >= 0:
                anotar_metrica("LatenciaExtremoAExtremo", latencia_ms, "Milliseconds")
                anotar_metrica(
                    "LatenciaExtremoAExtremo", latencia_ms, "Milliseconds",
                    {"rsu_id": rsu_id},
                )
            elif not aviso_reloj_mostrado:
                aviso_reloj_mostrado = True
                print(
                    "Aviso: latencia negativa, el reloj del origen va adelantado "
                    "respecto al servidor. Se ignoran esas muestras.",
                    flush=True,
                )

        # Tiempo desde que SQS recibió el mensaje (reloj de AWS).
        enviado_ms = (mensaje.get("Attributes") or {}).get("SentTimestamp")
        if enviado_ms:
            en_cola_ms = ahora.timestamp() * 1000.0 - float(enviado_ms)
            anotar_metrica("TiempoEnCola", max(0.0, en_cola_ms), "Milliseconds")

    except Exception as error:
        print(f"No se pudo registrar la métrica: {error}", flush=True)
#####################################################################

############################################## Nuevo
SNS_TOPIC_ARN = os.getenv(
    "SNS_TOPIC_ARN",
    "arn:aws:sns:us-east-1:TU_ID_CUENTA:v2x-alertas"
)

sns = boto3.client("sns", region_name=AWS_REGION)
###################################################3


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

MAX_ALERTAS_CLOUD = 50

alertas_cloud_activas = []
alertas_cloud_lock = threading.Lock()

# Detección de zona peligrosa: UMBRAL_ZONA incidencias en el mismo tramo de
# TRAMO_M metros dentro de VENTANA_ZONA_S segundos.
TRAMO_M = 500.0
VENTANA_ZONA_S = 300.0
UMBRAL_ZONA = 2
incidencias_por_tramo = {}  # tramo -> lista de (instante, event_id)

################################################################3333


DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "v2x_eventos")
DYNAMODB_ALERTS_TABLE = os.getenv("DYNAMODB_ALERTS_TABLE", "v2x_alertas")
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
tabla_eventos = dynamodb.Table(DYNAMODB_TABLE_NAME)
tabla_alertas = dynamodb.Table(DYNAMODB_ALERTS_TABLE)

def asegurar_tabla_dynamodb(nombre, clave):
    """Crea la tabla en DynamoDB si no existe todavía."""
    tabla = dynamodb.Table(nombre)
    try:
        tabla.load()
        print(f"Tabla DynamoDB '{nombre}' lista.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"Creando tabla DynamoDB '{nombre}'...")
            tabla = dynamodb.create_table(
                TableName=nombre,
                KeySchema=[
                    {"AttributeName": clave, "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": clave, "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            tabla.wait_until_exists()
            print(f"Tabla '{nombre}' creada con éxito.")
        else:
            print(f"Error verificando tabla DynamoDB: {e}")

def borrar_eventos_dynamodb():
    respuesta = tabla_eventos.scan(
        ProjectionExpression="event_id"
    )

    with tabla_eventos.batch_writer() as batch:
        for item in respuesta.get("Items", []):
            batch.delete_item(Key={"event_id": item["event_id"]})

    print("Eventos anteriores eliminados de DynamoDB.")

def float_a_decimal(obj):
    """Convierte floats a Decimal para compatibilidad con DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: float_a_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [float_a_decimal(v) for v in obj]
    return obj

def guardar_evento_dynamodb(evento: dict) -> bool:
    """Guarda un evento en DynamoDB.

    Devuelve False si el event_id ya existía (evento duplicado). Si DynamoDB
    falla, lanza la excepción para que el mensaje se reintente.
    """
    location = evento.get("location") or {}
    payload = evento.get("payload") or {}

    item = {
        "event_id": str(evento.get("event_id") or f"evt-{uuid.uuid4().hex[:12]}"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": str(evento.get("event_type", "desconocido")),
        "source_id": str(evento.get("source_id", "desconocido")),
        "rsu_id": str(evento.get("rsu_id", "desconocida")),
        "x": float_a_decimal(location.get("x", 0.0)),
        "speed": float_a_decimal(payload.get("speed", 0.0)),
        "direction": str(payload.get("direction", "ida")),
        "descripcion": str(
            payload.get("descripcion", payload.get("description", "Sin descripción"))
        ),
        "payload": float_a_decimal(payload),
        "ttl": int(time.time()) + TTL_DIAS * 86400,
    }

    try:
        # Escritura condicional: solo se guarda si el event_id no existe.
        tabla_eventos.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(event_id)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise

    print(f"Evento {item['event_id']} guardado en DynamoDB.", flush=True)
    return True

def guardar_alerta_dynamodb(alerta: dict):
    """Guarda una alerta publicada en la tabla de alertas."""
    item = float_a_decimal(alerta)
    item["ttl"] = int(time.time()) + TTL_DIAS * 86400
    tabla_alertas.put_item(Item=item)


def cargar_alertas_recientes_dynamodb(limite=50):
    """Carga los últimos eventos de DynamoDB al iniciar el servidor."""
    try:
        respuesta = tabla_eventos.scan(Limit=limite)
        items = respuesta.get("Items", [])
        # Ordenar por timestamp descendente
        items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        with alertas_cloud_lock:
            for item in items:
                alertas_cloud_activas.append({
                    "event_id": item.get("event_id"),
                    "event_type": item.get("event_type"),
                    "source_id": item.get("source_id"),
                    "rsu_id": item.get("rsu_id"),
                    "x": float(item.get("x", 0.0)) if item.get("x") is not None else None,
                    "direction": item.get("direction", "ida"),
                    "descripcion": item.get("descripcion", ""),
                    "timestamp": item.get("timestamp", "")[:19].replace("T", " "),
                })
        print(f"Se cargaron {len(items)} eventos históricos desde DynamoDB.")
    except Exception as e:
        print(f"No se pudieron cargar eventos históricos de DynamoDB: {e}")


########################################################################
# ALERTAS
########################################################################

def crear_alerta(alert_type, alert_id, evento, descripcion):
    return {
        "tipo": "alerta_cloud",
        "alert_id": alert_id,
        "alert_type": alert_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "descripcion": descripcion,
        "evento": evento,
    }

def detectar_alertas(evento: dict) -> list:
    """Reglas de alerta del cloud.

    1. Cada incidencia (accidente, obras, bache...) genera un aviso.
    2. Zona peligrosa: UMBRAL_ZONA incidencias en el mismo tramo en la ventana.
    """
    event_id = evento.get("event_id")
    event_type = evento.get("event_type", "desconocido")
    x = float((evento.get("location") or {}).get("x", 0.0))
    tramo = int(x // TRAMO_M) + 1

    alertas = [crear_alerta(
        event_type,
        f"alerta-{event_id}",
        evento,
        f"{event_type} de {evento.get('source_id')} en x={x:.0f} m (tramo {tramo})",
    )]

    ahora = time.time()
    recientes = [
        (instante, eid)
        for instante, eid in incidencias_por_tramo.get(tramo, [])
        if ahora - instante <= VENTANA_ZONA_S
    ]
    # Si es un reintento del mismo evento no se cuenta dos veces.
    if event_id not in [eid for _, eid in recientes]:
        recientes.append((ahora, event_id))
    incidencias_por_tramo[tramo] = recientes

    if len(recientes) == UMBRAL_ZONA:
        alertas.append(crear_alerta(
            "zona_peligrosa",
            f"zona-{tramo}-{event_id}",
            evento,
            f"Zona peligrosa: {len(recientes)} incidencias en el tramo {tramo} "
            f"en los últimos {int(VENTANA_ZONA_S // 60)} min",
        ))

    return alertas

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
        "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S"),
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

def procesar_evento(evento: dict):
    """Procesa un evento. Si algo falla lanza una excepción y el mensaje
    se queda en SQS para reintentarlo."""
       
    
    if not guardar_evento_dynamodb(evento):
        print(
            f"♻️ Evento duplicado ignorado: "
            f"{evento.get('event_id')}",
            flush=True,
        )
        return False

    # Mostrar inmediatamente la última incidencia de este coche.
    registrar_alerta_cloud(evento)

    try:
        for alerta in detectar_alertas(evento):
            publicar_alerta_sns(alerta)
            guardar_alerta_dynamodb(alerta)
            anotar_metrica("AlertasGeneradas", 1, "Count")
   
    except Exception:
        tabla_eventos.delete_item(
            Key={"event_id": str(evento.get("event_id"))}
        )
        raise

    return True

def consumidor_sqs(queue_url):
    # lee de la cola sqs y procesa los eventos
    print(f" Escuchando SQS en segundo plano: {queue_url}", flush=True)

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,  # long polling
                MessageAttributeNames=["All"],
                AttributeNames=["SentTimestamp"],
            )

            for mensaje in response.get("Messages", []):
                try:
                    cuerpo = json.loads(mensaje["Body"])
                    event_type = cuerpo.get("event_type", "position")

                    # El Cloud no procesa telemetría periódica.
                    if event_type == "position":
                        print(f"Telemetría descartada en Cloud: {cuerpo.get('source_id')}")
                        procesado = True
                    else:
                        print(
                            f"☁️ Evento recibido desde {cuerpo.get('rsu_id', 'RSU desconocida')}: "
                            f"{event_type}"
                        )
                        procesado = procesar_evento(cuerpo)

                    registrar_metricas(cuerpo, mensaje, procesado)

                except json.JSONDecodeError:
                    # Un mensaje mal formado no se arregla reintentando: se borra.
                    print("Mensaje con JSON inválido descartado.")

                except Exception as error:
                    # No se borra: SQS lo volverá a entregar al acabar el
                    anotar_metrica("ErroresProcesamiento", 1, "Count")
                    
                    print(f"Error procesando mensaje, se reintentará: {error}", flush=True)
                    continue

                # Solo se borra el mensaje cuando se ha procesado.
                try:
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=mensaje["ReceiptHandle"],
                    )
                except Exception as error:
                    print(f"Error borrando mensaje de SQS: {error}")

        except Exception as error:
            print(f"Error en recepcion SQS: {error}")
            time.sleep(2)


########################################### Nuevo
def publicar_alerta_sns(alerta):
    """Publica la alerta en SNS. Si falla, lanza la excepción."""
    respuesta = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=json.dumps(alerta, ensure_ascii=False),
        Subject=f"Alerta V2X: {alerta['alert_type']}",
        MessageAttributes={
            "alert_type": {
                "DataType": "String",
                "StringValue": alerta["alert_type"],
            }
        }
    )

    print(
        f"Alerta '{alerta['alert_type']}' ({alerta['alert_id']}) publicada en SNS: "
        f"{respuesta['MessageId']}",
        flush=True,
    )
###################################################

class ServidorHTTPNativo(BaseHTTPRequestHandler):
    # Sirve la API JSON y los recursos estaticos

    def _responder(self, codigo, contenido, content_type):
        self.send_response(codigo)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(contenido)))
        self.end_headers()
        self.wfile.write(contenido)

    def _scan_tabla(self, tabla, clave_json):
        items = []
        scan_kwargs = {}

        # scan es paginado: así se recuperan todos los elementos.
        while True:
            resp = tabla.scan(**scan_kwargs)
            items.extend(resp.get("Items", []))

            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break

            scan_kwargs["ExclusiveStartKey"] = last_key

        # Más recientes primero.
        items.sort(
            key=lambda item: item.get("timestamp", ""),
            reverse=True,
        )

        contenido = json.dumps(
            {clave_json: items},
            ensure_ascii=False,
            default=lambda value: float(value) if isinstance(value, Decimal) else str(value),
        ).encode("utf-8")

        self._responder(200, contenido, "application/json; charset=utf-8")

    def do_GET(self):
        ruta = urlsplit(self.path).path

        if ruta in ("/api/data", "/api/coches"):
            with alertas_cloud_lock:
                alertas_cloud = list(alertas_cloud_activas)

            respuesta = {
                "alertas_cloud": alertas_cloud,
            }

            contenido = json.dumps(respuesta, ensure_ascii=False).encode("utf-8")
            self._responder(200, contenido, "application/json; charset=utf-8")
            return

      ###########################################################3
        if ruta in ("/api/historial", "/api/alertas"):
            try:
                if ruta == "/api/historial":
                    self._scan_tabla(tabla_eventos, "eventos")
                else:
                    self._scan_tabla(tabla_alertas, "alertas")
            except Exception as e:
                self._responder(
                    500,
                    json.dumps({"error": str(e)}).encode("utf-8"),
                    "application/json",
                )
            return
###############################################################################

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
    # 1. Inicializar DynamoDB
    try:
        asegurar_tabla_dynamodb(DYNAMODB_TABLE_NAME, "event_id")
        asegurar_tabla_dynamodb(DYNAMODB_ALERTS_TABLE, "alert_id")
        #borrar_eventos_dynamodb()
        #cargar_alertas_recientes_dynamodb()
    except Exception as e:
        print(f"Advertencia DynamoDB en inicio: {e}", flush=True)

    # 2. Conectar a SQS e iniciar hilo consumidor
    respuesta_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
    queue_url = respuesta_url["QueueUrl"]

    hilo_metricas = threading.Thread(target=bucle_envio_metricas, daemon=True)
    hilo_metricas.start()
    
    hilo_sqs = threading.Thread(target=consumidor_sqs, args=(queue_url,), daemon=True)
    hilo_sqs.start()

    # 3. Iniciar servidor web HTTP del Cloud
    httpd = ThreadingHTTPServer(("", PORT), ServidorHTTPNativo)
    print(f" Servidor V2X iniciado en: http://localhost:{PORT}\n")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
