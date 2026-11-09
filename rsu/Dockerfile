FROM python:3.10-slim

# Instalar Mosquitto en el sistema operativo del contenedor
RUN apt-get update && \
    apt-get install -y mosquitto && \
    rm -rf /var/lib/apt/lists/*

# Configurar Mosquitto para permitir conexiones externas sin contraseña (necesario para Docker)
RUN echo "listener 1883" > /etc/mosquitto/mosquitto.conf && \
    echo "allow_anonymous true" >> /etc/mosquitto/mosquitto.conf

WORKDIR /rsu

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del RSU y el script de inicio
COPY rsu.py .
COPY start.sh .
COPY static/ ./static/

# Dar permisos de ejecución al script
RUN chmod +x start.sh

# Exponer el puerto MQTT estándar
EXPOSE 1883
EXPOSE 8443
EXPOSE 5001

# Arrancar el contenedor usando el script
CMD ["./start.sh"]
