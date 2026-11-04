#!/bin/bash
echo "Iniciando Broker MQTT (Mosquitto)..."
# Se ejecuta en segundo plano con el '&'
mosquitto -c /etc/mosquitto/mosquitto.conf &

# Esperar 2 segundos para asegurar que el broker está listo
sleep 2

echo "Iniciando Cliente RSU (Python)..."
# Se ejecuta en primer plano
python rsu.py
