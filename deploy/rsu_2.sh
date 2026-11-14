#!/bin/bash
set -e
cd /tmp
aws s3 cp s3://minuevoprueba1/manifest.json ./manifest.json
AWS_REGION="us-east-1"
RSU_ID="RSU_2"
SNS_HTTP_PORT=8443
# 1. Obtener la IP pública actual de la instancia EC2
TOKEN=$(curl -s -S -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)
if [ -n "$TOKEN" ]; then
  PUBLIC_IP=$(curl -s -S -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4 || true)
fi
if [ -z "$PUBLIC_IP" ]; then
  PUBLIC_IP=$(curl -s -S https://checkip.amazonaws.com | tr -d '\n')
fi
echo "=========================================="
echo "RSU ID: $RSU_ID"
echo "IP Pública detectada: $PUBLIC_IP"
echo "=========================================="
# 2. Obtener ARN del Topic SNS
SNS_TOPIC_ARN=$(aws sns create-topic \
  --name v2x-alertas \
  --region "$AWS_REGION" \
  --query 'TopicArn' \
  --output text)
echo "Topic SNS: $SNS_TOPIC_ARN"
# 3. Descargar y desplegar contenedor Docker
IMAGE_URI_RSU=$(grep -oP '(?<="rsu_image_uri": ")[^"]*' manifest.json)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
aws ecr get-login-password --region "$AWS_REGION" | \
  sudo docker login --username AWS --password-stdin \
  "$ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com"
sudo docker pull "$IMAGE_URI_RSU"
sudo docker stop rsu_nodo_1 rsu_nodo_2 2>/dev/null || true
sudo docker rm rsu_nodo_1 rsu_nodo_2 2>/dev/null || true
sudo docker run -d --name rsu_nodo_2 \
  --log-driver=awslogs \
  --log-opt awslogs-region=us-east-1 \
  --log-opt awslogs-group=/v2x/rsu \
  --log-opt awslogs-stream="$RSU_ID" \
  --log-opt awslogs-create-group=true \
  --log-opt mode=non-blocking \
  --log-opt max-buffer-size=4m \
  -p 1883:1883 \
  -p 8443:8443 \
  -p 5001:5001 \
  -e RSU_DASHBOARD_PORT=5001 \
  -e PYTHONUNBUFFERED=1 \
  -e AWS_REGION="$AWS_REGION" \
  -e AWS_DEFAULT_REGION="$AWS_REGION" \
  -e SQS_QUEUE_NAME=v2x-events \
  -e SNS_TOPIC_ARN="$SNS_TOPIC_ARN" \
  -e RSU_ID="$RSU_ID" \
  -e MQTT_PORT=1883 \
  -e SNS_HTTP_PORT="$SNS_HTTP_PORT" \
  "$IMAGE_URI_RSU"
echo "Esperando 3 segundos a que el servidor HTTP de la RSU inicie..."
sleep 3
# 4. Suscribir la IP pública actual al Topic SNS
ENDPOINT_URL="http://${PUBLIC_IP}:${SNS_HTTP_PORT}/sns"
echo "Registrando suscripción en SNS hacia: $ENDPOINT_URL"
aws sns subscribe \
  --topic-arn "$SNS_TOPIC_ARN" \
  --protocol http \
  --notification-endpoint "$ENDPOINT_URL" \
  --region "$AWS_REGION"
echo "Suscripción solicitada con éxito."
sudo docker logs -f rsu_nodo_2
