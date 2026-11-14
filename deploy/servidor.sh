#!/bin/bash
set -e

cd /tmp
aws s3 cp s3://minuevoprueba1/manifest.json ./manifest.json

AWS_REGION="us-east-1"
SNS_TOPIC_ARN=$(aws sns create-topic \
  --name v2x-alertas \
  --region "$AWS_REGION" \
  --query 'TopicArn' \
  --output text)

IMAGE_URI_SERVER=$(grep -oP '(?<="server_image_uri": ")[^"]*' manifest.json)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws ecr get-login-password --region "$AWS_REGION" | \
  sudo docker login --username AWS --password-stdin \
  "$ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com"

sudo docker pull "$IMAGE_URI_SERVER"
sudo docker stop mireposdis 2>/dev/null || true
sudo docker rm mireposdis 2>/dev/null || true

sudo docker run -d --name mireposdis \
  --log-driver=awslogs \
  --log-opt awslogs-region="$AWS_REGION" \
  --log-opt awslogs-group=/v2x/server \
  --log-opt awslogs-stream=server \
  --log-opt awslogs-create-group=true \
  --log-opt mode=non-blocking \
  --log-opt max-buffer-size=4m \
  -p 5000:5000 \
  -e PYTHONUNBUFFERED=1 \
  -e AWS_REGION="$AWS_REGION" \
  -e AWS_DEFAULT_REGION="$AWS_REGION" \
  -e SQS_QUEUE_NAME=v2x-events \
  -e SNS_TOPIC_ARN="$SNS_TOPIC_ARN" \
  -e DYNAMODB_TABLE_NAME=v2x_eventos \
  "$IMAGE_URI_SERVER"

echo "=== Suscripciones activas en SNS ==="
aws sns list-subscriptions-by-topic \
  --topic-arn "$SNS_TOPIC_ARN" \
  --region "$AWS_REGION" \
  --output table

