#!/bin/bash
# Lanza las 3 EC2 (servidor, RSU_1 y RSU_2) y cada una arranca su contenedor sola.
#
# Uso, desde CloudShell y en la carpeta del repositorio:
#   bash deploy/lanzar_todo.sh <id-de-una-instancia-existente>
#
# La instancia existente solo sirve de molde: de ella se copian la imagen del
# sistema (AMI), el tipo, la clave, el grupo de seguridad, la subred y el rol.
set -e

REGION=us-east-1
BUCKET=minuevoprueba1
MOLDE=$1

if [ -z "$MOLDE" ]; then
  echo "Uso: bash deploy/lanzar_todo.sh <id-de-instancia-molde>   (ej: i-0abc123...)"
  exit 1
fi

cd "$(dirname "$0")"

# 1. Subir los scripts a S3 (las instancias los descargan al arrancar)
for rol in servidor rsu_1 rsu_2; do
  aws s3 cp "$rol.sh" "s3://$BUCKET/deploy/$rol.sh" --region $REGION
done

# 2. Copiar los ajustes de la instancia molde
dato() {
  aws ec2 describe-instances --instance-ids "$MOLDE" --region $REGION \
    --query "Reservations[0].Instances[0].$1" --output text
}
AMI=$(dato ImageId)
TIPO=$(dato InstanceType)
CLAVE=$(dato KeyName)
SG=$(dato 'SecurityGroups[].GroupId')
SUBRED=$(dato SubnetId)
PERFIL=$(dato IamInstanceProfile.Arn)
[ "$CLAVE" = "None" ] && CLAVE=""

echo "Molde: $AMI | $TIPO | clave=${CLAVE:-ninguna} | $SG | $SUBRED"
echo "Rol de la instancia: $PERFIL"

# 3. Lanzar una instancia con su rol
lanzar() {  # $1 = rol (servidor, rsu_1, rsu_2)   $2 = nombre en la consola
  sed "s/__ROLE__/$1/" user_data.sh > "/tmp/user_data_$1.sh"
  aws ec2 run-instances --region $REGION \
    --image-id "$AMI" --instance-type "$TIPO" \
    ${CLAVE:+--key-name "$CLAVE"} \
    --security-group-ids $SG --subnet-id "$SUBRED" \
    --iam-instance-profile "Arn=$PERFIL" \
    --associate-public-ip-address \
    --user-data "file:///tmp/user_data_$1.sh" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$2}]" \
    --query 'Instances[0].InstanceId' --output text
}

ID_SERVIDOR=$(lanzar servidor v2x-servidor)
ID_RSU1=$(lanzar rsu_1 v2x-rsu-1)
ID_RSU2=$(lanzar rsu_2 v2x-rsu-2)

echo "Lanzadas: $ID_SERVIDOR $ID_RSU1 $ID_RSU2. Esperando a que estén en marcha..."
aws ec2 wait instance-running --region $REGION --instance-ids $ID_SERVIDOR $ID_RSU1 $ID_RSU2

ip() {
  aws ec2 describe-instances --region $REGION --instance-ids "$1" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
}

echo
echo "=============================================================="
echo "Instancias en marcha. Los contenedores tardan unos 3-5 min más."
echo "Servidor (dashboard):  http://$(ip $ID_SERVIDOR):5000"
echo "Pon esto en config_simulador.txt:"
echo
echo "[RSU_1]"
echo "host = $(ip $ID_RSU1)"
echo "port = 1883"
echo "posicion_m = 1250.0"
echo
echo "[RSU_2]"
echo "host = $(ip $ID_RSU2)"
echo "port = 1883"
echo "posicion_m = 3750.0"
echo "=============================================================="
