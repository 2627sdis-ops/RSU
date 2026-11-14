#!/bin/bash
# Datos de usuario de la EC2: se ejecuta como root en el primer arranque.
# __ROLE__ lo sustituye lanzar_todo.sh por: servidor, rsu_1 o rsu_2.
ROLE=__ROLE__
export AWS_DEFAULT_REGION=us-east-1

# 1. Docker (solo si la imagen del sistema no lo trae)
if ! command -v docker >/dev/null 2>&1; then
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y docker
  else
    apt-get update && apt-get install -y docker.io awscli
  fi
fi
systemctl enable --now docker

# 2. Guardar el rol y el script que se ejecuta en CADA arranque.
#    Así, si la instancia se para y se vuelve a arrancar, relanza su contenedor
#    con la última versión del manifiesto (y la RSU se suscribe con su IP nueva).
echo "$ROLE" > /etc/v2x-role
mkdir -p /var/lib/cloud/scripts/per-boot
cat > /var/lib/cloud/scripts/per-boot/v2x.sh <<'EOF'
#!/bin/bash
export AWS_DEFAULT_REGION=us-east-1
ROLE=$(cat /etc/v2x-role)
for intento in 1 2 3; do
  aws s3 cp "s3://minuevoprueba1/deploy/$ROLE.sh" "/tmp/v2x-$ROLE.sh" || { sleep 10; continue; }
  # Se quita el 'docker logs -f' final del script: bloquearía el arranque.
  sed -i '/docker logs -f/d' "/tmp/v2x-$ROLE.sh"
  bash "/tmp/v2x-$ROLE.sh" && break
  sleep 10
done
EOF
chmod +x /var/lib/cloud/scripts/per-boot/v2x.sh

# 3. En el primer arranque los scripts per-boot ya se han recorrido antes que
#    este, así que se lanza a mano esta vez.
/var/lib/cloud/scripts/per-boot/v2x.sh
