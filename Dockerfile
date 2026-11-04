FROM python:3.10-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del servicio
COPY app/ /app/

# Variable de entorno por defecto
EXPOSE 5000


# Comando de inicio del servidor V2X-SERVICE
CMD ["python", "server.py"]
