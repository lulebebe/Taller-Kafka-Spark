import json
import random
import time

from kafka import KafkaProducer


# ============================================================
# CONFIGURACIÓN
# ============================================================

KAFKA_SERVER = "kafka:9092"
TOPIC = "actividad-topic"

# 5 máquinas, cada una asociada a un sensor
maquinas = {
    "M001": "S001",
    "M002": "S002",
    "M003": "S003",
    "M004": "S004",
    "M005": "S005"
}


# ============================================================
# UMBRALES DE REFERENCIA
# Se utilizan para generar datos normales y anormales.
# El Consumer/Spark será quien determine el estado.
# ============================================================

umbrales = {
    "M001": {
        "temp_max": 30.0,
        "humedad_max": 70.0,
        "presion_min": 1000.0
    },
    "M002": {
        "temp_max": 35.0,
        "humedad_max": 75.0,
        "presion_min": 1000.0
    },
    "M003": {
        "temp_max": 28.0,
        "humedad_max": 65.0,
        "presion_min": 1005.0
    },
    "M004": {
        "temp_max": 32.0,
        "humedad_max": 80.0,
        "presion_min": 995.0
    },
    "M005": {
        "temp_max": 30.0,
        "humedad_max": 70.0,
        "presion_min": 1005.0
    }
}


# ============================================================
# CONEXIÓN CON KAFKA
# ============================================================

try:

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER
    )

    print("Conexión con Kafka exitosa")

except Exception as e:

    print(f"Error conectando con Kafka: {e}")
    exit()


# ============================================================
# GENERACIÓN DE DATOS
# ============================================================

print("Iniciando productor de sensores...")
print("Topic:", TOPIC)

try:

    while True:

        # Seleccionar aleatoriamente una máquina
        maquina_id = random.choice(list(maquinas.keys()))

        sensor_id = maquinas[maquina_id]

        limite = umbrales[maquina_id]


        # ----------------------------------------------------
        # Generar mediciones
        # ----------------------------------------------------

        temperatura = round(
            random.uniform(
                limite["temp_max"] - 8,
                limite["temp_max"] + 8
            ),
            2
        )

        humedad = round(
            random.uniform(
                limite["humedad_max"] - 15,
                limite["humedad_max"] + 15
            ),
            2
        )

        presion = round(
            random.uniform(
                limite["presion_min"] - 15,
                limite["presion_min"] + 15
            ),
            2
        )


        # ----------------------------------------------------
        # Crear evento
        # ----------------------------------------------------

        evento = {

            "sensor_id": sensor_id,

            "maquina_id": maquina_id,

            "timestamp": int(time.time()),

            "temperatura": temperatura,

            "humedad": humedad,

            "presion": presion

        }


        # ----------------------------------------------------
        # Convertir a JSON
        # ----------------------------------------------------

        mensaje = json.dumps(evento)


        # ----------------------------------------------------
        # Enviar a Kafka
        # ----------------------------------------------------

        producer.send(
            TOPIC,
            mensaje.encode("utf-8")
        )

        producer.flush()


        # ----------------------------------------------------
        # Mostrar en pantalla
        # ----------------------------------------------------

        print(
            f"Enviado → "
            f"{maquina_id} | "
            f"T={temperatura}°C | "
            f"H={humedad}% | "
            f"P={presion} hPa"
        )


        # Esperar entre 1 y 3 segundos

        time.sleep(
            random.uniform(1, 3)
        )


except KeyboardInterrupt:

    print("\nProductor detenido.")


finally:

    producer.close()

    print("Conexión con Kafka cerrada.")