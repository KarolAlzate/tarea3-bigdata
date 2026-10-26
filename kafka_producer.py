# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD) - Productor Kafka (Anexo 3 adaptado al problema de la TRM)
Simula operaciones de compra y venta de dólares en el mercado cambiario colombiano
y las envía al topic 'trm_operaciones' una vez por segundo.

Ejecución:  python3 kafka_producer.py            (precio base 3312.84 COP)
            python3 kafka_producer.py 3400       (otro precio base)
Detener:    Ctrl+C
"""
import json
import random
import sys
import time

from kafka import KafkaProducer

TOPIC = "trm_operaciones"
BASE = float(sys.argv[1]) if len(sys.argv) > 1 else 3312.84     # última TRM conocida
precio = BASE
ENTIDADES = ["Banco_A", "Banco_B", "Banco_C", "Banco_D", "Comisionista_E"]
contador = 0


def generar_operacion():
    """Caminata aleatoria del precio con saltos ocasionales (noticias del mercado)."""
    global precio, contador
    contador += 1
    cambio = random.gauss(0, 0.0008)          # movimiento normal ~0,08 %
    cambio += 0.05 * (BASE - precio) / precio  # el precio tiende a volver a la TRM base
    if random.random() < 0.03:                 # 3 % de probabilidad de salto brusco
        cambio += random.choice([-1, 1]) * random.uniform(0.008, 0.015)
    precio = round(precio * (1 + cambio), 2)
    return {
        "operacion_id": contador,
        "entidad": random.choice(ENTIDADES),
        "tipo": random.choice(["compra", "venta"]),
        "precio": precio,
        "monto_usd": round(random.uniform(50_000, 2_000_000), 2),
        "timestamp": int(time.time()),
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

print(f"Enviando operaciones al topic '{TOPIC}' (Ctrl+C para detener)...")
try:
    while True:
        dato = generar_operacion()
        producer.send(TOPIC, value=dato)
        print(f"Enviado: {dato}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nProductor detenido.")
finally:
    producer.flush()
    producer.close()
