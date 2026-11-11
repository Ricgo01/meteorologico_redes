"""Nodo de sensores de la estacion meteorologica (Kafka Producer).

Genera telemetria simulada (temperatura, humedad relativa y direccion del
viento) y la publica periodicamente en el Kafka Broker del Edge Server.

Uso:
    python producer.py --topic 231234                  # envia JSON
    python producer.py --topic 231234 --mode raw       # envia 3 bytes (LoRa)
    python producer.py --topic 231234 --min 2 --max 4  # intervalo corto, pruebas
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

import codec
import config

# Los sensores muestrean de una distribucion normal centrada en 55.
MEDIA = 55.0
DESVIACION = 15.0


def leer_temperatura() -> float:
    """Termometro: [0, 110.00] grados C, float de dos decimales."""
    valor = random.gauss(MEDIA, DESVIACION)
    valor = min(max(valor, 0.0), 110.0)
    return round(valor, 2)


def leer_humedad() -> int:
    """Higrometro: [0, 100] %, entero."""
    valor = random.gauss(MEDIA, DESVIACION)
    valor = min(max(valor, 0.0), 100.0)
    return int(round(valor))


def leer_viento() -> str:
    """Veleta: una de las 8 direcciones, uniforme."""
    return random.choice(codec.WIND_DIRS)


def generar_data() -> dict:
    return {
        "temperatura": leer_temperatura(),
        "humedad": leer_humedad(),
        "direccion_viento": leer_viento(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Productor IoT - estacion meteorologica")
    parser.add_argument("--topic", default=config.TOPIC, help="topic de Kafka (carne)")
    parser.add_argument("--mode", choices=["json", "raw"], default="json",
                        help="json = payload JSON; raw = payload codificado en 3 bytes")
    parser.add_argument("--min", type=float, default=15.0, dest="min_s",
                        help="segundos minimos entre envios (default 15)")
    parser.add_argument("--max", type=float, default=30.0, dest="max_s",
                        help="segundos maximos entre envios (default 30)")
    args = parser.parse_args()

    if args.topic == "CAMBIAR_CARNE":
        print("ERROR: defina el topic con --topic <carne> o editelo en config.py")
        return 1

    print(f"Conectando al broker {config.BOOTSTRAP_SERVERS} ...")
    producer = KafkaProducer(
        **config.kafka_auth(),
        acks="all",
    )
    print(f"Conectado. Topic: '{args.topic}' | key: '{config.KEY}' | modo: {args.mode}")
    print(f"Enviando cada {args.min_s:.0f}-{args.max_s:.0f} s. Ctrl-C para detener.\n")

    def detener(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, detener)

    enviados = 0
    try:
        while True:
            data = generar_data()

            if args.mode == "raw":
                payload = codec.encode(data)
                descripcion = f"{payload.hex()}  ({len(payload)} bytes)"
            else:
                payload = codec.to_json_bytes(data)
                descripcion = f"{payload.decode()}  ({len(payload)} bytes)"

            future = producer.send(args.topic, key=config.KEY.encode("utf-8"), value=payload)
            meta = future.get(timeout=10)
            enviados += 1

            hora = datetime.now().strftime("%H:%M:%S")
            print(f"[{hora}] #{enviados:03d} -> particion {meta.partition}, offset {meta.offset} | {descripcion}")
            if args.mode == "raw":
                print(f"           data: {json.dumps(data)}")

            time.sleep(random.uniform(args.min_s, args.max_s))

    except KeyboardInterrupt:
        print(f"\nDetenido por el usuario. Mensajes enviados: {enviados}")
    finally:
        producer.flush()
        producer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
