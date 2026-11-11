"""Codificacion de la telemetria en 3 bytes (24 bits) para redes tipo LoRa/Sigfox.

Distribucion de los 24 bits (de mas significativo a menos significativo):

    [ temperatura : 14 bits ][ humedad : 7 bits ][ direccion_viento : 3 bits ]

  - temperatura: se envia round(temp * 100) -> entero en [0, 11000].
                 2^14 = 16384 >= 11001, por lo que el rango [0, 110.00] cabe
                 exacto conservando los dos decimales.
  - humedad:     entero en [0, 100]. 2^7 = 128 >= 101.
  - viento:      indice categorico en [0, 7]. 2^3 = 8 opciones exactas.
"""

import json

WIND_DIRS = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

TEMP_BITS = 14
HUM_BITS = 7
WIND_BITS = 3

TEMP_MAX = 110.00
HUM_MAX = 100


def encode(data: dict) -> bytes:
    """JSON/dict -> 3 bytes."""
    temp = min(max(float(data["temperatura"]), 0.0), TEMP_MAX)
    hum = min(max(int(data["humedad"]), 0), HUM_MAX)
    wind = WIND_DIRS.index(data["direccion_viento"])

    temp_q = round(temp * 100)  # 0..11000, cabe en 14 bits

    packed = (temp_q << (HUM_BITS + WIND_BITS)) | (hum << WIND_BITS) | wind
    return packed.to_bytes(3, "big")


def decode(payload: bytes) -> dict:
    """3 bytes -> JSON/dict."""
    if len(payload) != 3:
        raise ValueError(f"se esperaban 3 bytes, llegaron {len(payload)}")

    packed = int.from_bytes(payload, "big")

    wind = packed & 0b111
    hum = (packed >> WIND_BITS) & 0b1111111
    temp_q = (packed >> (HUM_BITS + WIND_BITS)) & 0b11111111111111

    return {
        "temperatura": round(temp_q / 100, 2),
        "humedad": hum,
        "direccion_viento": WIND_DIRS[wind],
    }


def to_json_bytes(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


def from_json_bytes(payload: bytes) -> dict:
    return json.loads(payload.decode("utf-8"))


def parse_message(payload: bytes) -> tuple[dict, str]:
    """Detecta automaticamente el formato del mensaje recibido.

    Devuelve (data, formato) donde formato es "json" o "3-bytes".
    """
    if len(payload) == 3:
        return decode(payload), "3-bytes"
    return from_json_bytes(payload), "json"
