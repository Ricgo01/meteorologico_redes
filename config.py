"""Parametros de conexion al Kafka Broker del laboratorio (Edge Server).

Las credenciales SASL no viven aqui: se leen de 'credenciales.env', que esta
fuera del repositorio. Copiar 'credenciales.env.example' como
'credenciales.env' y llenarlo con los valores del enunciado.
"""

import os
from pathlib import Path

BOOTSTRAP_SERVERS = "161.35.11.141:9092"
SECURITY_PROTOCOL = "SASL_PLAINTEXT"
SASL_MECHANISM = "PLAIN"

# Topic de la pareja. Cada integrante tiene el suyo, con su carne:
#   23247 -> nodo/estacion de un integrante
#   23201 -> nodo/estacion del otro integrante
TOPIC = "23247"
TOPIC_PAREJA = "23201"

KEY = "sensor1"

_ARCHIVO_CREDENCIALES = Path(__file__).with_name("credenciales.env")


def _cargar_credenciales():
    """Lee credenciales.env y lo vuelca al ambiente (sin pisar lo ya definido)."""
    if not _ARCHIVO_CREDENCIALES.exists():
        return
    for linea in _ARCHIVO_CREDENCIALES.read_text().splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        os.environ.setdefault(clave.strip(), valor.strip())


def kafka_auth():
    """Kwargs de autenticacion comunes para Producer y Consumer."""
    _cargar_credenciales()
    usuario = os.environ.get("KAFKA_USERNAME")
    password = os.environ.get("KAFKA_PASSWORD")

    if not usuario or not password:
        raise SystemExit(
            "Faltan las credenciales del broker.\n"
            "Copie 'credenciales.env.example' como 'credenciales.env' y llenelo "
            "con el usuario y contrasena del enunciado."
        )

    return {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": usuario,
        "sasl_plain_password": password,
    }
