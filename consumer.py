"""Consumidor de telemetria de la estacion meteorologica (Kafka Consumer).

Se suscribe al topic de la pareja, decodifica los mensajes (JSON o el payload
restringido de 3 bytes, detectado automaticamente) y grafica la telemetria en
vivo, actualizando las graficas cada vez que entra un dato nuevo.

Uso:
    python consumer.py --topic 231234
    python consumer.py --topic 231234 --desde-inicio   # reprocesa el historial
"""

import argparse
import sys
from collections import Counter
from datetime import datetime

import matplotlib

matplotlib.use("QtAgg")  # backend interactivo (en este equipo no hay tkinter)

import matplotlib.pyplot as plt
from kafka import KafkaConsumer

import codec
import config

all_temp: list[float] = []
all_hume: list[int] = []
all_wind: list[str] = []
all_time: list[datetime] = []


def crear_figura(topic: str):
    fig, (ax_temp, ax_hume, ax_wind) = plt.subplots(3, 1, figsize=(11, 9))
    fig.canvas.manager.set_window_title(f"Estacion Meteorologica - topic {topic}")
    fig.suptitle(f"Telemetria en vivo - topic '{topic}'", fontsize=14, fontweight="bold")
    fig.subplots_adjust(hspace=0.55, top=0.90, bottom=0.08)
    return fig, ax_temp, ax_hume, ax_wind


def plot_all_data(fig, ax_temp, ax_hume, ax_wind, topic: str, formato: str):
    """Redibuja las tres graficas con todo el historico recibido."""
    ax_temp.clear()
    ax_hume.clear()
    ax_wind.clear()

    n = len(all_temp)
    etiquetas = [t.strftime("%H:%M:%S") for t in all_time]

    # --- Temperatura -----------------------------------------------------
    ax_temp.plot(etiquetas, all_temp, marker="o", color="#d62728", linewidth=1.8)
    ax_temp.set_title(f"Temperatura (°C)   -   ultima: {all_temp[-1]:.2f} °C" if n else "Temperatura (°C)")
    ax_temp.set_ylabel("°C")
    ax_temp.set_ylim(0, 110)
    ax_temp.grid(alpha=0.3)

    # --- Humedad relativa ------------------------------------------------
    ax_hume.plot(etiquetas, all_hume, marker="s", color="#1f77b4", linewidth=1.8)
    ax_hume.set_title(f"Humedad relativa (%)   -   ultima: {all_hume[-1]} %" if n else "Humedad relativa (%)")
    ax_hume.set_ylabel("%")
    ax_hume.set_ylim(0, 100)
    ax_hume.grid(alpha=0.3)

    for ax in (ax_temp, ax_hume):
        if n > 12:  # no saturar el eje x
            paso = max(1, n // 12)
            ax.set_xticks(range(0, n, paso))
            ax.set_xticklabels(etiquetas[::paso], rotation=45, ha="right", fontsize=8)
        else:
            ax.tick_params(axis="x", rotation=45, labelsize=8)

    # --- Direccion del viento --------------------------------------------
    conteo = Counter(all_wind)
    frecuencias = [conteo.get(d, 0) for d in codec.WIND_DIRS]
    barras = ax_wind.bar(codec.WIND_DIRS, frecuencias, color="#2ca02c")
    ultima = all_wind[-1] if n else "-"
    for direccion, barra in zip(codec.WIND_DIRS, barras):
        if direccion == ultima:
            barra.set_color("#ff7f0e")  # resalta la ultima lectura
    ax_wind.set_title(f"Direccion del viento (frecuencia)   -   ultima: {ultima}")
    ax_wind.set_ylabel("lecturas")
    ax_wind.grid(alpha=0.3, axis="y")
    if frecuencias:
        ax_wind.set_ylim(0, max(frecuencias) + 1)

    fig.suptitle(
        f"Telemetria en vivo - topic '{topic}' - {n} mensajes recibidos (formato: {formato})",
        fontsize=14,
        fontweight="bold",
    )

    fig.canvas.draw_idle()
    plt.pause(0.01)


def main() -> int:
    parser = argparse.ArgumentParser(description="Consumidor IoT - estacion meteorologica")
    parser.add_argument("--topic", default=config.TOPIC, help="topic de Kafka (carne)")
    parser.add_argument("--group", default="estacion-meteorologica", help="group_id del consumidor")
    parser.add_argument("--desde-inicio", action="store_true",
                        help="leer el topic desde el primer mensaje disponible")
    parser.add_argument("--save", metavar="ARCHIVO.png", default=None,
                        help="guardar la grafica al salir con Ctrl-C")
    args = parser.parse_args()

    if args.topic == "CAMBIAR_CARNE":
        print("ERROR: defina el topic con --topic <carne> o editelo en config.py")
        return 1

    print(f"Conectando al broker {config.BOOTSTRAP_SERVERS} ...")
    consumer = KafkaConsumer(
        args.topic,
        **config.kafka_auth(),
        group_id=args.group,
        auto_offset_reset="earliest" if args.desde_inicio else "latest",
        enable_auto_commit=True,
    )
    print(f"Suscrito al topic '{args.topic}' (group_id: {args.group}).")
    print("Esperando mensajes... Ctrl-C para detener.\n")

    plt.ion()
    fig, ax_temp, ax_hume, ax_wind = crear_figura(args.topic)
    plt.show(block=False)

    formato = "-"
    try:
        while True:
            # poll() en lugar de for-in para poder refrescar la ventana
            # aunque no esten entrando mensajes.
            registros = consumer.poll(timeout_ms=500)

            for _, mensajes in registros.items():
                for mensaje in mensajes:
                    payload, formato = codec.parse_message(mensaje.value)

                    all_temp.append(payload["temperatura"])
                    all_hume.append(payload["humedad"])
                    all_wind.append(payload["direccion_viento"])
                    all_time.append(datetime.fromtimestamp(mensaje.timestamp / 1000))

                    hora = all_time[-1].strftime("%H:%M:%S")
                    print(f"[{hora}] particion {mensaje.partition}, offset {mensaje.offset}, "
                          f"key={mensaje.key.decode() if mensaje.key else None}, formato={formato}")
                    print(f"           temp={payload['temperatura']:6.2f} °C | "
                          f"humedad={payload['humedad']:3d} % | viento={payload['direccion_viento']}")

                    plot_all_data(fig, ax_temp, ax_hume, ax_wind, args.topic, formato)

            plt.pause(0.05)  # mantiene la ventana viva entre mensajes

            if not plt.fignum_exists(fig.number):
                print("\nVentana cerrada.")
                break

    except KeyboardInterrupt:
        print(f"\nDetenido por el usuario. Mensajes recibidos: {len(all_temp)}")
    finally:
        if args.save and all_temp:
            fig.savefig(args.save, dpi=150, bbox_inches="tight")
            print(f"Grafica guardada en {args.save}")
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
