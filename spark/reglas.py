"""Reglas de negocio y validación del evento de seguridad del repartidor.

"""
from datetime import datetime

UMBRAL_VELOCIDAD_KMH = 60
UMBRAL_BATERIA_PCT = 15
UMBRAL_DESVIO_M = 300

ESTADOS_VALIDOS = {"EN_RUTA", "ENTREGADO", "PAUSADO"}
CAMPOS_OBLIGATORIOS = (
    "event_id", "repartidor_id", "pedido_id", "timestamp",
    "gps", "velocidad_kmh", "bateria_pct", "estado",
)


def es_exceso_velocidad(velocidad_kmh):
    return velocidad_kmh > UMBRAL_VELOCIDAD_KMH


def es_bateria_critica(bateria_pct):
    return bateria_pct <= UMBRAL_BATERIA_PCT


def es_desvio_critico(desvio_metros):
    return desvio_metros is not None and desvio_metros > UMBRAL_DESVIO_M


def _es_numero(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _timestamp_valido(ts):
    try:
        datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        return True
    except (TypeError, ValueError):
        return False


def es_evento_valido(ev):
    """Aplica las reglas de docs/contrato-eventos.md."""
    if not isinstance(ev, dict):
        return False
    for campo in CAMPOS_OBLIGATORIOS:
        if ev.get(campo) is None:
            return False
    if not all(isinstance(ev[c], str) and ev[c] for c in
               ("event_id", "repartidor_id", "pedido_id")):
        return False
    if not _timestamp_valido(ev["timestamp"]):
        return False
    gps = ev["gps"]
    if not isinstance(gps, dict):
        return False
    lat, lon = gps.get("latitud"), gps.get("longitud")
    if not (_es_numero(lat) and _es_numero(lon)):
        return False
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False
    if not _es_numero(ev["velocidad_kmh"]) or ev["velocidad_kmh"] < 0:
        return False
    if not _es_numero(ev["bateria_pct"]) or not 0 <= ev["bateria_pct"] <= 100:
        return False
    if ev["estado"] not in ESTADOS_VALIDOS:
        return False
    desvio = ev.get("desvio_metros")
    if desvio is not None and (not _es_numero(desvio) or desvio < 0):
        return False
    return True


def detectar_alertas(ev):
    """Devuelve la lista de alertas que dispara un evento válido."""
    if not es_evento_valido(ev):
        return []
    alertas = []
    if es_exceso_velocidad(ev["velocidad_kmh"]):
        alertas.append("EXCESO_VELOCIDAD")
    if es_bateria_critica(ev["bateria_pct"]):
        alertas.append("BATERIA_CRITICA")
    return alertas
