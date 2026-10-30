"""Validación estricta del JSON original, antes de las conversiones Spark."""
import json
import math
import re
from datetime import datetime


def validation_errors(value, key):
    """Motivos estables para auditoría; nunca convierte strings o bool a números."""
    try:
        ev = json.loads(value)
    except (ValueError, TypeError):
        return ["json_malformado"]
    if not isinstance(ev, dict):
        return ["json_no_objeto"]
    errors = []
    for field in ("event_id", "repartidor_id", "pedido_id"):
        if not isinstance(ev.get(field), str) or not ev[field].strip():
            errors.append(field + "_invalido")
    ts = ev.get("timestamp")
    try:
        if not isinstance(ts, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", ts):
            raise ValueError()
        datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        errors.append("timestamp_utc_invalido")
    gps = ev.get("gps")
    if not isinstance(gps, dict):
        gps = {}
    fields = [("gps.latitud", gps.get("latitud"), -90, 90),
              ("gps.longitud", gps.get("longitud"), -180, 180),
              ("velocidad_kmh", ev.get("velocidad_kmh"), 0, None),
              ("bateria_pct", ev.get("bateria_pct"), 0, 100)]
    if ev.get("desvio_metros") is not None:
        fields.append(("desvio_metros", ev["desvio_metros"], 0, None))
    for name, number, low, high in fields:
        try:
            valid = (type(number) in (int, float) and math.isfinite(number)
                     and number >= low and (high is None or number <= high))
        except (OverflowError, TypeError):
            valid = False
        if not valid:
            errors.append(name + "_invalido")
    if not isinstance(ev.get("estado"), str) or ev["estado"] not in {"EN_RUTA", "ENTREGADO", "PAUSADO"}:
        errors.append("estado_invalido")
    if key is None or key != ev.get("repartidor_id"):
        errors.append("clave_no_coincide")
    return errors
