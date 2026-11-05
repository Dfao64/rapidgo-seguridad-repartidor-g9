import json
from pathlib import Path

from spark.reglas import es_evento_valido
from spark.stream_validation import validation_errors

RUTA = Path(__file__).resolve().parent.parent / "samples" / "evento_repartidor.json"
NAN, INF = float("nan"), float("inf")


def base():
    with open(RUTA, encoding="utf-8") as f:
        return json.load(f)


def con(**cambios):
    ev = base()
    ev.update(cambios)
    return ev


def sin(campo):
    ev = base()
    ev.pop(campo)
    return ev


def casos():
    return {
        "ejemplo valido": base(),
        "sin desvio": con(desvio_metros=None),
        "bateria 150": con(bateria_pct=150),
        "bateria negativa": con(bateria_pct=-1),
        "velocidad negativa": con(velocidad_kmh=-10),
        "velocidad como texto": con(velocidad_kmh="50"),
        "velocidad booleana": con(velocidad_kmh=True),
        "velocidad NaN": con(velocidad_kmh=NAN),
        "velocidad infinita": con(velocidad_kmh=INF),
        "desvio NaN": con(desvio_metros=NAN),
        "desvio negativo": con(desvio_metros=-5),
        "sin pedido_id": sin("pedido_id"),
        "sin event_id": sin("event_id"),
        "estado desconocido": con(estado="VOLANDO"),
        "timestamp mal formado": con(timestamp="22/10/2026 14:30"),
        "latitud fuera de rango": con(gps={"latitud": 95.0, "longitud": -74.2}),
        "sin gps": sin("gps"),
    }


def test_velocidad_nan_o_infinita_es_invalida():
    for valor in (NAN, INF, -INF):
        assert not es_evento_valido(con(velocidad_kmh=valor)), valor


def test_ambos_validadores_coinciden_en_los_mismos_casos():
    for nombre, ev in casos().items():
        texto = json.dumps(ev)  # igual que viaja por Kafka
        por_reglas = es_evento_valido(json.loads(texto))
        por_stream = validation_errors(texto, ev.get("repartidor_id")) == []
        assert por_reglas == por_stream, nombre
