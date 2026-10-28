import json
from pathlib import Path

from spark.reglas import es_evento_valido

RUTA = Path(__file__).resolve().parent.parent / "samples" / "evento_repartidor.json"


def cargar_ejemplo():
    with open(RUTA, encoding="utf-8") as f:
        return json.load(f)


def test_ejemplo_es_valido():
    assert es_evento_valido(cargar_ejemplo())


def test_ejemplo_tiene_todos_los_campos():
    esperado = {"event_id", "repartidor_id", "pedido_id", "timestamp", "gps",
                "velocidad_kmh", "bateria_pct", "estado", "desvio_metros"}
    assert set(cargar_ejemplo()) == esperado


def test_falta_pedido_id_es_invalido():
    ev = cargar_ejemplo()
    del ev["pedido_id"]
    assert not es_evento_valido(ev)


def test_bateria_fuera_de_rango_es_invalida():
    ev = cargar_ejemplo()
    ev["bateria_pct"] = 150
    assert not es_evento_valido(ev)


def test_velocidad_negativa_es_invalida():
    ev = cargar_ejemplo()
    ev["velocidad_kmh"] = -10
    assert not es_evento_valido(ev)


def test_latitud_fuera_de_rango_es_invalida():
    ev = cargar_ejemplo()
    ev["gps"]["latitud"] = 95.0
    assert not es_evento_valido(ev)


def test_estado_desconocido_es_invalido():
    ev = cargar_ejemplo()
    ev["estado"] = "VOLANDO"
    assert not es_evento_valido(ev)


def test_timestamp_mal_formado_es_invalido():
    ev = cargar_ejemplo()
    ev["timestamp"] = "22/10/2026 14:30"
    assert not es_evento_valido(ev)


def test_sin_desvio_sigue_siendo_valido():
    ev = cargar_ejemplo()
    ev["desvio_metros"] = None
    assert es_evento_valido(ev)
