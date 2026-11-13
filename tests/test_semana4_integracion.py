"""Semana 4: pruebas deterministas del contrato Producer -> KPIs y alertas.

Estas pruebas cubren componentes Python reales, pero NO sustituyen una prueba
end-to-end con Kafka, Spark Structured Streaming, Parquet y Airflow activos.
"""
from copy import deepcopy

import pytest

from producer.main import config, events, encode
from spark.indicadores import calcular_indicadores
from spark.reglas import detectar_alertas, es_evento_valido
from spark.stream_validation import validation_errors


def _event(event_id, speed, battery, deviation, rider="REP-001"):
    return {
        "event_id": event_id, "repartidor_id": rider, "pedido_id": "PED-001",
        "timestamp": "2026-11-01T12:00:00Z",
        "gps": {"latitud": 4.732, "longitud": -74.264},
        "velocidad_kmh": speed, "bateria_pct": battery,
        "estado": "EN_RUTA", "desvio_metros": deviation,
    }


def test_productor_genera_eventos_validos_compatibles_con_streaming():
    args = config(["--dry-run", "--riders", "2", "--rounds", "3",
                   "--seed", "42", "--run-id", "semana4-e2e",
                   "--start-time", "2026-11-01T12:00:00Z"])
    generated = list(events(args))
    assert len(generated) == 6
    assert len({event["event_id"] for event in generated}) == 6
    for event in generated:
        assert es_evento_valido(event)
        assert validation_errors(encode(event).decode("utf-8"),
                                 event["repartidor_id"]) == []


def test_repetibilidad_productor_y_claves():
    argv = ["--dry-run", "--riders", "2", "--rounds", "2", "--seed", "7",
            "--run-id", "semana4", "--start-time", "2026-11-01T12:00:00Z"]
    assert list(events(config(argv))) == list(events(config(argv)))


def test_tres_kpis_y_dos_alertas_con_datos_controlados():
    data = [
        _event("evt-1", 68.5, 12, 180),
        _event("evt-2", 31.5, 80, 450),
        _event("evt-3", 60, 15, None),
        _event("evt-4", 40, 60, 300),
    ]
    result = calcular_indicadores(data)
    rider = result["por_repartidor"]["REP-001"]
    assert result["eventos_validos"] == 4
    assert result["eventos_descartados"] == 0
    assert rider["velocidad_promedio"] == 50.0
    assert rider["eventos_bateria_critica"] == 2
    assert rider["pct_bateria_critica"] == 50.0
    assert rider["eventos_desvio_critico"] == 1
    assert rider["pct_desvio_critico"] == 33.33
    assert rider["eventos_exceso_velocidad"] == 1
    assert detectar_alertas(data[0]) == ["EXCESO_VELOCIDAD", "BATERIA_CRITICA"]
    assert detectar_alertas(data[1]) == []
    assert detectar_alertas(data[2]) == ["BATERIA_CRITICA"]
    assert detectar_alertas(data[3]) == []


@pytest.mark.parametrize("field,value", [
    ("velocidad_kmh", -1), ("bateria_pct", 101),
    ("desvio_metros", -2), ("estado", "INVALIDO"),
])
def test_eventos_invalidos_no_generan_alertas_ni_kpis(field, value):
    valid = _event("evt-valido", 40, 70, 100)
    invalid = deepcopy(valid)
    invalid["event_id"] = "evt-invalido"
    invalid[field] = value
    assert not es_evento_valido(invalid)
    assert detectar_alertas(invalid) == []
    result = calcular_indicadores([valid, invalid])
    assert result["eventos_validos"] == 1
    assert result["eventos_descartados"] == 1
