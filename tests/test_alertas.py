from spark.reglas import (
    detectar_alertas, es_bateria_critica, es_desvio_critico, es_exceso_velocidad,
)


def evento(velocidad=40, bateria=80, desvio=100):
    return {
        "event_id": "evt-1", "repartidor_id": "REP-001", "pedido_id": "PED-1",
        "timestamp": "2026-10-22T14:30:00Z",
        "gps": {"latitud": 4.73, "longitud": -74.26},
        "velocidad_kmh": velocidad, "bateria_pct": bateria,
        "estado": "EN_RUTA", "desvio_metros": desvio,
    }


def test_velocidad_borde():
    assert not es_exceso_velocidad(60)
    assert es_exceso_velocidad(60.1)


def test_bateria_borde():
    assert es_bateria_critica(15)
    assert not es_bateria_critica(16)


def test_desvio_borde():
    assert not es_desvio_critico(300)
    assert es_desvio_critico(301)
    assert not es_desvio_critico(None)


def test_evento_normal_no_alerta():
    assert detectar_alertas(evento()) == []


def test_exceso_de_velocidad_alerta():
    assert detectar_alertas(evento(velocidad=68.5)) == ["EXCESO_VELOCIDAD"]


def test_bateria_critica_alerta():
    assert detectar_alertas(evento(bateria=12)) == ["BATERIA_CRITICA"]


def test_dos_alertas_a_la_vez():
    assert detectar_alertas(evento(velocidad=70, bateria=10)) == [
        "EXCESO_VELOCIDAD", "BATERIA_CRITICA"]


def test_evento_invalido_no_genera_alertas():
    assert detectar_alertas(evento(velocidad=-5, bateria=150)) == []
