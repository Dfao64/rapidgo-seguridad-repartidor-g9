"""Pruebas de las salidas Curated del Data Lake, sin Kafka (DataFrames estáticos)."""
import copy
import json
from datetime import datetime
from pathlib import Path

import pytest


@pytest.fixture
def event():
    return json.loads(Path('samples/evento_repartidor.json').read_text())


@pytest.fixture(scope='module')
def session():
    pytest.importorskip('pyspark')
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[2]').appName('test-datalake')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    yield spark
    spark.stop()


def variante(event, event_id, repartidor='REP-009', velocidad=40, bateria=80,
             timestamp='2026-10-22T14:30:00Z'):
    ev = copy.deepcopy(event)
    ev.update(event_id=event_id, repartidor_id=repartidor, velocidad_kmh=velocidad,
              bateria_pct=bateria, timestamp=timestamp)
    return ev


def fila_kafka(ev, offset):
    return ('test', 0, offset, datetime(2026, 10, 30),
            ev['repartidor_id'].encode(), json.dumps(ev).encode())


def parsear(session, eventos):
    from spark.streaming import parse_events
    esquema = ('topic string, partition int, offset long, timestamp timestamp, '
               'key binary, value binary')
    filas = [fila_kafka(ev, i) for i, ev in enumerate(eventos)]
    return parse_events(session.createDataFrame(filas, esquema)).cache()


def test_alertas_por_tipo(session, event):
    from spark.datalake import alertas
    from spark.streaming import unique_valid
    parsed = parsear(session, [
        variante(event, 'normal', velocidad=40, bateria=80),
        variante(event, 'rapido', velocidad=70, bateria=80),
        variante(event, 'bateria', velocidad=40, bateria=10),
        variante(event, 'ambas', velocidad=68.5, bateria=12),
    ])
    filas = alertas(unique_valid(parsed)).collect()
    tipos = {r.event_id: r.tipo_alerta for r in filas}
    assert tipos == {
        'rapido': 'EXCESO_VELOCIDAD',
        'bateria': 'BATERIA_CRITICA',
        'ambas': 'EXCESO_VELOCIDAD,BATERIA_CRITICA',
    }
    parsed.unpersist()


def test_kpi_velocidad_por_ventana(session, event):
    from spark.datalake import kpi_velocidad
    parsed = parsear(session, [
        variante(event, 'a', 'REP-009', velocidad=40, bateria=10,
                 timestamp='2026-10-22T14:30:00Z'),
        variante(event, 'b', 'REP-009', velocidad=60, bateria=80,
                 timestamp='2026-10-22T14:31:00Z'),
        variante(event, 'c', 'REP-010', velocidad=30, bateria=90,
                 timestamp='2026-10-22T14:32:00Z'),
    ])
    filas = {r.repartidor_id: r for r in kpi_velocidad(parsed).collect()}
    assert set(filas) == {'REP-009', 'REP-010'}
    r = filas['REP-009']
    assert r.velocidad_promedio == 50.0
    assert r.total_eventos == 2
    assert r.eventos_bateria_critica == 1
    assert filas['REP-010'].total_eventos == 1
    parsed.unpersist()


def test_kpi_ignora_eventos_invalidos(session, event):
    from spark.datalake import kpi_velocidad
    malo = variante(event, 'malo', velocidad=-5)
    parsed = parsear(session, [variante(event, 'ok', velocidad=40), malo])
    filas = kpi_velocidad(parsed).collect()
    assert len(filas) == 1 and filas[0].total_eventos == 1
    parsed.unpersist()
