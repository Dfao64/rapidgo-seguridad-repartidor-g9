"""Pruebas de validación y del plan Spark real, sin broker compartido."""
import copy
import json
from datetime import datetime
from pathlib import Path

import pytest

from spark.stream_validation import validation_errors


@pytest.fixture
def event():
    return json.loads(Path('samples/evento_repartidor.json').read_text())


@pytest.mark.parametrize('field,value', [
    ('velocidad_kmh', True), ('velocidad_kmh', '12'), ('velocidad_kmh', float('nan')),
    ('velocidad_kmh', float('inf')), ('velocidad_kmh', -1), ('bateria_pct', 101),
    ('bateria_pct', None), ('desvio_metros', -1), ('estado', []), ('estado', 'VOLANDO'),
    ('timestamp', '2026-02-30T14:00:00Z'), ('timestamp', '2026-1-01T14:00:00Z'),
    ('event_id', ' '), ('pedido_id', None), ('gps', {'latitud': 91, 'longitud': 0}),
])
def test_invalid_types_ranges_dates(event, field, value):
    event[field] = value
    assert validation_errors(json.dumps(event), event['repartidor_id'])


@pytest.mark.parametrize('payload', ['{', 'null', '[]', 'true', '1', '"texto"'])
def test_bad_json(payload):
    assert validation_errors(payload, 'REP-009')


def test_key_and_optional_deviation(event):
    assert validation_errors(json.dumps(event), 'otra') == ['clave_no_coincide']
    for deviation in (None, 0, 300):
        event['desvio_metros'] = deviation
        assert validation_errors(json.dumps(event), event['repartidor_id']) == []
    del event['desvio_metros']
    event.update(bateria_pct=0, velocidad_kmh=0, gps={'latitud': -90, 'longitud': 180})
    assert validation_errors(json.dumps(event), event['repartidor_id']) == []
    event['bateria_pct'] = 100
    assert not validation_errors(json.dumps(event), event['repartidor_id'])


@pytest.fixture(scope='module')
def session():
    pytest.importorskip('pyspark')
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[2]').appName('test-kevin')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    yield spark
    spark.stop()


def kafka_row(event, offset=0):
    return ('test', 0, offset, datetime(2026, 10, 30),
            event['repartidor_id'].encode(), json.dumps(event).encode())


def source_schema():
    return 'topic string, partition int, offset long, timestamp timestamp, key binary, value binary'


def test_spark_parsing_and_dedup(session, event):
    from spark.streaming import parse_events, unique_valid
    optional = copy.deepcopy(event)
    optional['event_id'] = 'sin-desvio'
    optional.pop('desvio_metros', None)
    invalid = copy.deepcopy(event)
    invalid['velocidad_kmh'] = True
    rows = [kafka_row(event), kafka_row(event, 1), kafka_row(optional, 2), kafka_row(invalid, 3)]
    parsed = parse_events(session.createDataFrame(rows, source_schema())).cache()
    assert parsed.count() == 4
    bad = parsed.filter('not valido').collect()
    assert len(bad) == 1
    assert 'velocidad_kmh_invalido' in bad[0].motivos
    result = unique_valid(parsed).collect()
    assert len(result) == 2
    missing = next(row for row in result if row.event_id == 'sin-desvio')
    assert not missing.incluye_kpi3 and missing.desvio_metros is None
    assert missing.timestamp_utc is not None
    assert missing.value_original == json.dumps(optional)
    parsed.unpersist()


def test_stream_checkpoint_recovery(session, event, tmp_path):
    from spark.streaming import parse_events, unique_valid
    input_dir = tmp_path / 'input'
    input_dir.mkdir()
    checkpoint = str(tmp_path / 'checkpoint')
    output = str(tmp_path / 'output')

    def put(name, ev, offset):
        row = dict(topic='test', partition=0, offset=offset,
                   timestamp='2026-10-30T00:00:00Z', key=ev['repartidor_id'], value=json.dumps(ev))
        (input_dir / name).write_text(json.dumps(row) + '\n')

    def start():
        source = session.readStream.schema(
            'topic string, partition int, offset long, timestamp timestamp, key string, value string'
        ).json(str(input_dir))
        return (unique_valid(parse_events(source)).writeStream.format('json')
                .option('checkpointLocation', checkpoint).option('path', output).start())

    put('one.json', event, 0)
    query = start()
    try:
        query.processAllAvailable()
        assert query.isActive
    finally:
        query.stop()
    put('duplicate.json', event, 1)
    fresh = copy.deepcopy(event)
    fresh['event_id'] = 'nuevo-tras-reinicio'
    put('new.json', fresh, 2)
    query = start()
    try:
        query.processAllAvailable()
        assert query.isActive
        ids = [r.event_id for r in session.read.json(output).select('event_id').collect()]
        assert sorted(ids) == sorted([event['event_id'], fresh['event_id']])
        assert query.lastProgress['batchId'] >= 1
    finally:
        query.stop()
