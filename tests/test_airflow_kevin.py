"""Contrato de calidad y fallos; I/O Parquet real, sin servidor Airflow."""
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from test_airflow_daniel import module


def event(**changes):
    return {"event_id": "e1", "repartidor_id": "r1", "pedido_id": "p1",
            "timestamp": "2026-11-05T12:00:00Z", "gps": {"latitud": 4, "longitud": -74},
            "velocidad_kmh": 20., "bateria_pct": 80., "estado": "EN_RUTA", **changes}


@pytest.mark.parametrize('changes,reason', [
    ({"event_id": " "}, "event_id"), ({"pedido_id": None}, "pedido_id"),
    ({"repartidor_id": ""}, "repartidor_id"),
    ({"timestamp": "2026-02-30T00:00:00Z"}, "timestamp"),
    ({"timestamp": "2026-11-05"}, "timestamp"),
    ({"timestamp": "2026-11-05T12:00:00+00:60"}, "timestamp"),
    ({"gps": {"latitud": 91, "longitud": 0}}, "latitud"),
    ({"gps": {"latitud": 0, "longitud": -181}}, "longitud"),
    ({"gps": None}, "gps"), ({"velocidad_kmh": -1}, "velocidad"),
    ({"velocidad_kmh": float('inf')}, "velocidad"),
    ({"velocidad_kmh": True}, "velocidad"),
    ({"bateria_pct": 101}, "bateria"), ({"bateria_pct": float('nan')}, "bateria"),
    ({"estado": "DESCONOCIDO"}, "estado")])
def test_reglas(module, changes, reason):
    assert any(reason in r for r in module.motivos_evento(event(**changes)))


@pytest.mark.parametrize('timestamp', ['2026-11-05T12:00:00Z', '2026-11-05T12:00:00.123-05:00'])
def test_iso_y_limites(module, timestamp):
    assert module.motivos_evento(event(timestamp=timestamp, gps={"latitud": -90, "longitud": 180}, velocidad_kmh=0, bateria_pct=100)) == []


def test_rechazos_no_alimentan_indicadores(module):
    df = pd.DataFrame([event(), event(event_id='malo', velocidad_kmh=-100),
                       event(event_id='dup'), event(event_id='dup', velocidad_kmh=100)])
    df.to_parquet(module.ROOT / 'processed' / 'parte.parquet', index=False)
    values = {}
    ctx = dict(ti=SimpleNamespace(xcom_pull=lambda task_ids: values[task_ids]), run_id='prueba', ds='2026-11-05')
    values['validar_calidad'] = module.validar_calidad(**ctx)
    meta = values['validar_calidad']
    assert (meta['procesados'], meta['invalidos'], meta['eventos']) == (4, 3, 1)
    assert len(json.dumps(meta)) < 2000
    values['agregar_datos'] = module.agregar_datos(**ctx)
    result = pd.read_csv(values['agregar_datos']['ruta'])
    assert result.iloc[0]['eventos'] == 1
    assert result.iloc[0]['velocidad_promedio'] == 20
    rejects = pd.read_json(meta['ruta_rechazos'], lines=True)
    assert len(rejects) == 3
    assert rejects.motivos.str.contains('duplicado').sum() == 2
    assert module.generar_reporte(**ctx)['eventos'] == 1


def test_columnas_por_archivo(module):
    pd.DataFrame([event()]).to_parquet(module.ROOT / 'processed' / 'a.parquet')
    pd.DataFrame([{'event_id': 'e2'}]).to_parquet(module.ROOT / 'processed' / 'b.parquet')
    with pytest.raises(ValueError, match='Columnas faltantes.*b.parquet'):
        module.validar_calidad()


def test_error_lectura(module, caplog):
    (module.ROOT / 'processed' / 'roto.parquet').write_text('corrupto')
    with pytest.raises(OSError, match='Error de lectura'):
        module.validar_calidad()
    assert 'Error de lectura Parquet' in caplog.text


def test_vacio(module):
    pd.DataFrame(columns=list(module.REQUIRED)).to_parquet(module.ROOT / 'processed' / 'vacio.parquet')
    with pytest.raises(ValueError, match='No hay eventos'):
        module.validar_calidad()


def test_fallo_temporal_solo_primer_intento(module):
    pd.DataFrame([event()]).to_parquet(module.ROOT / 'processed' / 'datos.parquet')
    ctx = dict(dag_run=SimpleNamespace(conf={'simular_fallo_temporal': True}), ti=SimpleNamespace(try_number=1))
    with pytest.raises(OSError, match='temporal controlado'):
        module.validar_calidad(**ctx)
    ctx['ti'].try_number = 2
    assert module.validar_calidad(**ctx)['eventos'] == 1


def test_agregacion_error_y_xcom(module, caplog):
    with pytest.raises(ValueError, match='metadatos XCom'):
        module.agregar_datos(ti=SimpleNamespace(xcom_pull=lambda **_: None))
    with pytest.raises(FileNotFoundError):
        module.agregar_datos(ti=SimpleNamespace(xcom_pull=lambda **_: {'ruta_validos': '/no/existe', 'eventos': 1}))
    assert 'Error durante agregación' in caplog.text


def test_politica_reintentos(module):
    args = module.dag.kwargs['default_args']
    assert args['retries'] == 2
    assert args['retry_delay'].total_seconds() == 30
    assert args['execution_timeout'].total_seconds() == 300
    assert args['on_failure_callback'] is module.fallo_definitivo
