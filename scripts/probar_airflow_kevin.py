"""Prueba de integración con Airflow real y Parquet sintético, sin Kafka.
Ejecutar en entorno aislado con AIRFLOW_HOME nuevo y airflow db migrate previo.
También puede ejecutarse dentro del servicio Docker Airflow (ver guía).
"""
import importlib.util
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from airflow.utils.session import create_session
from airflow.models.xcom import XCom

project = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('rapidgo_integration', project / 'airflow/dags/rapidgo_seguridad.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
evidence = Path(os.environ.get('RAPIDGO_EVIDENCE_ROOT', project / 'docs/evidencias/semana3-kevin'))
evidence.mkdir(parents=True, exist_ok=True)
with TemporaryDirectory(prefix='rapidgo-calidad-') as tmp:
    mod.ROOT = Path(tmp) / 'datalake'
    mod.REPORTS = Path(tmp) / 'reports'
    source = mod.ROOT / 'processed'
    source.mkdir(parents=True)
    event = json.loads((project / 'samples/evento_repartidor.json').read_text())
    pd.DataFrame([event, {**event, 'event_id': 'invalido', 'bateria_pct': 101}]).to_parquet(source / 'parte.parquet', index=False)
    run = mod.dag.test(run_conf={'simular_fallo_temporal': True})
    with create_session() as session:
        tasks = run.get_task_instances(session=session)
        states = {ti.task_id: {'state': ti.state, 'try_number': ti.try_number} for ti in tasks}
        assert len(tasks) == 3 and all(ti.state == 'success' for ti in tasks), states
        quality = next(ti for ti in tasks if ti.task_id == 'validar_calidad')
        assert quality.try_number >= 2, states
        metadata = {ti.task_id: XCom.get_one(run_id=run.run_id, dag_id=run.dag_id,
                    task_id=ti.task_id, key='return_value', session=session) for ti in tasks}
        assert metadata['validar_calidad']['invalidos'] == 1
        assert metadata['agregar_datos']['eventos'] == 1
        for name, value in metadata.items():
            assert len(json.dumps(value)) < 2000, name
        result = {'dag_id': run.dag_id, 'run_id': run.run_id, 'state': run.state,
                  'modo': 'DAG.test con Airflow real; no Docker ni scheduler',
                  'tasks': states, 'xcom': metadata}
        (evidence / 'airflow-integracion.json').write_text(json.dumps(result, indent=2))
        report = Path(metadata['generar_reporte']['ruta'])
        (evidence / 'reporte-ejemplo.json').write_text(report.read_text())
        rejects = Path(metadata['validar_calidad']['ruta_rechazos'])
        (evidence / 'rechazados-ejemplo.jsonl').write_text(rejects.read_text())
        print(json.dumps(result, indent=2))
