"""Pruebas aisladas de las funciones del DAG sin necesitar un servidor Airflow."""
import importlib.util
import json
import sys
import types
from pathlib import Path

import pandas as pd
import pytest


class FakeDAG:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False


class FakeOperator:
    def __init__(self, task_id, python_callable):
        self.task_id = task_id
        self.python_callable = python_callable
    def __rshift__(self, other):
        return other


@pytest.fixture
def module(monkeypatch, tmp_path):
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    operators = types.ModuleType("airflow.operators")
    python = types.ModuleType("airflow.operators.python")
    python.PythonOperator = FakeOperator
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.operators", operators)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", python)
    path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "rapidgo_seguridad.py"
    spec = importlib.util.spec_from_file_location("rapidgo_dag_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.ROOT = tmp_path / "datalake"
    mod.REPORTS = tmp_path / "reports"
    (mod.ROOT / "processed").mkdir(parents=True)
    return mod


def test_flujo_calidad_agregacion_reporte(module, monkeypatch):
    file = module.ROOT / "processed" / "parte.parquet"
    file.touch()
    data = pd.DataFrame([
        {"event_id": "e1", "repartidor_id": "r1", "velocidad_kmh": 68.5, "bateria_pct": 12},
        {"event_id": "e2", "repartidor_id": "r1", "velocidad_kmh": 35.5, "bateria_pct": 70},
    ])
    monkeypatch.setattr(pd, "read_parquet", lambda _: data.copy())
    class TI:
        def __init__(self):
            self.values = {}
        def xcom_pull(self, task_ids):
            return self.values[task_ids]
    ti = TI()
    ctx = {"ti": ti, "ds_nodash": "20261105", "ds": "2026-11-05"}
    ti.values["validar_calidad"] = module.validar_calidad(**ctx)
    assert ti.values["validar_calidad"] == {"archivos": 1, "eventos": 2}
    ti.values["agregar_datos"] = module.agregar_datos(**ctx)
    csv = pd.read_csv(ti.values["agregar_datos"]["ruta"])
    assert csv.loc[0, "velocidad_promedio"] == 52
    assert csv.loc[0, "eventos_bateria_critica"] == 1
    assert csv.loc[0, "eventos_exceso_velocidad"] == 1
    report = json.loads(Path(module.generar_reporte(**ctx)).read_text(encoding="utf-8"))
    assert report["eventos"] == 2
    assert report["repartidores"] == 1


def test_sin_parquet_falla_con_mensaje(module):
    with pytest.raises(ValueError, match="No hay Parquet"):
        module.validar_calidad()


def test_datos_invalidos_se_rechazan(module, monkeypatch):
    (module.ROOT / "processed" / "parte.parquet").touch()
    monkeypatch.setattr(pd, "read_parquet", lambda _: pd.DataFrame([
        {"event_id": "e1", "repartidor_id": "r1", "velocidad_kmh": -1, "bateria_pct": 101}
    ]))
    with pytest.raises(ValueError, match="eventos inválidos"):
        module.validar_calidad()
