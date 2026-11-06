"""RapidGo 360: orquestación batch de datos sintéticos producidos por Spark."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path(os.getenv("RAPIDGO_DATALAKE_ROOT", "/opt/airflow/datalake"))
REPORTS = Path(os.getenv("RAPIDGO_REPORTS_ROOT", "/opt/airflow/reports"))


def validar_calidad(**context):
    """Comprueba datos procesados y devuelve solo metadatos pequeños para XCom."""
    import pandas as pd

    source = ROOT / "processed"
    files = sorted(source.rglob("*.parquet")) if source.exists() else []
    if not files:
        raise ValueError("No hay Parquet en datalake/processed; ejecutar Spark primero")
    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    required = {"event_id", "repartidor_id", "velocidad_kmh", "bateria_pct"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes: {sorted(missing)}")
    if df.empty:
        raise ValueError("No hay eventos procesados")
    invalid = df[list(required)].isna().any(axis=1)
    invalid |= (df["velocidad_kmh"] < 0) | ~df["bateria_pct"].between(0, 100)
    if invalid.any():
        raise ValueError(f"Se detectaron {int(invalid.sum())} eventos inválidos")
    return {"archivos": len(files), "eventos": int(len(df))}


def agregar_datos(**context):
    """Calcula velocidad media y eventos críticos por repartidor."""
    import pandas as pd

    meta = context["ti"].xcom_pull(task_ids="validar_calidad")
    files = sorted((ROOT / "processed").rglob("*.parquet"))
    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    df["bateria_critica_calculada"] = df["bateria_pct"] <= 15
    df["exceso_velocidad_calculado"] = df["velocidad_kmh"] > 60
    summary = (
        df.groupby("repartidor_id", dropna=False)
        .agg(eventos=("event_id", "count"),
             velocidad_promedio=("velocidad_kmh", "mean"),
             eventos_bateria_critica=("bateria_critica_calculada", "sum"),
             eventos_exceso_velocidad=("exceso_velocidad_calculado", "sum"))
        .reset_index()
    )
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / f"agregados_{context['ds_nodash']}.csv"
    summary.to_csv(out, index=False)
    return {"ruta": str(out), "repartidores": int(len(summary)), "eventos": meta["eventos"]}


def generar_reporte(**context):
    """Guarda un resumen JSON legible; no pasa datasets por XCom."""
    meta = context["ti"].xcom_pull(task_ids="agregar_datos")
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / f"reporte_{context['ds_nodash']}.json"
    payload = {"proyecto": "RapidGo 360", "fecha": context["ds"],
               "generado_utc": datetime.now(timezone.utc).isoformat(),
               "origen": "datalake/processed", **meta}
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(out)


with DAG(
    dag_id="rapidgo_seguridad_semanal",
    description="Validación, agregación y reporte del Data Lake de RapidGo",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "daniel", "retries": 1,
                  "retry_delay": timedelta(minutes=1)},
    tags=["rapidgo", "semana3", "seguridad"],
) as dag:
    calidad = PythonOperator(task_id="validar_calidad", python_callable=validar_calidad)
    agregacion = PythonOperator(task_id="agregar_datos", python_callable=agregar_datos)
    reporte = PythonOperator(task_id="generar_reporte", python_callable=generar_reporte)
    calidad >> agregacion >> reporte
