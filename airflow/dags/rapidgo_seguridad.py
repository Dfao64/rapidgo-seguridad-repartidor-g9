"""Validación, snapshot aislado y metadatos pequeños para RapidGo 360."""
from __future__ import annotations

import hashlib
import json
import logging
import math
import numbers
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path(os.getenv("RAPIDGO_DATALAKE_ROOT", "/opt/airflow/datalake"))
REPORTS = Path(os.getenv("RAPIDGO_REPORTS_ROOT", "/opt/airflow/reports"))
LOG = logging.getLogger(__name__)
REQUIRED = {"event_id", "repartidor_id", "pedido_id", "timestamp", "gps",
            "velocidad_kmh", "bateria_pct", "estado"}


def _fail(message):
    LOG.error(message)
    raise ValueError(message)


def _directory(context):
    identity = str(context.get("run_id", context.get("ds_nodash", "manual")))
    directory = REPORTS / hashlib.sha256(identity.encode()).hexdigest()[:24]
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _number(value, low, high=math.inf):
    return (isinstance(value, numbers.Real) and not isinstance(value, bool)
            and math.isfinite(value) and low <= value <= high)


def motivos_evento(row):
    errors = []
    for field in ("event_id", "repartidor_id", "pedido_id"):
        if not isinstance(row.get(field), str) or not row[field].strip():
            errors.append(field + "_invalido")
    ts = row.get("timestamp")
    try:
        if not isinstance(ts, str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)", ts):
            raise ValueError()
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        errors.append("timestamp_iso8601_invalido")
    gps = row.get("gps")
    gps = gps if isinstance(gps, dict) else {}
    for field, value, low, high in (
        ("gps.latitud", gps.get("latitud"), -90, 90),
        ("gps.longitud", gps.get("longitud"), -180, 180),
        ("velocidad_kmh", row.get("velocidad_kmh"), 0, math.inf),
        ("bateria_pct", row.get("bateria_pct"), 0, 100)):
        if not _number(value, low, high):
            errors.append(field + "_invalido")
    if row.get("estado") not in ("EN_RUTA", "ENTREGADO", "PAUSADO"):
        errors.append("estado_invalido")
    return errors


def fallo_definitivo(context):
    ti = context["ti"]
    LOG.error("Fallo definitivo: dag=%s tarea=%s run=%s error=%s. Dependientes bloqueadas (all_success).",
              ti.dag_id, ti.task_id, context.get("run_id"), context.get("exception"))


def _fallo_temporal(context):
    run = context.get("dag_run")
    conf = getattr(run, "conf", {}) or {}
    ti = context.get("ti")
    if conf.get("simular_fallo_temporal") is True and getattr(ti, "try_number", 1) == 1:
        LOG.warning("Fallo temporal controlado: intento 1; Airflow debe reintentar")
        raise OSError("Fallo temporal controlado de lectura")


def verificar_fuente(**context):
    """Descubre Parquet y valida el esquema de cada archivo antes de leer filas."""
    from pyarrow.parquet import read_schema

    files = sorted((ROOT / "processed").rglob("*.parquet"))
    if not files:
        _fail("No hay Parquet en datalake/processed; ejecutar Spark primero")
    for file in files:
        try:
            columns = set(read_schema(file).names)
        except Exception as exc:
            LOG.exception("Error de lectura Parquet: %s", file)
            raise OSError(f"Error de lectura Parquet: {file}") from exc
        missing = REQUIRED - columns
        if missing:
            _fail(f"Columnas faltantes en {file}: {sorted(missing)}")

    directory = _directory(context)
    manifest = directory / "manifest_fuentes.json"
    payload = {"archivos": [str(file) for file in files],
               "bytes_total": sum(file.stat().st_size for file in files)}
    manifest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LOG.info("Fuente verificada: archivos=%s bytes=%s manifest=%s",
             len(files), payload["bytes_total"], manifest)
    return {"archivos": len(files), "bytes_total": payload["bytes_total"],
            "ruta_manifest": str(manifest)}


def _source_files(context):
    """Lee las rutas verificadas; permite invocación directa en pruebas unitarias."""
    ti = context.get("ti")
    if ti is not None and hasattr(ti, "xcom_pull"):
        meta = ti.xcom_pull(task_ids="verificar_fuente")
        if not isinstance(meta, dict) or not meta.get("ruta_manifest"):
            _fail("Calidad: faltan metadatos XCom de verificar_fuente")
        try:
            payload = json.loads(Path(meta["ruta_manifest"]).read_text(encoding="utf-8"))
            files = [Path(value) for value in payload["archivos"]]
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("Calidad: manifiesto de fuentes ilegible") from exc
        if len(files) != meta.get("archivos") or not files:
            _fail("Calidad: el manifiesto no coincide con el conteo de fuentes")
        return files
    return sorted((ROOT / "processed").rglob("*.parquet"))


def validar_calidad(**context):
    import pandas as pd
    _fallo_temporal(context)
    files = _source_files(context)
    if not files:
        _fail("No hay Parquet en datalake/processed; ejecutar Spark primero")
    frames = []
    for file in files:
        try:
            frame = pd.read_parquet(file)
        except Exception as exc:
            LOG.exception("Error de lectura Parquet: %s", file)
            raise OSError(f"Error de lectura Parquet: {file}") from exc
        missing = REQUIRED - set(frame.columns)
        if missing:
            _fail(f"Columnas faltantes en {file}: {sorted(missing)}")
        frames.append(frame)
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        _fail("No hay eventos procesados")
    reasons = df.apply(motivos_evento, axis=1)
    # Se excluyen TODAS las apariciones de un ID repetido: no se elige arbitrariamente.
    duplicates = df.event_id.duplicated(keep=False)
    for index in df.index[duplicates]:
        reasons.at[index].append("event_id_duplicado")
    invalid = reasons.map(bool)
    directory = _directory(context)
    rejected = df.loc[invalid].copy()
    rejected["motivos"] = reasons.loc[invalid].map(lambda x: ";".join(x))
    rejected.to_json(directory / "rechazados.jsonl", orient="records", lines=True, force_ascii=False)
    LOG.info("Calidad: procesados=%s invalidos=%s validos=%s; rechazos=%s",
             len(df), int(invalid.sum()), int((~invalid).sum()), directory / "rechazados.jsonl")
    if invalid.all():
        _fail(f"Se detectaron {int(invalid.sum())} eventos inválidos; no quedan eventos válidos")
    snapshot = directory / "validos.parquet"
    df.loc[~invalid].to_parquet(snapshot, index=False)
    return {"archivos": len(files), "procesados": len(df), "invalidos": int(invalid.sum()),
            "eventos": int((~invalid).sum()), "ruta_validos": str(snapshot),
            "ruta_rechazos": str(directory / "rechazados.jsonl"),
            "estado": "con_rechazos" if invalid.any() else "valido"}


def agregar_datos(**context):
    import pandas as pd
    meta = context["ti"].xcom_pull(task_ids="validar_calidad")
    if not isinstance(meta, dict) or not meta.get("ruta_validos"):
        _fail("Agregación: faltan metadatos XCom de validar_calidad")
    try:
        df = pd.read_parquet(meta["ruta_validos"])
        if len(df) != meta["eventos"]:
            _fail("Agregación: snapshot no coincide con conteo validado")
        df["bateria_critica_calculada"] = df["bateria_pct"] <= 15
        df["exceso_velocidad_calculado"] = df["velocidad_kmh"] > 60
        summary = df.groupby("repartidor_id").agg(
            eventos=("event_id", "count"), velocidad_promedio=("velocidad_kmh", "mean"),
            eventos_bateria_critica=("bateria_critica_calculada", "sum"),
            eventos_exceso_velocidad=("exceso_velocidad_calculado", "sum")).reset_index()
        out = _directory(context) / "agregados.csv"
        summary.to_csv(out, index=False)
    except Exception:
        LOG.exception("Error durante agregación del snapshot validado")
        raise
    return {**meta, "ruta": str(out), "repartidores": len(summary)}


def generar_reporte(**context):
    meta = context["ti"].xcom_pull(task_ids="agregar_datos")
    if not isinstance(meta, dict) or not meta.get("ruta"):
        _fail("Reporte: faltan metadatos XCom de agregar_datos")
    out = _directory(context) / "reporte.json"
    payload = {"proyecto": "RapidGo 360", "fecha": context["ds"],
               "generado_utc": datetime.now(timezone.utc).isoformat(), **meta}
    try:
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        LOG.exception("Error al escribir reporte: %s", out)
        raise
    return {"ruta": str(out), "estado": meta["estado"], "eventos": meta["eventos"]}


def auditar_salida(**context):
    """Comprueba que el reporte JSON y el agregado describan el mismo resultado."""
    import pandas as pd

    report_meta = context["ti"].xcom_pull(task_ids="generar_reporte")
    aggregate_meta = context["ti"].xcom_pull(task_ids="agregar_datos")
    if not isinstance(report_meta, dict) or not report_meta.get("ruta"):
        _fail("Auditoría: faltan metadatos XCom de generar_reporte")
    if not isinstance(aggregate_meta, dict) or not aggregate_meta.get("ruta"):
        _fail("Auditoría: faltan metadatos XCom de agregar_datos")
    report_path = Path(report_meta["ruta"])
    aggregate_path = Path(aggregate_meta["ruta"])
    if not report_path.is_file() or not aggregate_path.is_file():
        _fail("Auditoría: no se encontraron los artefactos de salida")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    summary = pd.read_csv(aggregate_path)
    total = int(summary["eventos"].sum()) if not summary.empty else 0
    riders = int(len(summary))
    expected = (int(report.get("eventos", -1)), int(report.get("repartidores", -1)))
    if (total, riders) != expected:
        _fail(f"Auditoría: conteos inconsistentes; CSV={(total, riders)} JSON={expected}")
    result = {"estado": "verificado", "eventos": total, "repartidores": riders,
              "reporte": str(report_path), "agregado": str(aggregate_path)}
    LOG.info("Salida auditada: eventos=%s repartidores=%s", total, riders)
    return result


with DAG(
    dag_id="rapidgo_seguridad_semanal",
    description="Validación, agregación y reporte del Data Lake de RapidGo",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), schedule=None,
    catchup=False, max_active_runs=1,
    default_args={"owner": "kevin", "retries": 2, "retry_delay": timedelta(seconds=30),
                  "execution_timeout": timedelta(minutes=5), "on_failure_callback": fallo_definitivo},
    tags=["rapidgo", "semana3", "seguridad"],
) as dag:
    fuente = PythonOperator(task_id="verificar_fuente", python_callable=verificar_fuente)
    calidad = PythonOperator(task_id="validar_calidad", python_callable=validar_calidad)
    agregacion = PythonOperator(task_id="agregar_datos", python_callable=agregar_datos)
    reporte = PythonOperator(task_id="generar_reporte", python_callable=generar_reporte)
    auditoria = PythonOperator(task_id="auditar_salida", python_callable=auditar_salida)
    fuente >> calidad >> agregacion >> reporte >> auditoria
