"""Prueba de integración reproducible con infraestructura REAL.

Requiere Docker Compose, Python (kafka-python, pandas, pyarrow) y Spark con
el conector spark-sql-kafka-0-10. No simula resultados de Kafka ni Airflow.
Ejecutar desde la raíz: python scripts/semana4_e2e.py
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "rapidgo.seguridad.repartidor.v1"


def run(*args, timeout=180, capture=True):
    cmd = list(args)
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=ROOT, check=True, text=True, timeout=timeout,
                          capture_output=capture)


def compose(*args, timeout=180):
    return run("docker", "compose", *args, timeout=timeout)


def event(index, speed, battery, deviation):
    return {"event_id": f"semana4-{index}-{uuid.uuid4().hex}",
            "repartidor_id": "REP-E2E", "pedido_id": "PED-E2E",
            "timestamp": "2026-11-01T12:00:%02dZ" % index,
            "gps": {"latitud": 4.732, "longitud": -74.264},
            "velocidad_kmh": speed, "bateria_pct": battery,
            "estado": "EN_RUTA", "desvio_metros": deviation}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--spark-package", default="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
    args = parser.parse_args()
    if not shutil.which("docker"):
        parser.error("Docker no está instalado o no está en PATH")
    try:
        import pandas as pd
        from kafka import KafkaProducer
    except ImportError as exc:
        parser.error(f"Dependencias Python ausentes: {exc}; instalar kafka-python pandas pyarrow")

    # Evitar falsos positivos: directorio de prueba nuevo en cada ejecución.
    run_id = uuid.uuid4().hex[:12]
    lake = ROOT / "datalake"
    output = lake / f"semana4-{run_id}"
    output.mkdir(parents=True)
    result = {"run_id": run_id, "directorio": str(output), "estado": "iniciado"}
    manifest = output / "resultado_e2e.json"
    def save():
        manifest.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    job = None
    try:
        compose("up", "-d", "--wait", "kafka", "spark", timeout=args.timeout)
        compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "kafka:29092", "--create", "--if-not-exists",
                "--topic", TOPIC, "--partitions", "3", "--replication-factor", "1")
        # Se lanza un job real en Spark y se conserva su salida para diagnóstico.
        cmd = ["docker", "compose", "exec", "-T", "spark", "/opt/spark/bin/spark-submit",
               "--packages", args.spark_package, "/workspace/spark/datalake.py",
               "--datalake", f"/workspace/datalake/{output.name}",
               "--trigger", "2 seconds", "--ventana", "5 minutes"]
        print("+", " ".join(cmd), flush=True)
        log = (output / "spark.log").open("w", encoding="utf-8")
        job = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        time.sleep(12)
        if job.poll() is not None:
            raise RuntimeError("Spark terminó antes de publicar eventos; revisar spark.log")

        data = [event(1, 68.5, 12, 180), event(2, 31.5, 80, 450),
                event(3, 60, 15, None), event(4, 40, 60, 300)]
        producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092", acks="all",
                                 value_serializer=lambda x: json.dumps(x).encode())
        try:
            for row in data:
                producer.send(TOPIC, key=row["repartidor_id"].encode(),
                              value=row).get(timeout=30)
            producer.flush(timeout=30)
        finally:
            producer.close(timeout=30)
        result["publicados"] = 4
        deadline = time.monotonic() + args.timeout
        processed = output / "processed"
        while time.monotonic() < deadline:
            files = list(processed.rglob("*.parquet"))
            if files:
                df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
                selected = df[df["event_id"].isin([x["event_id"] for x in data])]
                if len(selected) == 4:
                    break
            if job.poll() is not None:
                raise RuntimeError("Spark finalizó durante el procesamiento; revisar spark.log")
            time.sleep(3)
        else:
            raise TimeoutError("No se encontraron los cuatro eventos en Processed")
        assert selected["velocidad_kmh"].mean() == 50.0
        assert int((selected["bateria_pct"] <= 15).sum()) == 2
        with_deviation = selected["desvio_metros"].dropna()
        assert len(with_deviation) == 3
        assert int((with_deviation > 300).sum()) == 1
        result["kpis"] = {"velocidad_promedio": 50, "bateria_critica_pct": 50,
                          "desvio_critico_pct": 33.33}
        alert_files = list((output / "curated" / "alertas").rglob("*.parquet"))
        if not alert_files:
            raise RuntimeError("No se escribieron alertas en Curated")
        alerts = pd.concat((pd.read_parquet(f) for f in alert_files), ignore_index=True)
        alerts = alerts[alerts["event_id"].isin([x["event_id"] for x in data])]
        assert set(alerts["event_id"]) == {data[0]["event_id"], data[2]["event_id"]}
        assert {"EXCESO_VELOCIDAD", "BATERIA_CRITICA"} <= set(
            ",".join(alerts["tipo_alerta"].tolist()).split(","))
        result["alertas_verificadas"] = 2
        # Airflow procesa /workspace/datalake, no el subdirectorio aislado de
        # este run: configurar un override de volumen y ejecutar el DAG por
        # separado antes de declarar la integración completa.
        result["estado"] = "kafka_spark_datalake_verificados_airflow_pendiente"
        save()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:
        result["estado"] = "fallido"
        result["error"] = str(exc)
        save()
        print(f"E2E FALLÓ: {exc}. Evidencia: {manifest}", file=sys.stderr)
        return 1
    finally:
        if job is not None and job.poll() is None:
            job.terminate()
            try:
                job.wait(timeout=20)
            except subprocess.TimeoutExpired:
                job.kill()


if __name__ == "__main__":
    sys.exit(main())
