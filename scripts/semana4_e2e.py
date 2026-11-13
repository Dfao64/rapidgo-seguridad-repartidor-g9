"""Ejecuta Producer → Kafka → Spark → Parquet → Airflow y guarda evidencia."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from producer.main import config, events, publish

DAG_ID = "rapidgo_seguridad_semanal"
KPI = {
    "velocidad_promedio": 54.0,
    "eventos_bateria_critica": 4,
    "bateria_critica_pct": 400 / 7,
    "eventos_con_desvio": 6,
    "eventos_desvio_critico": 1,
    "desvio_critico_pct": 100 / 6,
}


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def read_rows(directory):
    import pandas as pd

    files = sorted(directory.rglob("*.parquet"))
    return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True) if files else pd.DataFrame()


def wait_rows(directory, ready, job, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if job.poll() is not None:
            raise RuntimeError(f"Spark terminó con código {job.returncode}. Revisar spark.log")
        rows = read_rows(directory)
        if not rows.empty and ready(rows):
            return rows
        time.sleep(2)
    raise TimeoutError(f"Datos esperados ausentes en {directory}")


def verify_kpis(row, count_field):
    import math

    actual = {key: float(row[key]) for key in KPI}
    assert row[count_field] == 7, row.to_dict()
    for key, expected in KPI.items():
        assert math.isclose(actual[key], expected, rel_tol=1e-9), (key, expected, actual[key])
    return actual


def main():
    from contextlib import redirect_stdout
    import pandas as pd
    from kafka import KafkaProducer

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=360)
    args = parser.parse_args()
    run_id = uuid.uuid4().hex[:12]
    topic = f"rapidgo.semana4.{run_id}"
    output = ROOT / "datalake" / f"semana4-{run_id}"
    reports = output / "reports"
    reports.mkdir(parents=True)
    # El contenedor Airflow tiene un UID diferente en Linux.
    reports.chmod(0o777)
    compose = ["docker", "compose", "-p", f"rapidgo-e2e-{run_id}",
               "-f", "docker-compose.yml", "-f", "docker-compose.airflow.yml"]
    result = {"run_id": run_id, "topic": topic, "kpi_esperados": KPI}
    job = None

    def execute(name, *command, check=True):
        print(f"{name}: {' '.join(command)}", flush=True)
        with (output / f"{name}.log").open("wb") as log:
            return subprocess.run([*compose, *command], cwd=ROOT, stdout=log,
                                  stderr=subprocess.STDOUT, check=check, timeout=args.timeout)

    try:
        execute("compose-config", "config", "--quiet")
        execute("compose-up", "up", "-d", "--build", "--wait", "kafka", "spark")
        execute("kafka-topic", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "kafka:29092", "--create", "--topic", topic,
                "--partitions", "3", "--replication-factor", "1")
        lake = f"/workspace/datalake/{output.name}"
        spark = ["exec", "-T", "spark", "/bin/bash", "-ec",
                 'exec /opt/spark/bin/spark-submit --master local[4] '
                 '--conf spark.ui.enabled=false '
                 '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 '
                 '/workspace/spark/datalake.py --topic "$1" --datalake "$2" --trigger "2 seconds"',
                 "spark", topic, lake]
        with (output / "spark.log").open("wb") as log:
            job = subprocess.Popen([*compose, *spark], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)

        producer_args = config(["--topic", topic, "--riders", "8", "--rounds", "1",
                                "--start-time", "2026-11-01T12:00:00Z", "--run-id", run_id])
        generated = list(events(producer_args))
        variants = [(68, 12, 180, "12:00:00"), (70, 14, 450, "12:00:30"),
                    (60, 50, 300, "12:01:00"), (65, 15, None, "12:03:30"),
                    (65, 15, 100, "12:09:00"), (30, 80, 200, "12:01:10"),
                    (20, 90, 50, "12:01:20")]
        controlled = [
            dict(base, repartidor_id="REP-E2E", pedido_id="PED-E2E", estado="EN_RUTA",
                 velocidad_kmh=speed, bateria_pct=battery, desvio_metros=deviation,
                 timestamp=f"2026-11-01T{stamp}Z")
            for base, (speed, battery, deviation, stamp) in zip(generated, variants)
        ]
        # Mantener orden cronológico por clave. El recordatorio cae en la segunda ventana.
        marker = dict(generated[-1], timestamp="2026-11-01T12:30:00Z", desvio_metros=None)
        rejected = dict(marker, event_id=f"{run_id}-invalid", bateria_pct=101)
        initial = [*controlled[:3], *controlled[5:], *controlled[3:5], controlled[0], rejected]
        write_json(output / "eventos.json", [*initial, marker])
        client = KafkaProducer(bootstrap_servers="127.0.0.1:9092", acks="all")
        try:
            with (output / "producer.log").open("w", encoding="utf-8") as log, redirect_stdout(log):
                assert publish(producer_args, client, sleep=lambda _: None, records=initial) == len(initial)
                processed = wait_rows(output / "processed", lambda rows: len(rows) >= 7, job, args.timeout)
                assert set(processed.event_id) == {row["event_id"] for row in controlled}
                assert len(processed) == 7
                assert publish(producer_args, client, records=[marker]) == 1
        finally:
            client.close(timeout=30)

        raw = wait_rows(output / "raw", lambda rows: len(rows) >= 10, job, args.timeout)
        processed = wait_rows(output / "processed", lambda rows: len(rows) >= 8, job, args.timeout)
        alerts = wait_rows(output / "curated" / "alertas",
                           lambda rows: len(rows[rows.repartidor_id == "REP-E2E"]) >= 3,
                           job, args.timeout)
        kpis = wait_rows(output / "curated" / "kpi_velocidad",
                         lambda rows: len(rows[rows.repartidor_id == "REP-E2E"]) >= 2, job, args.timeout)
        assert len(raw) == 10 and int(raw.valido.sum()) == 9
        assert len(processed) == 8 and processed.event_id.is_unique
        relevant_alerts = alerts[alerts.repartidor_id == "REP-E2E"]
        expected_alerts = {controlled[i]["event_id"]: "EXCESO_VELOCIDAD,BATERIA_CRITICA" for i in (0, 3, 4)}
        assert len(relevant_alerts) == 3
        assert dict(zip(relevant_alerts.event_id, relevant_alerts.tipo_alerta)) == expected_alerts
        relevant_kpis = kpis[kpis.repartidor_id == "REP-E2E"].sort_values("ventana_inicio")
        assert list(relevant_kpis.total_eventos) == [6, 1]
        first, second = (row for _, row in relevant_kpis.iterrows())
        assert first.velocidad_promedio == 313 / 6
        assert first.eventos_bateria_critica == 3 and first.bateria_critica_pct == 50
        assert first.eventos_con_desvio == 5 and first.eventos_desvio_critico == 1
        assert first.desvio_critico_pct == 20
        assert second.velocidad_promedio == 65 and second.bateria_critica_pct == 100
        assert second.eventos_con_desvio == 1 and second.desvio_critico_pct == 0
        result.update(raw=len(raw), processed=len(processed), alertas=expected_alerts)
        relevant_kpis.to_csv(output / "curated-kpi.csv", index=False)
        relevant_alerts.to_csv(output / "curated-alertas.csv", index=False)

        execute("airflow", "run", "--rm", "--no-deps", "-T",
                "-e", f"RAPIDGO_DATALAKE_ROOT=/opt/airflow/datalake/{output.name}",
                "-e", f"RAPIDGO_REPORTS_ROOT=/opt/airflow/datalake/{output.name}/reports",
                "-e", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/rapidgo-airflow.db",
                "-v", f"{output.as_posix()}:/opt/airflow/datalake/{output.name}",
                "airflow", "bash", "-ec",
                f"airflow db migrate && airflow dags test {DAG_ID} 2026-11-02T00:00:00+00:00")
        (report,) = reports.rglob("reporte.json")
        airflow_report = json.loads(report.read_text(encoding="utf-8"))
        summary = pd.read_csv(report.with_name("agregados.csv"))
        assert airflow_report["eventos"] == int(summary.eventos.sum()) == 8
        assert airflow_report["repartidores"] == len(summary) == 2
        assert airflow_report["invalidos"] == 0 and airflow_report["estado"] == "valido"
        airflow_log = (output / "airflow.log").read_text(encoding="utf-8", errors="replace")
        assert "Salida auditada: eventos=8 repartidores=2" in airflow_log
        assert any("DagRun Finished:" in line and "state=success" in line
                   for line in airflow_log.splitlines()), "Airflow no registró un DAG exitoso"
        result["kpi_airflow"] = verify_kpis(summary.set_index("repartidor_id").loc["REP-E2E"], "eventos")
        result["estado"] = "verificado"
        return 0
    except Exception as exc:
        result.update(estado="fallido", error=repr(exc))
        raise
    finally:
        write_json(output / "resultado_e2e.json", result)
        print(json.dumps(result, indent=2), flush=True)
        try:
            execute("servicios", "logs", "--no-color", check=False)
        finally:
            execute("cleanup", "down", "--volumes", check=False)
            if job is not None and job.poll() is None:
                job.terminate()
                try:
                    job.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    job.kill()
                    job.wait()


if __name__ == "__main__":
    sys.exit(main())
