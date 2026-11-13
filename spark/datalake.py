"""Persistencia del Data Lake en Parquet: Raw, Processed y Curated.

Reutiliza parse_events y unique_valid de spark/streaming.py; no duplica la
validación. Escribe una consulta por salida, cada una con su checkpoint:

  raw                    todos los eventos recibidos, con motivos de rechazo
  processed              eventos válidos sin duplicados y con columnas derivadas
  curated/alertas        transiciones y recordatorios por repartidor
  curated/kpi_velocidad  tres KPI por ventana y repartidor
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark.streaming import parse_events, unique_valid

ALERT_FIELDS = ("event_id", "repartidor_id", "pedido_id", "timestamp_utc",
                "velocidad_kmh", "bateria_pct")
ALERT_RULES = (("exceso_velocidad", "EXCESO_VELOCIDAD"),
               ("bateria_critica", "BATERIA_CRITICA"))


def _transiciones(filas, estado, vistos):
    for fila in filas:
        if fila.event_id in vistos:
            continue
        vistos.add(fila.event_id)
        segundo = fila.timestamp_utc.value // 10**9
        tipos = []
        for campo, tipo in ALERT_RULES:
            if not getattr(fila, campo):
                estado.pop(tipo, None)
            elif tipo not in estado or segundo >= estado[tipo] + 300:
                estado[tipo] = segundo
                tipos.append(tipo)
        if tipos:
            yield (*[getattr(fila, campo) for campo in ALERT_FIELDS], ",".join(tipos))


def _estado_alertas(_key, batches, state):
    import pandas as pd

    estado, ids = json.loads(state.get[0]) if state.exists else ({}, [])
    vistos = set(ids)
    ordered = pd.concat(batches).sort_values(["partition", "offset"])
    emissions = list(_transiciones(ordered.itertuples(index=False), estado, vistos))
    state.update((json.dumps([estado, sorted(vistos)]),))
    if emissions:
        yield pd.DataFrame(emissions, columns=[*ALERT_FIELDS, "tipo_alerta"])


def alertas(validos):
    """Evalúa alertas en orden de Kafka y conserva el estado en el checkpoint."""
    from pyspark.sql import functions as F
    if validos.isStreaming:
        from pyspark.sql.types import StringType, StructField, StructType

        output = StructType([validos.schema[field] for field in ALERT_FIELDS] +
                            [StructField("tipo_alerta", StringType())])
        return validos.select(*ALERT_FIELDS, "partition", "offset",
                              *[campo for campo, _ in ALERT_RULES]).groupBy(
            "repartidor_id").applyInPandasWithState(
                _estado_alertas, output, "payload string", "Append", "NoTimeout")

    tipo = F.concat_ws(
        ",",
        F.when(F.col("exceso_velocidad"), F.lit("EXCESO_VELOCIDAD")),
        F.when(F.col("bateria_critica"), F.lit("BATERIA_CRITICA")))
    return (validos
            .filter(F.col("exceso_velocidad") | F.col("bateria_critica"))
            .select("event_id", "repartidor_id", "pedido_id", "timestamp_utc",
                    "velocidad_kmh", "bateria_pct", tipo.alias("tipo_alerta")))


def kpi_velocidad(parsed, ventana="5 minutes", watermark="10 minutes"):
    """Calcula los tres KPI a partir de eventos deduplicados."""
    return _kpi_processed(unique_valid(parsed), ventana, watermark)


def _kpi_processed(validos, ventana, watermark):
    from pyspark.sql import functions as F
    return (validos.withWatermark("timestamp_utc", watermark)
            .groupBy(F.window("timestamp_utc", ventana), "repartidor_id")
            .agg(F.avg("velocidad_kmh").alias("velocidad_promedio"),
                 F.count(F.lit(1)).alias("total_eventos"),
                 F.sum(F.col("bateria_critica").cast("int")).alias("eventos_bateria_critica"),
                 F.sum(F.col("incluye_kpi3").cast("int")).alias("eventos_con_desvio"),
                 F.sum(F.col("desvio_critico").cast("int")).alias("eventos_desvio_critico"))
            .withColumn("bateria_critica_pct", 100.0 * F.col("eventos_bateria_critica") / F.col("total_eventos"))
            .withColumn("desvio_critico_pct", F.when(F.col("eventos_con_desvio") > 0,
                        100.0 * F.col("eventos_desvio_critico") / F.col("eventos_con_desvio")))
            .select(F.col("window.start").alias("ventana_inicio"),
                    F.col("window.end").alias("ventana_fin"),
                    "repartidor_id", "velocidad_promedio", "total_eventos",
                    "eventos_bateria_critica", "bateria_critica_pct",
                    "eventos_con_desvio", "eventos_desvio_critico", "desvio_critico_pct"))


def start_queries(parsed, lake, trigger="5 seconds", ventana="5 minutes",
                  watermark="10 minutes"):
    """Inicia una consulta Parquet por salida, con checkpoint propio."""
    for nombre in ("alertas", "kpi_velocidad"):
        legacy_checkpoint = Path(lake) / "checkpoints" / nombre
        if legacy_checkpoint.is_dir() and any(legacy_checkpoint.iterdir()):
            raise ValueError(f"Checkpoint anterior en {legacy_checkpoint}. Use un Data Lake nuevo para reprocesar Kafka.")

    validos = unique_valid(parsed)
    salidas = (
        ("raw", parsed, "raw"),
        ("processed", validos, "processed"),
        ("alertas_v2", alertas(unique_valid(parsed, deduplicate=False)), "curated/alertas"),
    )
    queries = []
    try:
        for nombre, frame, ruta in salidas:
            query = (frame.writeStream.queryName(nombre).outputMode("append")
                     .format("parquet")
                     .option("path", f"{lake}/{ruta}")
                     .option("checkpointLocation", f"{lake}/checkpoints/{nombre}")
                     .trigger(processingTime=trigger).start())
            queries.append(query)
        processed = (parsed.sparkSession.readStream.schema(validos.schema)
                     .parquet(f"{lake}/processed"))
        queries.append(
            _kpi_processed(processed, ventana, watermark).writeStream
            .queryName("kpi_velocidad").outputMode("append").format("parquet")
            .option("path", f"{lake}/curated/kpi_velocidad")
            .option("checkpointLocation", f"{lake}/checkpoints/kpi_velocidad_v2")
            .trigger(processingTime=trigger).start())
        return queries
    except Exception:
        for query in queries:
            query.stop()
        raise


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="rapidgo.seguridad.repartidor.v1")
    parser.add_argument("--datalake", default="/workspace/datalake")
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--ventana", default="5 minutes")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--offsets", default="earliest",
                        help="Solo aplica con un checkpoint nuevo")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("RapidGo-DataLake")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.shuffle.partitions", "3").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap_servers)
              .option("subscribe", args.topic)
              .option("startingOffsets", args.offsets)
              .option("failOnDataLoss", "true").load())
    queries = start_queries(parse_events(source), args.datalake, args.trigger,
                            args.ventana, args.watermark)
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
