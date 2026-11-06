"""Persistencia del Data Lake en Parquet: Raw, Processed y Curated.

Reutiliza parse_events y unique_valid de spark/streaming.py; no duplica la
validación. Escribe una consulta por salida, cada una con su checkpoint:

  raw                    todos los eventos recibidos, con motivos de rechazo
  processed              eventos válidos sin duplicados y con columnas derivadas
  curated/alertas        eventos que disparan EXCESO_VELOCIDAD o BATERIA_CRITICA
  curated/kpi_velocidad  KPI por ventana y repartidor (velocidad y batería crítica)

Limitación: el KPI por ventana se calcula sobre los eventos válidos sin
deduplicar; los duplicados repetidos inflarían el conteo.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark.reglas import UMBRAL_BATERIA_PCT
from spark.streaming import parse_events, unique_valid


def alertas(validos):
    """Eventos válidos que disparan al menos una alerta."""
    from pyspark.sql import functions as F
    tipo = F.concat_ws(
        ",",
        F.when(F.col("exceso_velocidad"), F.lit("EXCESO_VELOCIDAD")),
        F.when(F.col("bateria_critica"), F.lit("BATERIA_CRITICA")))
    return (validos
            .filter(F.col("exceso_velocidad") | F.col("bateria_critica"))
            .select("event_id", "repartidor_id", "pedido_id", "timestamp_utc",
                    "velocidad_kmh", "bateria_pct", tipo.alias("tipo_alerta")))


def kpi_velocidad(parsed, ventana="5 minutes", watermark="10 minutes"):
    """Velocidad promedio y batería crítica por ventana y repartidor."""
    from pyspark.sql import functions as F
    validos = (parsed.filter("valido")
               .select("timestamp_utc",
                       F.col("evento.repartidor_id").alias("repartidor_id"),
                       F.col("evento.velocidad_kmh").alias("velocidad_kmh"),
                       F.col("evento.bateria_pct").alias("bateria_pct")))
    return (validos.withWatermark("timestamp_utc", watermark)
            .groupBy(F.window("timestamp_utc", ventana), "repartidor_id")
            .agg(F.avg("velocidad_kmh").alias("velocidad_promedio"),
                 F.count(F.lit(1)).alias("total_eventos"),
                 F.sum(F.when(F.col("bateria_pct") <= UMBRAL_BATERIA_PCT, 1)
                       .otherwise(0)).alias("eventos_bateria_critica"))
            .select(F.col("window.start").alias("ventana_inicio"),
                    F.col("window.end").alias("ventana_fin"),
                    "repartidor_id", "velocidad_promedio", "total_eventos",
                    "eventos_bateria_critica"))


def start_queries(parsed, lake, trigger="5 seconds", ventana="5 minutes",
                  watermark="10 minutes"):
    """Inicia una consulta Parquet por salida, con checkpoint propio."""
    validos = unique_valid(parsed)
    salidas = (
        ("raw", parsed, "raw"),
        ("processed", validos, "processed"),
        ("alertas", alertas(validos), "curated/alertas"),
        ("kpi_velocidad", kpi_velocidad(parsed, ventana, watermark),
         "curated/kpi_velocidad"),
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
