"""Consumidor continuo Kafka. Auditoría completa y salida válida deduplicada."""
import argparse
import json
import sys
from pathlib import Path

# spark-submit y los workers deben poder importar el paquete spark del proyecto.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark.stream_validation import validation_errors
from spark.reglas import UMBRAL_VELOCIDAD_KMH, UMBRAL_BATERIA_PCT, UMBRAL_DESVIO_M


def event_schema():
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    fields = [StructField(n, StringType()) for n in
              ("event_id", "repartidor_id", "pedido_id", "timestamp", "estado")]
    fields += [StructField("gps", StructType([
        StructField("latitud", DoubleType()), StructField("longitud", DoubleType())]))]
    fields += [StructField(n, DoubleType()) for n in
               ("velocidad_kmh", "bateria_pct", "desvio_metros")]
    fields.append(StructField("_corrupt_record", StringType()))
    return StructType(fields)


def parse_events(source):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType
    validate = F.udf(validation_errors, ArrayType(StringType(), False))
    data = source.select(
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("key_original"),
        F.col("value").cast("string").alias("value_original"))
    return (data.withColumn("motivos", validate("value_original", "key_original"))
            .withColumn("evento", F.from_json("value_original", event_schema(),
                        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
            .withColumn("motivos", F.when(F.col("evento._corrupt_record").isNotNull(),
                        F.array_union(F.col("motivos"), F.array(F.lit("parseo_spark_invalido"))))
                        .otherwise(F.col("motivos")))
            .withColumn("valido", (F.size("motivos") == 0) & F.col("evento._corrupt_record").isNull())
            .withColumn("timestamp_utc", F.to_timestamp("evento.timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")))


def unique_valid(parsed):
    from pyspark.sql import functions as F
    # Sin watermark: deduplicación histórica dentro del checkpoint de esta consulta.
    return (parsed.filter("valido").select(
        "topic", "partition", "offset", "kafka_timestamp", "key_original", "value_original",
        "timestamp_utc", "evento.*")
        .drop("_corrupt_record").dropDuplicates(["event_id"])
        .withColumn("incluye_kpi3", F.col("desvio_metros").isNotNull())
        .withColumn("exceso_velocidad", F.col("velocidad_kmh") > UMBRAL_VELOCIDAD_KMH)
        .withColumn("bateria_critica", F.col("bateria_pct") <= UMBRAL_BATERIA_PCT)
        .withColumn("desvio_critico", F.coalesce(F.col("desvio_metros") > UMBRAL_DESVIO_M, F.lit(False))))


def start_queries(parsed, checkpoint, output=None, trigger="5 seconds"):
    queries = []
    try:
        for name, frame in (("auditoria", parsed), ("validos", unique_valid(parsed))):
            writer = (frame.writeStream.queryName(name).outputMode("append")
                      .option("checkpointLocation", checkpoint + "/" + name)
                      .trigger(processingTime=trigger))
            if output:
                writer = writer.format("json").option("path", output + "/" + name)
            else:
                writer = writer.format("console").option("truncate", "false").option("numRows", 1000)
            queries.append(writer.start())
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
    parser.add_argument("--checkpoint", default="/state/checkpoints/kevin-v1")
    parser.add_argument("--output", help="Destino JSON aislado; omitir para console")
    parser.add_argument("--trigger", default="5 seconds")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("RapidGo-Kevin-Semana2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.shuffle.partitions", "3").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap_servers)
              .option("subscribe", args.topic).option("startingOffsets", "earliest")
              .option("failOnDataLoss", "true").load())
    queries = start_queries(parse_events(source), args.checkpoint, args.output, args.trigger)
    try:
        seen = {}
        while not spark.streams.awaitAnyTermination(5):
            for query in queries:
                progress = query.lastProgress
                if progress and seen.get(query.id) != progress["batchId"]:
                    print("RAPIDGO_PROGRESS " + json.dumps(progress), flush=True)
                    seen[query.id] = progress["batchId"]
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
