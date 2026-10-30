from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from spark.reglas import es_evento_valido, es_exceso_velocidad, es_bateria_critica # Ajusta según tus importaciones reales

def iniciar_pipeline():
    # 1. Inicializar SparkSession
    spark = SparkSession.builder \
        .appName("RapidGo360-DataLake-Streaming") \
        .getOrCreate()

    # 2. Leer eventos desde Kafka
    # Reemplaza 'localhost:9092' y 'topic_nombre' con tus datos reales
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "rapidgo.seguridad.repartidor.v1") \
        .load()

    # 3. Guardar en capa Raw (sin transformar)
    query_raw = df_kafka.writeStream \
        .format("parquet") \
        .option("path", "datalake/raw/") \
        .option("checkpointLocation", "datalake/checkpoints/raw/") \
        .start()

    # 4. Transformar datos (Parsear JSON y aplicar reglas)
    # df_parsed = df_kafka.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), esquema).alias("data")).select("data.*")
    
    # 5. Guardar en capa Processed (datos validados)
    # query_processed = df_parsed.filter(...).writeStream \
    #    .format("parquet") \
    #    .option("path", "datalake/processed/") \
    #    .option("checkpointLocation", "datalake/checkpoints/processed/") \
    #    .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    iniciar_pipeline()
