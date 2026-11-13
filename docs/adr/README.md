# Decisiones técnicas (ADR)

- **ADR-001:** [Diseño del topic `rapidgo.seguridad.repartidor.v1` y 3 particiones Kafka por `repartidor_id`](../decisions/ADR-001-kafka-particiones.md) (Aceptada)
- **ADR-002:** Procesamiento en Spark Structured Streaming con validación estricta del contrato y Data Lake Parquet en 3 capas (`raw`, `processed`, `curated`) (Aceptada)
- **ADR-003:** [Orquestación de 5 tareas, reintentos y auditoría de salida en Apache Airflow](../decisions/ADR-003-orquestacion-airflow.md) (Aceptada)

## Resumen de ADR-002 — Spark Structured Streaming y Data Lake Parquet
- **Contexto:** Se requiere procesar el stream de Kafka en tiempo real sin que eventos malformados detengan la ingesta ni alteren el cálculo de los KPIs, persistiendo resultados para consumo batch en Airflow.
- **Decisión:** Implementar en `spark/datalake.py` la separación del flujo usando `spark/reglas.py` hacia tres capas en formato Apache Parquet con checkpoints independientes: `datalake/raw` (todos los eventos para auditoría), `datalake/processed` (solo eventos válidos particionados por `fecha` y `hora`) y `datalake/curated` (`kpi_velocidad` por ventana con *watermark* y `alertas` por evento).
- **Consecuencias:** Preserva el esquema tipado para que Airflow valide `datalake/processed` directamente mediante un volumen compartido, garantizando que ningún registro inválido afecte los reportes finales.