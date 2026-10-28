# Data Lake - RapidGo 360

Directorio donde el pipeline persiste los datos de la Variante #2: Seguridad del repartidor. Todos los datos son sintéticos. Los archivos generados no se versionan (ver `.gitignore`); solo se conservan los `.gitkeep` que mantienen la estructura de carpetas.

## Flujo

Producer → Kafka → Spark Structured Streaming → Raw → Processed → Curated. Airflow (Semana 3) validará la calidad y generará reportes sobre estas capas.

## Capas de almacenamiento

| Capa | Ruta | Formato | Contenido |
| --- | --- | --- | --- |
| Raw | `datalake/raw/` | Parquet | Eventos originales recibidos, sin transformar. |
| Processed | `datalake/processed/` | Parquet | Eventos válidos, validados y con columnas derivadas. |
| Curated | `datalake/curated/` | Parquet | KPIs por ventana y alertas, listos para reportes. |
| Checkpoints | `datalake/checkpoints/<consulta>/` | Metadatos de Spark | Un directorio por consulta de streaming. |

## Columnas esperadas

Propuesta inicial, a confirmar con quien implemente el job de Spark (Issue #14).

**Raw**
- `value`: JSON original del evento.
- `kafka_partition`, `kafka_offset`, `kafka_timestamp`: metadatos de Kafka.

**Processed**
- Campos del contrato (`docs/contrato-eventos.md`): `event_id`, `repartidor_id`, `pedido_id`, `timestamp`, `velocidad_kmh`, `bateria_pct`, `estado`, `desvio_metros` y las coordenadas del GPS.
- Columnas derivadas: `exceso_velocidad`, `bateria_critica`, `desvio_critico`.

**Curated**
- `datalake/curated/kpi_velocidad/`: `repartidor_id`, `ventana_inicio`, `ventana_fin`, `velocidad_promedio`, `total_eventos`.
- `datalake/curated/alertas/`: `event_id`, `repartidor_id`, `tipo_alerta` (`EXCESO_VELOCIDAD` o `BATERIA_CRITICA`), `valor`, `timestamp`.

## Reglas

- Los eventos se validan según `docs/contrato-eventos.md`. Los inválidos se conservan en Raw para auditoría y no pasan a Processed.
- Las alertas y los umbrales (60 km/h, 15 %, 300 m) están definidos en `spark/reglas.py`. Son umbrales didácticos, no normativos.
