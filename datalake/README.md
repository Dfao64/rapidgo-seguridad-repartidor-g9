# Data Lake - RapidGo 360

Persistencia en Parquet de la Variante #2: Seguridad del repartidor. Todos los datos son sintéticos. Los archivos generados no se versionan (ver `.gitignore`); solo se conservan los `.gitkeep`.

## Flujo

Producer → Kafka (`rapidgo.seguridad.repartidor.v1`) → Spark Structured Streaming (`spark/datalake.py`) → Raw, Processed y Curated.

## Capas

| Capa | Ruta | Contenido |
| --- | --- | --- |
| Raw | `raw/` | Todos los eventos recibidos, incluso los inválidos, con `motivos` y `valido`. |
| Processed | `processed/` | Eventos válidos, sin duplicados por `event_id`, con indicadores derivados. |
| Curated | `curated/alertas/` | Eventos que disparan al menos una alerta. |
| Curated | `curated/kpi_velocidad/` | KPI por ventana de tiempo y repartidor. |
| Checkpoints | `checkpoints/<consulta>/` | Un checkpoint por consulta: `raw`, `processed`, `alertas`, `kpi_velocidad`. |

## Columnas (observadas en una ejecución real)

**Raw:** `topic`, `partition`, `offset`, `kafka_timestamp`, `key_original`, `value_original`, `motivos`, `evento` (estructura con los campos del contrato), `valido`, `timestamp_utc`.

**Processed:** `topic`, `partition`, `offset`, `kafka_timestamp`, `key_original`, `value_original`, `timestamp_utc`, `event_id`, `repartidor_id`, `pedido_id`, `timestamp`, `estado`, `gps`, `velocidad_kmh`, `bateria_pct`, `desvio_metros`, `incluye_kpi3`, `exceso_velocidad`, `bateria_critica`, `desvio_critico`.

**Curated / alertas:** `event_id`, `repartidor_id`, `pedido_id`, `timestamp_utc`, `velocidad_kmh`, `bateria_pct`, `tipo_alerta` (`EXCESO_VELOCIDAD`, `BATERIA_CRITICA` o ambas separadas por coma).

**Curated / kpi_velocidad:** `ventana_inicio`, `ventana_fin`, `repartidor_id`, `velocidad_promedio`, `total_eventos`, `eventos_bateria_critica`.

## Ejecución

El job lee de Kafka y escribe en la ruta indicada por `--datalake` (por defecto `/workspace/datalake`, que requiere un montaje escribible en el contenedor; en pruebas locales se usó `/state/datalake`).

    docker-compose exec -T spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/spark/datalake.py --datalake /state/datalake

## Evidencia

Ejecución de referencia con 180 eventos: raw 180, processed 180, alertas 43 y 12 filas de KPI. Detalle en `docs/evidencias/deimer/persistencia-parquet.txt`.

## Limitaciones

- Cada evento que supera un umbral genera su propia alerta; no hay enfriamiento ni deduplicación por repartidor.
- El KPI por ventana no deduplica eventos y una ventana solo se escribe cuando el watermark la cierra.
- El producer solo emite eventos válidos: el rechazo de eventos inválidos se prueba con las pruebas automatizadas, no con datos reales.