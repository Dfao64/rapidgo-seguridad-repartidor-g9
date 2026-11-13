# Semana 4 — Daniel — Issue #19: integración y evidencias

## Alcance comprobable
`tests/test_semana4_integracion.py` comprueba el contrato del Producer, reproducibilidad, tres KPI y dos alertas con datos sintéticos controlados. **Estas pruebas no son end-to-end de infraestructura**: no levantan Kafka, Spark, Data Lake ni Airflow. No declarar cumplido el Issue #19 hasta completar la siguiente prueba integrada.

## Resultados esperados de datos controlados
| ID | Velocidad | Batería | Desvío | Alertas |
| --- | ---: | ---: | ---: | --- |
| evt-1 | 68.5 | 12 | 180 | EXCESO_VELOCIDAD, BATERIA_CRITICA |
| evt-2 | 31.5 | 80 | 450 | ninguna |
| evt-3 | 60 | 15 | ausente | BATERIA_CRITICA |
| evt-4 | 40 | 60 | 300 | ninguna |

Esperado por REP-001: velocidad media **50 km/h**; batería crítica **2/4 = 50 %**; desvíos críticos **1/3 = 33.33 %** (el evento sin desvío no entra al denominador); exceso de velocidad **1/4**. Un registro con batería 101 se rechaza y no genera alertas.

## Verificación local de pruebas unitarias
Desde la raíz del repositorio:

```bash
python -m pip install -r requirements.txt
python -m pytest tests/test_semana4_integracion.py -v
python -m pytest tests/ -v
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config --quiet
```

## Prueba real de infraestructura (pendiente hasta ejecución y evidencias)
1. Preparar Docker Desktop y verificar que Docker Compose funciona.
2. Crear topic si no existe:
   ```bash
   docker compose up -d kafka spark
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic rapidgo.seguridad.repartidor.v1 --partitions 3 --replication-factor 1
   ```
3. Ejecutar el job de Data Lake de Spark desde el contenedor (ver `docs/streaming-kevin.md` para paquete Kafka/Spark requerido por `spark-submit`). Confirmar escritura en `datalake/raw`, `datalake/processed` y `datalake/curated`. **Atención:** el volumen Spark en `docker-compose.yml` está montado como solo lectura; para persistir en `datalake/` se debe habilitar un volumen de escritura en un override de prueba antes de lanzar el job.
4. Generar eventos:
   ```bash
   python -m producer --riders 2 --rounds 10 --seed 42 --interval 1 --run-id semana4-prueba --start-time 2026-11-01T12:00:00Z
   ```
   Si el Producer corre fuera de Docker, usar el broker `localhost:9092`.
5. Verificar offsets del topic y archivos Parquet generados. No inferir éxito de la publicación a partir de logs de inicio.
6. Iniciar Airflow:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow
   ```
   Abrir http://localhost:8080, activar `rapidgo_seguridad_semanal`, ejecutar manualmente y comprobar tres tareas en verde y CSV/JSON en `airflow/reports`.
7. Comparar KPIs y alertas con un lote controlado; el Producer aleatorio no produce por sí solo las cuatro filas de la tabla. Para la comparación exacta publicar esas cuatro filas usando la misma clave Kafka `repartidor_id` y `event_id` únicos.
8. Guardar evidencias auténticas (capturas o logs) de contenedores saludables, topic, publicación, Spark, Parquet, DAG en verde, reportes, pytest y CI. Registrar fecha, comando, commit y resultado.

## Riesgos conocidos antes de cerrar
- `spark/datalake.py` calcula la agregación por ventana sin deduplicar eventos, según su docstring. Verificar comportamiento con duplicados.
- La capa Curated contiene `kpi_velocidad` y alertas; el KPI 3 de desvíos se calcula en `spark/indicadores.py`, pero no hay una salida Curated específica. Validar y resolver antes de afirmar que los tres KPI se persisten.
- El DAG de Airflow procesa `datalake/processed` en batch; no hay coordinación automática con la finalización de Spark.
- El contrato de alertas describe supresión de repetición por estado y ventana de cinco minutos, mientras que la implementación Curated actual emite alertas por evento. No declarar conformidad plena con esa regla sin corregirla.
- No crear el release `v1.0.0` hasta que los Issues #19, #20 y #21, revisiones y CI estén completados.

## Entrega
PR hacia `main`, revisión por Kevin y Deimer, CI verde y evidencias adjuntas antes de fusionar. El responsable del release final es Deimer (Issue #21).
