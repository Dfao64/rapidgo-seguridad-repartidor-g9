# Semana 4 · Prueba end-to-end

Desde la raíz, con Docker Engine y Compose v2 disponibles, y los puertos
locales de Kafka y Airflow libres:

```bash
python -m pip install pandas pyarrow kafka-python
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config --quiet
python scripts/semana4_e2e.py
```

El ejecutor crea un proyecto Compose, un topic y un Data Lake exclusivos de
la ejecución. Genera los eventos a partir del Producer y usa su ruta habitual
de publicación con ACK. Spark consume los eventos y escribe Parquet. Airflow
ejecuta las cinco tareas con `airflow dags test` sobre esos archivos. Al
terminar, Compose elimina los contenedores y volúmenes de esa ejecución.

## Resultados esperados

- Raw contiene diez filas, incluida una entrega duplicada y un evento rechazado.
- Processed contiene ocho eventos únicos: siete del repartidor controlado y
  uno de otro repartidor que permite cerrar las ventanas.
- Curated contiene dos ventanas del repartidor controlado. La primera tiene
  seis eventos y un desvío crítico de cinco informados (20 %). La segunda
  contiene un evento y ningún desvío crítico.
- Cada regla emite al activarse, al reactivarse después de volver a normal y
  al recibir un evento activo transcurridos cinco minutos. La prueba verifica
  los IDs y tipos emitidos, la supresión y los límites 60 km/h, 15 % y 300 m.
- El CSV de Airflow registra 54 km/h, cuatro eventos con batería crítica de
  siete (57,14 %) y un desvío crítico de seis informados (16,67 %).

La evidencia queda en `datalake/semana4-*/`, con resultado JSON, eventos,
ACK de Kafka, logs, CSV de Curated y reportes del DAG. El workflow
**RapidGo Semana 4 E2E** conserva estos archivos aunque falle una etapa.

## Operación del Data Lake

Los KPI consumen Processed deduplicado y publican al cerrar la ventana según
el watermark. Si ningún evento informa desvío, el porcentaje es nulo.
Las alertas se evalúan en orden de offsets por repartidor. El recordatorio
usa el timestamp de los eventos y requiere que llegue otro evento activo.
La deduplicación histórica conserva IDs en los checkpoints.

Las consultas Curated cambiaron de estado y esquema. Para migrar un Data
Lake anterior, conserva su historial, usa un directorio nuevo y reprocesa
los offsets retenidos por Kafka. Los eventos cuyos offsets expiraron
requieren una restauración o republicación desde el histórico.
