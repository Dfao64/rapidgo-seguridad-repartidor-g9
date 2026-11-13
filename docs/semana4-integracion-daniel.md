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

## Prueba real de infraestructura

El procedimiento reproducible y los resultados esperados están en [Semana 4 · Prueba end-to-end](semana4-ejecucion-e2e.md). El ejecutor publica datos controlados en Kafka, comprueba las tres capas Parquet y ejecuta las cinco tareas del DAG sobre Processed. Curated calcula los tres KPI desde los eventos deduplicados y aplica las transiciones de las dos alertas.

La ejecución en Docker y GitHub Actions se registra en el [PR de seguimiento #44](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/pull/44), abierto sobre la misma rama después de la fusión del #42.

## Entrega

PR hacia `main`, revisión por Kevin y Deimer, CI verde y evidencias adjuntas antes de fusionar. El responsable del release final es Deimer (Issue #21).
