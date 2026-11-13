# RapidGo 360

Sistema de procesamiento de eventos en tiempo real y orquestación batch que monitorea las condiciones operativas de los repartidores en ruta para detectar riesgos de seguridad (exceso de velocidad, batería crítica en el dispositivo móvil y desviaciones de ruta).

## Variante del proyecto
Variante #2: Seguridad del repartidor

## Equipo
| Integrante | Rol |
|---|---|
| Deimer Hincapié | Documentación y release |
| Daniel Abello | Integración y pruebas end-to-end |
| Kevin Sánchez | Quality Gate y GitHub Actions |

## Arquitectura
Producer → Kafka → Spark → Data Lake → Airflow

El Producer publica eventos JSON de telemetría en el topic `rapidgo.seguridad.repartidor.v1` de Apache Kafka (3 particiones por `repartidor_id`). Spark Structured Streaming consume el flujo en tiempo real, descarta eventos inválidos según el contrato de datos y escribe archivos Parquet en las capas `raw`, `processed` y `curated` del Data Lake. Finalmente, Apache Airflow ejecuta el DAG `rapidgo_seguridad_semanal` (5 tareas) para validar calidad, agregar indicadores, generar reportes CSV/JSON y auditar la consistencia de salida. Detalle en [docs/arquitectura.md](docs/arquitectura.md).

## KPIs y alertas
- **KPI 1 (Velocidad promedio por repartidor):** `sum(velocidad_kmh) / count(eventos válidos)` agrupado por `repartidor_id` y ventana temporal.
- **KPI 2 (Eventos con batería crítica):** Cantidad y porcentaje de eventos válidos con `bateria_pct <= 15` por repartidor y ventana.
- **KPI 3 (Desviaciones de ruta):** Cantidad y porcentaje de eventos válidos con `desvio_metros > 300` frente a la ruta de referencia (excluyendo del denominador los eventos sin `desvio_metros`).
- **Alerta 1 (Exceso de velocidad — `EXCESO_VELOCIDAD`):** Se activa por evento cuando `velocidad_kmh > 60`.
- **Alerta 2 (Batería crítica — `BATERIA_CRITICA`):** Se activa por evento cuando `bateria_pct <= 15`.

## Instalación y ejecución
Ver [docs/instalacion.md](docs/instalacion.md)

## Pruebas y CI
Para correr las pruebas automatizadas y validar la configuración en local:

```powershell
python -m pip install -r requirements.txt
python -m pytest tests/ -v
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config --quiet
```
## Documentación
- [Arquitectura](docs/arquitectura.md)
- [Instalación y ejecución](docs/instalacion.md)
- [Decisiones técnicas (ADR)](docs/adr/README.md)
- [Evidencias](docs/evidencias/README.md)
- [Guion de demostración](docs/demo.md)

## Release
v1.0.0