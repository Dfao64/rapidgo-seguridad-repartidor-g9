# rapidgo-seguridad-repartidor-g9

# RapidGo 360 - Grupo 9

Proyecto Final de Big Data
Universidad de Cundinamarca

## Variante #2: Seguridad del repartidor

Sistema de análisis de eventos para monitorear
condiciones operativas de los repartidores.

## Integrantes

- Daniel Abello
- Kevin Sánchez
- Deimer Hincapié

## Arquitectura prevista

Producer → Apache Kafka → Spark Structured
Streaming → Data Lake → Apache Airflow
→ KPIs y alertas

## Indicadores propuestos

1. Velocidad promedio por repartidor.
2. Eventos con batería crítica.
3. Desviaciones de ruta.

## Tecnologías

- Python
- Apache Kafka
- Apache Spark
- Apache Airflow
- Docker
- GitHub Actions

## Organización del repositorio

- producer/: generación de eventos.
- spark/: procesamiento streaming.
- airflow/dags/: orquestación.
- datalake/: almacenamiento por capas.
- samples/: ejemplos de eventos.
- tests/: pruebas automatizadas.
- docs/: arquitectura, decisiones y checkpoints.

## Releases

- Semana 1: v0.1.0-foundation
- Semana 2: v0.2.0-streaming
- Semana 3: v0.3.0-orchestration
- Semana 4: v1.0.0

## Estado actual

Semana 1: preparación de arquitectura,
contratos y estructura inicial.

Los componentes del pipeline aún no están
implementados.
