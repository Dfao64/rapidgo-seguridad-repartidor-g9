
# Checkpoint — Semana 1

**Proyecto:** RapidGo 360 — Grupo 9  
**Variante:** #2 — Seguridad del repartidor  
**Release objetivo:** v0.1.0-foundation  
**Integrantes:** Daniel Abello, Kevin Sánchez y Deimer Hincapié

## 1. Avances

- Repositorio público y Kanban de cinco estados.
- Planificación de 12 Issues para cuatro semanas.
- Estructura inicial del proyecto, README, dependencias y Docker Compose preliminar.
- Arquitectura documentada: Producer → Kafka → Spark Structured Streaming → Data Lake → Airflow.
- Contrato JSON v1 con evento de ejemplo y reglas de validación.
- Tres KPIs: velocidad promedio, batería crítica y desviaciones de ruta.
- Dos alertas: velocidad superior a 60 km/h y batería menor o igual al 15 %.
- Pull Requests #22, #23 y #24 integrados a main.

## 2. Decisiones técnicas

- Topic Kafka: `rapidgo.seguridad.repartidor.v1`.
- Clave de partición: `repartidor_id`, para conservar el orden relativo de eventos por repartidor.
- Tres particiones iniciales para permitir paralelismo.
- Umbral de desviación: más de 300 metros.
- Almacenamiento previsto: Raw, Processed y Curated.
- Los umbrales son didácticos y se validarán durante la simulación.

## 3. Riesgo principal

El KPI de desviación de ruta requiere una ruta de referencia simulada. Sin ella no es posible calcular correctamente `desvio_metros`.

**Mitigación:** definir la ruta de referencia con el Producer y validar los cálculos durante la Semana 2.

## 4. Plan para Semana 2

- Daniel: implementar el Producer de eventos simulados.
- Kevin: integrar Kafka con Spark Structured Streaming.
- Deimer: implementar Data Lake, alertas, pruebas y GitHub Actions.
- Equipo: comprobar el flujo de eventos y documentar evidencias.

**Estado:** Fundación documental preparada; implementación y pruebas del pipeline pendientes.
