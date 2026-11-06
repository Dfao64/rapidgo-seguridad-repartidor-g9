# ADR-003 — Orquestación y recuperación del pipeline

**Estado:** Aceptada  
**Fecha:** 5 de noviembre de 2026  
**Issue:** #16 · **Release previsto:** `v0.3.0-orchestration`

## Contexto

El DAG inicial agrupaba fuente, calidad y reporte en tres tareas. La guía de Semana 3 exige al menos cinco tareas, trazabilidad entre etapas, reintentos y evidencia de recuperación. Además, la agregación debe excluir inválidos y cada ejecución debe conservar una salida auditable.

## Opciones consideradas

1. Mantener tres tareas grandes y documentar su operación.
2. Separar verificación del origen y auditoría de salida, manteniendo validación, agregación y reporte como etapas independientes.
3. Convertir Airflow en consumidor continuo de Kafka.

## Decisión

Se adopta la segunda opción. El DAG ejecuta en secuencia:

`verificar_fuente → validar_calidad → agregar_datos → generar_reporte → auditar_salida`

`verificar_fuente` revisa el esquema de cada Parquet y guarda en disco un manifiesto. `validar_calidad` aplica el contrato, escribe rechazos JSONL y produce un snapshot solo con filas válidas. `agregar_datos` consume ese snapshot; `generar_reporte` produce el JSON; `auditar_salida` compara los conteos del CSV y el JSON. XCom transporta únicamente rutas y metadatos pequeños; los datos permanecen en el volumen compartido.

Las tareas heredan dos reintentos, intervalo de 30 segundos y timeout de cinco minutos. La configuración `simular_fallo_temporal` existe solo para ejecutar una prueba controlada; no debe activarse en ejecuciones normales.

## Evidencia considerada

- Contrato de eventos y reglas vigentes en `docs/contrato-eventos.md` y `spark/reglas.py`.
- Pruebas del flujo con Parquet real, rechazos y reintento documentados en `docs/evidencias/semana3-kevin/` (primera versión de tres tareas).
- La validación final de las cinco tareas se ejecuta con Airflow 2.10.5 mediante `DAG.test` en el job `airflow_integration` de GitHub Actions; el log y los resultados se adjuntan como artefacto del workflow.

## Consecuencias

- La separación hace visibles los fallos de esquema y de reporte, y permite auditar consistencia al final del DAG.
- El snapshot evita que eventos inválidos afecten indicadores y evita que tareas posteriores relean el conjunto de origen.
- El procesamiento sigue siendo batch y carga Parquet en memoria; no es un consumidor Kafka ni un pipeline incremental.
- SQLite y `SequentialExecutor` son adecuados para esta demostración académica, no para operación productiva.
