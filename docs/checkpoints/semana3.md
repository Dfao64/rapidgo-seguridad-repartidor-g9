# Checkpoint Semana 3

**RapidGo 360 · Grupo 9 · Variante 2: Seguridad del repartidor**  
**Responsable:** Daniel Abello · **Fecha:** 5 de noviembre de 2026

## Qué quedó funcionando

El DAG `rapidgo_seguridad_semanal` completa cinco etapas: `verificar_fuente → validar_calidad → agregar_datos → generar_reporte → auditar_salida`. Comprueba el esquema de cada Parquet; separa inválidos en JSONL; crea un snapshot de válidos; genera agregados CSV y reporte JSON; y contrasta sus conteos. XCom transporta metadatos pequeños, no los registros.

## Evidencia y recuperación

La prueba de Airflow 2.10.5 real (`DAG.test`) verifica las cinco tareas, usando un Parquet sintético de dos eventos: uno inválido (batería 101) y uno válido. La validación rechaza el primero y la agregación usa solo el segundo. Se configura un fallo transitorio: el primer intento de `validar_calidad` queda `UP_FOR_RETRY`; el reintento y las tareas siguientes terminan correctamente. `auditar_salida` verifica que los conteos del CSV y el JSON coincidan. La integración ampliada se ejecuta en GitHub Actions y conserva log, JSON, rechazos y reporte como artefacto.

La ejecución Docker capturada en el PR #39 corresponde a la versión anterior de tres tareas; no se presenta como evidencia de la versión de cinco tareas. La nueva comprobación usa Airflow real en CI como alternativa ejecutable al Docker local no disponible en este entorno.

## Decisión y pendientes de liberación

El ADR-003 separa inspección de esquema, calidad, agregación, reporte y auditoría. El snapshot por ejecución evita incluir rechazados en los KPIs; se mantienen dos reintentos de 30 segundos para fallos transitorios. El procesamiento continúa siendo batch, no integra el flujo completo Kafka → Spark → Airflow.

El release/tag `v0.3.0-orchestration` se publica después de que este PR sea revisado y fusionado. No se marca el Issue #16 como cerrado antes de ese paso.

**Referencias:** [Issue #16](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/issues/16) · [PR de implementación](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/pull/40) · [Acciones y artefacto de integración](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions)
