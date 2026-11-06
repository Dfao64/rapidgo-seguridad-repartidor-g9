# Checkpoint Semana 3

**RapidGo 360 · Grupo 9 · Variante 2: Seguridad del repartidor**  
**Responsable del componente:** Daniel Abello · **Fecha:** 5 de noviembre de 2026

## Qué quedó funcionando

El DAG `rapidgo_seguridad_semanal` ejecuta en secuencia `validar_calidad → agregar_datos → generar_reporte`. Valida el contrato del evento, separa los registros rechazados en JSONL, crea un snapshot Parquet de los válidos y genera agregados y un reporte JSON. El intercambio entre tareas usa metadatos pequeños en XCom.

## Evidencia de ejecución

Con Airflow 2.10.5, la prueba real `DAG.test` terminó con las tres tareas en `success`. En la prueba controlada se procesaron 2 eventos: 1 inválido y 1 válido; el evento inválido quedó en cuarentena y 1 repartidor llegó a los indicadores. `validar_calidad` falló en el primer intento (`UP_FOR_RETRY`) y pasó en el segundo. En la verificación guiada con Docker se observaron también las tres tareas exitosas y la recuperación del reintento. En una prueba separada Spark generó Parquet y Airflow procesó 1 evento válido. Estas pruebas no demuestran el flujo completo de Kafka.

La suite del repositorio registró **99 pruebas aprobadas** y el workflow de GitHub Actions **#41 terminó en verde**, incluidas sintaxis, pruebas y validación de Docker Compose. Las evidencias de logs, datos, reporte y capturas están en [`docs/evidencias/semana3-kevin/`](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/tree/main/docs/evidencias/semana3-kevin). La captura de trabajo se integró mediante los PR #34, #38 y #39; el PR #39 recibió aprobación antes del merge.

## Decisión técnica y problema resuelto

La agregación lee el snapshot validado de cada ejecución, no el directorio Processed completo; así los datos rechazados no alimentan los KPIs. XCom transporta conteos, estado y rutas, no DataFrames. Para probar resiliencia se provocó un `OSError` temporal en el primer intento; Airflow marcó `UP_FOR_RETRY` y la tarea concluyó correctamente al reintentarse. Un evento sintético con batería `101` fue rechazado con motivo `bateria_pct_invalido`.

## Pendientes frente a la guía

El DAG implementa **3 tareas**, mientras la guía de Semana 3 pide mínimo **5**. No se encontró ADR-003 ni el release/tag `v0.3.0-orchestration`, y el Issue #16 sigue abierto. Por tanto, el flujo de 3 etapas y su resiliencia están probados, pero estos elementos deben completarse antes de declarar cerrado el gate semanal.

**Referencias:** [Issue #16](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/issues/16) · [PR #34](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/pull/34) · [PR #38](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/pull/38) · [PR #39](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/pull/39) · [GitHub Actions #41](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions/runs/42158646647)
