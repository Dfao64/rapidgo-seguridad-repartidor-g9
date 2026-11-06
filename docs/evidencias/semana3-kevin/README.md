# Evidencias verificadas — Kevin — Semana 3

Base: main `f77bb397dc4104e62376748246f6f90d643bad88` (PR #34, aprobado e integrado).

- `pytest.txt`: salida completa de python -m pytest tests/ -v, con SPARK_LOCAL_IP=127.0.0.1. Incluye pruebas anteriores y nuevas; no se omiten pruebas de Spark.
- `airflow-integracion.log`: Airflow **2.10.5 real**, Python 3.12, SQLite, prueba mediante DAG.test. Registra excepción controlada, estado UP_FOR_RETRY, espera de 30 segundos, segundo intento exitoso, validación, agregación y reporte.
- `airflow-integracion.json`: consulta real de TaskInstance y XCom persistidos. Tres tareas success; validar_calidad try_number=2; 2 procesados, 1 inválido, 1 incluido en indicadores.
- `rechazados-ejemplo.jsonl`: registro sintético rechazado por batería 101.
- `reporte-ejemplo.json`: reporte real del mismo flujo.
- `SHA256SUMS.txt`: hashes de evidencias, código y script probados.

Las rutas /tmp de los JSON documentan los destinos reales del ensayo; los directorios temporales se eliminan al finalizar. Las copias de reporte y rechazos permanecen en esta carpeta.

## Pendiente de verificación

No hay Docker CLI ni socket Docker en este entorno. No se ejecutó el Compose completo RapidGo + Airflow, ni una ejecución del scheduler con Parquet real de Spark. Se verificó el contrato con Parquet sintético real y todas las pruebas anteriores de Spark.

No se adjuntan capturas de la interfaz: no fue posible obtener un navegador local (falló la descarga de Chromium). Los logs y metadatos son evidencias textuales reales, no capturas fabricadas. La guía `docs/semana3-kevin-validaciones.md` indica los pasos para completar las capturas y la prueba Docker. Revisión y merge pendientes de un colaborador; no se declara el Issue #17 totalmente completado mientras falten esas comprobaciones.
