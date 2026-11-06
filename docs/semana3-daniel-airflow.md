# Semana 3 — Daniel: Airflow (Issue #16)

## Alcance
Se añade el DAG `rapidgo_seguridad_semanal` con dependencias **validar_calidad → agregar_datos → generar_reporte**. Consume Parquet de `datalake/processed` generado por Spark; produce CSV y JSON en `airflow/reports`. No sustituye ni modifica los jobs de Kevin/Deimer.

## Preparación
1. Verificar Docker Desktop y que `datalake/processed` contenga archivos `.parquet` con `event_id`, `repartidor_id`, `velocidad_kmh`, `bateria_pct`.
2. Desde la raíz del repositorio ejecutar:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow
   docker compose -f docker-compose.yml -f docker-compose.airflow.yml logs -f airflow
   ```
3. Airflow standalone genera credenciales de administrador en sus logs. Acceder a http://localhost:8080 y comprobar que aparece `rapidgo_seguridad_semanal`.
4. Activar y disparar manualmente el DAG (botón **Trigger DAG**). Verificar las tres tareas en verde en **Grid**.
5. Revisar `airflow/reports/agregados_YYYYMMDD.csv` y `airflow/reports/reporte_YYYYMMDD.json`.

## Evidencias exigidas
El archivo `evidence/semana3-airflow-pytest.txt` conserva la salida real de las pruebas ejecutadas en el entorno de validación. La prueba `test_parquet_real_desde_contrato_json` genera un Parquet con el ejemplo de `samples/evento_repartidor.json`, ejecuta las tres funciones y comprueba el CSV y el JSON.

En el entorno de validación utilizado para este cambio no están instalados Docker/Podman ni Airflow, por lo que no se generaron capturas de la interfaz Grid ni logs de un servicio Airflow. La prueba de funciones y Parquet no sustituye esa comprobación del contenedor. Antes de cerrar la validación del Issue #16 en el equipo, se debe ejecutar el procedimiento Docker descrito arriba y guardar capturas auténticas de Docker, Grid, logs y reportes.

## Contrato de integración con Kevin
El DAG usa `PythonOperator`, no `TaskFlow`; las funciones aceptan contexto Airflow. XCom contiene exclusivamente metadatos pequeños (conteos y rutas). Kevin puede ampliar validaciones, reintentos y pruebas sin cambiar los nombres de tareas. El DAG se ejecuta manualmente para evitar leer archivos aún escritos por Spark. Para producción se requiere coordinación de snapshots/ventanas, no implementada en este prototipo.

## Limitaciones
Esta versión es una base académica de orquestación batch. Relee todos los Parquet de Processed en cada ejecución, y por ello no representa un procesamiento incremental. El servicio standalone utiliza SQLite, adecuado solo para demostración local. La instalación de dependencias mediante `_PIP_ADDITIONAL_REQUIREMENTS` requiere acceso a Internet al iniciar el contenedor.

## Validación local de esta revisión
- Suite completa: `74 passed, 2 skipped`.
- Prueba específica de Airflow/DAG: `7 passed`.
- La prueba de integración escribe y lee Parquet real usando `pyarrow`; no requiere un proceso Kafka ni Spark.
- El DAG no fue importado por una instalación real de Airflow y no se ejecutó en Docker porque esos motores no están disponibles en el entorno.
