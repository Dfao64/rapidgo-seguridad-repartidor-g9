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
Capturas: servicios Docker, DAG visible, Grid con tres tareas exitosas, logs de calidad y archivos de reporte. No afirmar ejecución correcta hasta probarlo localmente.

## Contrato de integración con Kevin
El DAG usa `PythonOperator`, no `TaskFlow`; las funciones aceptan contexto Airflow. XCom contiene exclusivamente metadatos pequeños (conteos y rutas). Kevin puede ampliar validaciones, reintentos y pruebas sin cambiar los nombres de tareas. El DAG se ejecuta manualmente para evitar leer archivos aún escritos por Spark. Para producción se requiere coordinación de snapshots/ventanas, no implementada en este prototipo.

## Limitaciones
Esta versión es una base académica de orquestación batch. Relee todos los Parquet de Processed en cada ejecución, y por ello no representa un procesamiento incremental. El servicio standalone utiliza SQLite, adecuado solo para demostración local. La instalación de dependencias mediante `_PIP_ADDITIONAL_REQUIREMENTS` requiere acceso a Internet al iniciar el contenedor.
