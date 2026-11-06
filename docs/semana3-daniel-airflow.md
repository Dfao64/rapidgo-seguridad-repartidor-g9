# Semana 3 — Daniel: Airflow (Issue #16)

## Alcance
Se añade el DAG `rapidgo_seguridad_semanal` con dependencias **validar_calidad → agregar_datos → generar_reporte**. Consume Parquet de `datalake/processed` generado por Spark; produce CSV y JSON en `airflow/reports`. No sustituye ni modifica los jobs de Kevin/Deimer.

## Preparación
1. Verificar Docker Desktop y que `datalake/processed` contenga Parquet con el esquema del contrato (`event_id`, `repartidor_id`, `pedido_id`, `timestamp`, GPS, velocidad, batería y estado).
2. Desde la raíz del repositorio ejecutar:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow
   docker compose -f docker-compose.yml -f docker-compose.airflow.yml logs -f airflow
   ```
3. Airflow standalone genera credenciales de administrador en sus logs. Acceder a http://localhost:8080 y comprobar que aparece `rapidgo_seguridad_semanal`.
4. Activar y disparar manualmente el DAG (botón **Trigger DAG**). Verificar las tres tareas en verde en **Grid**.
5. Revisar el directorio de salida de la ejecución dentro de `airflow/reports/`; el DAG conserva un snapshot de válidos, un archivo JSONL de rechazados, el CSV agregado y el reporte JSON.

## Evidencias exigidas
Capturas: servicios Docker, DAG visible, Grid con tres tareas exitosas, logs de calidad y archivos de reporte. No afirmar ejecución correcta hasta probarlo localmente.

La salida de las pruebas ejecutadas en este entorno está en `evidence/semana3-airflow-pytest.txt`. La prueba de integración usa Parquet real creado con el evento de `samples/evento_repartidor.json`; las pruebas adicionales verifican rechazo y cuarentena ante valores fuera de tipo/rango.

En el entorno de validación no estaban instalados Docker/Podman ni Airflow. Por eso no se generaron capturas de Grid ni logs de un servicio Airflow; las pruebas locales no sustituyen esa comprobación. Ejecutar el procedimiento Docker en un equipo con Docker y guardar esas evidencias auténticas antes de cerrar el Issue #16.

## Contrato de integración con Kevin
El DAG usa `PythonOperator`, no `TaskFlow`; las funciones aceptan contexto Airflow. XCom contiene exclusivamente metadatos pequeños (conteos y rutas). Kevin puede ampliar validaciones, reintentos y pruebas sin cambiar los nombres de tareas. El DAG se ejecuta manualmente para evitar leer archivos aún escritos por Spark. Para producción se requiere coordinación de snapshots/ventanas, no implementada en este prototipo.

## Limitaciones
Esta versión es una base académica de orquestación batch. El DAG valida el contrato, aparta registros inválidos, conserva un snapshot de válidos y calcula reportes sobre ese snapshot. El servicio standalone utiliza SQLite, adecuado solo para demostración local. La instalación de dependencias mediante `_PIP_ADDITIONAL_REQUIREMENTS` requiere acceso a Internet al iniciar el contenedor.

## Validación local de esta revisión
- Suite completa: ver resultado exacto en `evidence/semana3-airflow-pytest.txt`.
- Se prueban lectura de Parquet real, agregación, reporte y cuarentena de registros inválidos.
- El DAG no se ejecutó como servicio Airflow/Docker porque esos motores no estaban disponibles en el entorno.
