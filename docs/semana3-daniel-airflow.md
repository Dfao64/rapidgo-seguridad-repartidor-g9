# Semana 3 — Daniel: Airflow (Issue #16)

## Alcance
El DAG `rapidgo_seguridad_semanal` contiene cinco tareas secuenciales: **verificar_fuente → validar_calidad → agregar_datos → generar_reporte → auditar_salida**. Consume Parquet de `datalake/processed` generado por Spark; produce manifiesto de fuentes, snapshot válido, JSONL de rechazos, CSV agregado y reporte JSON en `airflow/reports`. No sustituye ni modifica los jobs de Kafka/Spark de Kevin y Deimer.

## Preparación
1. Verificar Docker Desktop y que `datalake/processed` contenga Parquet con el esquema del contrato (`event_id`, `repartidor_id`, `pedido_id`, `timestamp`, GPS, velocidad, batería y estado).
2. Desde la raíz del repositorio ejecutar:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow
   docker compose -f docker-compose.yml -f docker-compose.airflow.yml logs -f airflow
   ```
3. Airflow standalone genera credenciales de administrador en sus logs. Acceder a http://localhost:8080 y comprobar que aparece `rapidgo_seguridad_semanal`.
4. Activar y disparar manualmente el DAG (botón **Trigger DAG**). Verificar las cinco tareas en verde en **Grid**.
5. Revisar el directorio de salida de la ejecución dentro de `airflow/reports/`; contiene manifiesto, snapshot de válidos, JSONL de rechazados, CSV agregado y reporte JSON.

## Evidencias exigidas
Capturas: servicios Docker, DAG visible, Grid con tres tareas exitosas, logs de calidad y archivos de reporte. No afirmar ejecución correcta hasta probarlo localmente.

La salida de las pruebas ejecutadas en este entorno está en `evidence/semana3-airflow-pytest.txt`. La prueba de integración usa Parquet real creado con el evento de `samples/evento_repartidor.json`; las pruebas adicionales verifican rechazo y cuarentena ante valores fuera de tipo/rango.

En el entorno de validación no estaban instalados Docker/Podman ni Airflow. Por eso no se generaron capturas de Grid ni logs de un servicio Airflow; las pruebas locales no sustituyen esa comprobación. Ejecutar el procedimiento Docker en un equipo con Docker y guardar esas evidencias auténticas antes de cerrar el Issue #16.

## Contrato de integración con Kevin
El DAG usa `PythonOperator`, no `TaskFlow`; las funciones aceptan contexto Airflow. `verificar_fuente` inspecciona el esquema de cada Parquet y guarda un manifiesto; `validar_calidad` lee solo los archivos listados allí, clasifica inválidos y conserva un snapshot válido; `agregar_datos` consume solo el snapshot; `generar_reporte` escribe JSON; `auditar_salida` contrasta los conteos del CSV con el reporte. XCom transporta solo rutas y metadatos pequeños. El DAG se dispara manualmente para evitar leer archivos que Spark aún esté escribiendo. Para producción se requiere coordinación de snapshots/ventanas, no implementada en este prototipo.

## Limitaciones
Esta versión es una base académica de orquestación batch. El DAG valida el contrato, aparta registros inválidos, conserva un snapshot de válidos y calcula reportes sobre ese snapshot. El servicio standalone utiliza SQLite, adecuado solo para demostración local. La instalación de dependencias mediante `_PIP_ADDITIONAL_REQUIREMENTS` requiere acceso a Internet al iniciar el contenedor.

## Validación de cinco tareas
`python scripts/probar_airflow_kevin.py` ejecuta el DAG con Airflow 2.10.5 real mediante `DAG.test`, Parquet sintético y fallo temporal configurado. Comprueba que las cinco tareas terminan en `success`, que `validar_calidad` reintenta, que los rechazos no llegan a los indicadores y que `auditar_salida` confirma conteos consistentes. CI debe ejecutar esta prueba en Python 3.12 junto con toda la suite del proyecto.

La ejecución previa en Docker (PR #39) verificó tres tareas. No se reutiliza como evidencia del nuevo DAG de cinco tareas. La nueva ejecución de integración valida el código actualizado; el scheduler/UI de Docker debe comprobarse después de integrar esta versión.
