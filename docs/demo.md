# Guion de demostración

1. **Verificar contenedores activos de Kafka, Spark y Airflow**
   - Comando:
     ```powershell
     docker compose ps
     docker compose -f docker-compose.airflow.yml ps
     ```
   - Resultado esperado: Los contenedores `kafka`, `spark` y `airflow` aparecen en estado `Up` / `running`.

2. **Verificar el topic de Kafka y sus 3 particiones (ADR-001)**
   - Comando:
     ```powershell
     docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic rapidgo.seguridad.repartidor.v1
     ```
   - Resultado esperado: La consola muestra el topic `rapidgo.seguridad.repartidor.v1` con `PartitionCount: 3` (particiones 0, 1 y 2).

3. **Iniciar el procesamiento en Spark Structured Streaming (Terminal 1)**
   - Comando:
     ```powershell
     docker compose exec -T spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/spark/datalake.py --datalake /workspace/datalake --ventana '1 minute' --watermark '1 minute'
     ```
   - Resultado esperado: Spark inicia la sesión de streaming y queda escuchando nuevos eventos del topic de Kafka.

4. **Publicar eventos simulados con el Producer (Terminal 2)**
   - Comando:
     ```powershell
     .\.venv\Scripts\python -m producer --bootstrap-servers 127.0.0.1:9092 --rounds 60
     ```
   - Resultado esperado: Se envían 180 eventos a Kafka distribuidos por `repartidor_id`. Tras esperar 1 a 2 minutos por el cierre del *watermark*, Spark vuelca los datos al Data Lake.

5. **Comprobar la escritura de archivos Parquet en las 3 capas del Data Lake**
   - Comando:
     ```powershell
     Get-ChildItem datalake -Recurse -Directory | Select-Object -ExpandProperty FullName
     (Get-ChildItem datalake -Recurse -Filter *.parquet | Measure-Object).Count
     ```
   - Resultado esperado: Se listan los directorios `datalake/raw`, `datalake/processed` y `datalake/curated` con un conteo de archivos `.parquet` mayor a cero.

6. **Ejecutar el DAG `rapidgo_seguridad_semanal` en Apache Airflow (ADR-003)**
   - Acción: Ingresar a `http://localhost:8080`, mostrar los runs previos fallidos (evidencia de reintentos automáticos cuando `datalake/processed` estaba vacío) y ejecutar un run con **Trigger DAG** sobre el Data Lake poblado.
   - Resultado esperado: Las 5 tareas (`verificar_fuente`, `validar_calidad`, `agregar_datos`, `generar_reporte`, `auditar_salida`) finalizan en color **verde (`success`)** y se generan `manifest_fuentes.json`, `agregados.csv` y `reporte.json`.

7. **Demostrar las pruebas automatizadas y el Quality Gate en CI**
   - Comando:
     ```powershell
     .\.venv\Scripts\python -m pytest tests/ -v
     ```
   - Resultado esperado: Todos los tests unitarios y de integración (incluyendo el lote controlado de 4 eventos con velocidad media = 50 km/h, batería crítica = 50 %, desvío = 33.33 % y alertas `EXCESO_VELOCIDAD` / `BATERIA_CRITICA`) pasan en verde, al igual que el workflow `CI RapidGo` en la pestaña **Actions** de GitHub.