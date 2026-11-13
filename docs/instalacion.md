# Instalación y ejecución

## Prerrequisitos
- **Docker Desktop** con soporte para Docker Compose v2 (contenedores Linux activos).
- **Python 3.11 a 3.13** y `pip`.
- **Git** instalado.
- Puertos locales `9092` (Kafka) y `8080` (interfaz web de Apache Airflow) disponibles.

## Levantar el entorno
1. Crear el entorno virtual e instalar dependencias en la raíz del repositorio:
```powershell
cd rapidgo-seguridad-repartidor-g9
py -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -r requirements.txt
```

2. Iniciar los contenedores de Kafka, Spark y Apache Airflow:
```powershell
docker-compose up -d
docker-compose -f docker-compose.airflow.yml up -d
docker-compose ps
docker-compose -f docker-compose.airflow.yml ps
```

3. Crear y verificar el topic de Kafka con 3 particiones:
```powershell
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic rapidgo.seguridad.repartidor.v1 --partitions 3 --replication-factor 1
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic rapidgo.seguridad.repartidor.v1
```

## Ejecutar el pipeline
1. **Terminal 1 — Iniciar el job de Spark Structured Streaming** (dejar en ejecución):
```powershell
docker-compose exec -T spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/spark/datalake.py --datalake /workspace/datalake --ventana '1 minute' --watermark '1 minute'
```

2. **Terminal 2 — Ejecutar el Producer para publicar eventos en Kafka:**
```powershell
.\.venv\Scripts\python -m producer --bootstrap-servers 127.0.0.1:9092 --rounds 60
```
*(Esperar entre 1 y 2 minutos tras finalizar el Producer para que el watermark cierre la ventana y Spark escriba los archivos Parquet en `datalake/raw`, `datalake/processed` y `datalake/curated`).*

3. **Ejecutar el DAG en Apache Airflow:**
   - Consultar la contraseña del usuario `admin`:
     ```powershell
     docker exec -it rapidgo360-airflow-1 cat /opt/airflow/standalone_admin_password.txt
     ```
   - Abrir `http://localhost:8080`, iniciar sesión con `admin`, activar el DAG **`rapidgo_seguridad_semanal`** y pulsar **Trigger DAG**.
   - Confirmar que las 5 tareas (`verificar_fuente`, `validar_calidad`, `agregar_datos`, `generar_reporte`, `auditar_salida`) finalicen en estado `success` (verde).

4. **Ejecución automatizada End-to-End con lote controlado (opcional):**
```powershell
.\.venv\Scripts\python scripts/semana4_e2e.py --timeout 300
```

## Correr las pruebas
```powershell
.\.venv\Scripts\python -m pytest tests/ -v
.\.venv\Scripts\python -m pytest tests/test_semana4_integracion.py -v
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config --quiet
```