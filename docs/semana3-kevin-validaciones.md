# Semana 3 — Kevin Sánchez — Issue #17

Base revisada: PR #34 aprobado por Deimer3 e integrado en main, commit `f77bb397dc4104e62376748246f6f90d643bad88`.

## Calidad e indicadores
Se conserva el DAG `rapidgo_seguridad_semanal` y sus tres tareas: validar_calidad → agregar_datos → generar_reporte.
Cada Parquet debe tener event_id, repartidor_id, pedido_id, timestamp, gps, velocidad_kmh, bateria_pct y estado. Se comprueba el esquema de **cada archivo**, evitando ocultar columnas ausentes al concatenar.

Los identificadores deben ser cadenas no vacías. Timestamp debe ser ISO 8601 con hora y zona (Z u offset); se valida también el calendario. GPS usa el struct del contrato: gps.latitud [-90,90] y gps.longitud [-180,180]. Velocidad debe ser finita y >=0; batería finita y entre 0 y 100. No se aceptan booleanos como números. Estados permitidos: EN_RUTA, ENTREGADO, PAUSADO.

Todas las apariciones de event_id duplicado se rechazan para no elegir arbitrariamente entre eventos conflictivos. `rechazados.jsonl` conserva los registros con motivos estables. Si quedan válidos, el flujo continúa con `estado=con_rechazos`; si todos son inválidos, falla y bloquea las tareas dependientes. El origen permanece intacto.

Se escribe un snapshot `validos.parquet` por run_id (directorio hash). La agregación lee **solo ese snapshot**, nunca relee Processed: los registros rechazados no alimentan indicadores y las ejecuciones del mismo día no comparten archivos. Los rechazos y snapshots contienen datos del lote: conservarlos bajo los mismos controles de acceso y retención que el Data Lake. El hash identifica la ejecución, no anonimiza los eventos.

## XCom
Solo retorna diccionarios pequeños: conteos, rutas y estado. El dataset queda en disco compartido. `validar_calidad`: archivos, procesados, invalidos, eventos válidos, ruta_validos, ruta_rechazos, estado. `agregar_datos` añade ruta del CSV y repartidores. `generar_reporte` retorna ruta del JSON, estado y conteo. No se transmiten DataFrames ni listas de eventos por XCom.

## Reintentos y fallos
Todas las tareas heredan 2 reintentos (hasta 3 intentos), intervalo fijo de 30 segundos y execution_timeout de 5 minutos **por intento**. Fallo definitivo invoca callback con DAG, tarea, run y excepción en logs; dependientes usan all_success y quedan bloqueadas. No se envían notificaciones externas.

Sin Parquet o lote vacío: ValueError claro. Columnas ausentes: archivo y columnas en log. Lectura corrupta: OSError con archivo y causa original. Inválidos: conteos y ruta de auditoría, fallo si no queda ningún válido. Error de agregación/escritura: traceback y contexto, propagado para reintento. Un fallo de esquema persistente también consume los reintentos; requiere corregir el origen y lanzar otra ejecución.

Para demostrar un fallo transitorio, configurar el DAG run con `{"simular_fallo_temporal": true}`: falla la primera tentativa de validar_calidad y permite las siguientes. No activarlo en ejecuciones normales.

## Pruebas reproducibles
```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```
En un entorno que resuelva el hostname a una dirección no disponible usar `SPARK_LOCAL_IP=127.0.0.1`. Las pruebas de Daniel se adaptan al contrato ampliado y utilizan Parquet real en el flujo exitoso. Las nuevas pruebas cubren reglas, límites, duplicados, rechazos fuera de KPI, esquemas por archivo, corrupción, lote vacío, metadatos ausentes y fallo de agregación. Los stubs de Airflow de las pruebas unitarias **no prueban el scheduler**.

Para integración con Airflow 2.10.5 real, en un entorno aislado:
```bash
export AIRFLOW_HOME="$(mktemp -d)"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
airflow db migrate
python scripts/probar_airflow_kevin.py > docs/evidencias/semana3-kevin/airflow-integracion.log 2>&1
```
El script usa datos sintéticos y directorios temporales; prueba las tres tareas con `DAG.test`, comprueba reintento real, estados y XCom, y guarda JSON y logs. No sustituye la prueba del scheduler ni la interfaz Docker.

## Verificación Docker y capturas
Desde la raíz, con Docker Desktop activo y Parquet real de Spark:
```bash
mkdir -p airflow/reports
# Dar al UID del contenedor permisos de escritura en airflow/reports.
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
docker compose -f docker-compose.yml -f docker-compose.airflow.yml logs -f airflow
```
Abrir http://localhost:8080 con las credenciales de standalone de los logs. Localizar el DAG, activarlo y disparar una ejecución con la configuración de fallo temporal. Capturar Graph/Grid con tres tareas, estado up_for_retry y log del primer intento, segundo intento exitoso, XCom return_value de cada tarea y JSON de reporte. Para probar rechazos agregar un evento con batería 101 a un Parquet de prueba aislado y comprobar invalidos y rechazados.jsonl. Nunca modificar el Data Lake real para una demostración.

## Límites
Pipeline batch académico: lee todos los Parquet en memoria, no incremental; requiere que Spark haya terminado de escribir el lote. No se implementa coordinación transaccional con Spark. SQLite y SequentialExecutor son para demostración. Los resultados y estados realmente comprobados se registran en `docs/evidencias/semana3-kevin/README.md`; no se presentan capturas simuladas.
