# Guía para Kevin — Semana 2, Issue #14

Objetivo: demostrar Producer → Kafka → Spark Structured Streaming. Esta guía
es un plan de implementación; el streaming de Kevin no se declara ejecutado
por el PR de Daniel. Deimer conserva sus reglas, KPIs, Data Lake y CI.

## 1. Preparar rama y revisar acuerdos

```powershell
git switch main
git pull --ff-only
git switch -c feature/kevin-semana2-kafka-spark
```

Trabajar con cambios locales guardados. Leer Issue #14, `docs/contrato-eventos.md`,
`docs/decisions/ADR-001-kafka-particiones.md`, `spark/reglas.py`,
`spark/indicadores.py`, `datalake/README.md` y `producer/README.md`.
Esperar la aprobación e integración del PR de Daniel antes de usar su Producer
desde main. Si se necesita probar antes, acordar una rama de integración sin
fusionar ni copiar trabajo ajeno silenciosamente.

Acuerdos: topic `rapidgo.seguridad.repartidor.v1`, 3 particiones, clave
`repartidor_id`, valores JSON UTF-8. Un broker académico puede usar réplica 1.
No aumentar particiones durante una prueba de orden.

## 2. Implementar el Compose del equipo

Editar `docker-compose.yml` en tu rama. Incorporar Kafka en KRaft y Spark;
fijar versiones de las imágenes. Usar la imagen oficial de Kafka y tomar como
referencia `producer/compose.test.yml`, sin reemplazar ese entorno temporal.
Mantener nombre de proyecto distinto (`rapidgo360`), red y almacenamiento propios.
Para Spark se puede usar `apache/spark:3.5.7-python3` si la etiqueta está disponible;
verificarla al descargar y registrar el digest real. Elegir Spark/Scala/Java/Python
compatibles y documentar las versiones que efectivamente se ejecuten.

Configurar dos listeners de broker y uno de controlador:

| Uso | Dirección anunciada de ejemplo |
| --- | --- |
| Producer desde Windows/host | `127.0.0.1:9092` |
| Spark y herramientas dentro de la red Docker | `kafka:29092` |
| Controlador KRaft | `kafka:9093` (no es bootstrap de clientes) |

Mapear host 9092 al listener externo; el interno puede escuchar 29092.
Configurar `listener.security.protocol.map`, `inter.broker.listener.name`,
`controller.listener.names` y quorum KRaft coherentes. Con un nodo establecer
replicación de offsets y transacciones en 1 y min ISR transaccional en 1.
Desactivar autocreación de topics. Añadir healthcheck que consulte el listener
interno y dependencias que esperen Kafka saludable.

Montar `spark/` en el contenedor, trabajar desde la raíz montada del proyecto y
conservar los directorios de Deimer. Usar checkpoint persistente y específico
por consulta, sin compartir un checkpoint entre consultas. No borrar datos
del equipo para resolver errores de esquema. El entorno temporal de Daniel
usa 19092 y no debe incorporarse al Compose del equipo.

```powershell
docker compose config --quiet
docker compose up -d --wait
docker compose ps
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic rapidgo.seguridad.repartidor.v1 --partitions 3 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic rapidgo.seguridad.repartidor.v1
```

Guardar salida con PartitionCount 3, líderes e ISR. `--if-not-exists` no corrige
un topic creado con otra configuración: inspeccionarlo y acordar corrección
sin borrarlo si tiene datos útiles.

## 3. Añadir el consumidor continuo en un archivo nuevo

Crear, por ejemplo, `spark/streaming.py`, sin sobrescribir `reglas.py` ni
`indicadores.py`. Parámetros configurables: bootstrap, topic, checkpoint y
destino de salida. Para Spark 3.5.7 con Scala 2.12, lanzar con:

```text
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/spark/streaming.py
```

Adaptar `/workspace` al montaje real y documentar el comando completo de
`docker compose exec -T <servicio-spark> ...`. El conector no viene por instalar
`kafka-python`; debe coincidir con Spark y el binario de Scala.

La consulta debe usar `spark.readStream.format("kafka")`,
`kafka.bootstrap.servers=kafka:29092`, `subscribe` con el topic acordado y
`startingOffsets=earliest` en la primera prueba con checkpoint nuevo. Con un
checkpoint existente, Spark recupera sus offsets; cambiar startingOffsets no
reinicia la lectura.

Conservar `topic`, `partition`, `offset`, timestamp Kafka, clave original y
valor original. Convertir `key`/`value` a string. Definir un `StructType`
explícito para `from_json`:

| Campo | Tipo Spark |
| --- | --- |
| event_id, repartidor_id, pedido_id, timestamp, estado | StringType |
| gps | StructType con latitud y longitud DoubleType |
| velocidad_kmh, bateria_pct, desvio_metros | DoubleType |

Conservar timestamp original y convertirlo adicionalmente a tiempo UTC;
establecer sesión UTC. Validar formato exacto `yyyy-MM-dd'T'HH:mm:ss'Z'` y
fecha real, no solo una coincidencia textual. Incluir registro corrupto o
detectar JSON nulo/malformado y campos mal tipados; probar estos casos.

## 4. Validar sin cambiar el contrato de Deimer

Separar válidos y descartes con motivo; conservar payload original en ambos
para auditoría y coordinar con Deimer el destino Raw. Campos obligatorios:
IDs no vacíos, timestamp UTC válido, gps con latitud [-90,90] y longitud
[-180,180], velocidad >=0, batería [0,100], estado entre EN_RUTA, ENTREGADO y
PAUSADO. Rechazar tipos incorrectos, booleanos en numéricos, NaN/infinito y
valores no finitos. La clave Kafka debe coincidir con repartidor_id.

`desvio_metros` ausente/null sigue siendo válido y se excluye **solo del KPI 3**;
si aparece debe ser numérico >=0. No imputar ceros. Duplicados por event_id
se conservan una sola vez para KPIs/alertas. Definir con Deimer si se necesita
deduplicación histórica o una ventana limitada; una marca de agua limita el
estado y no garantiza deduplicación eterna.

Usar las reglas actuales como referencia y agregar pruebas del parsing Spark.
No recolectar un stream ilimitado al driver ni reemplazar el consumidor por
lectura batch. Usar `writeStream`, trigger por ejemplo cada 5 segundos y
`awaitTermination()`. Para demostración puede usarse console con truncate=false;
coordinar persistencia con Deimer antes de escribir en sus capas.

## 5. Probar la integración

Primero iniciar la consulta y comprobar que permanece activa. Luego, desde
host con el Producer instalado:

```powershell
python -m producer --bootstrap-servers 127.0.0.1:9092 --interval 5 --riders 3 --rounds 12 --seed 42 --start-time 2026-10-30T14:00:00Z --run-id kevin-integracion-01
```

Esperar 36 ACK del Producer. Registrar los event_id y verificar que Spark ve
los mismos 36 IDs válidos, 3 repartidores y ningún descarte en esta corrida.
Cruzar topic/partition/offset entre ambas salidas. El orden exigido es por
repartidor, no global; varias claves pueden caer en una misma partición.
Una consulta puede emitir microbatches vacíos: registrar varios con datos y
sus `numInputRows`, batchId y offsets. Publicar después una segunda corrida
con run-id distinto y comprobar que la consulta sigue consumiendo sin reinicio.

Pruebas adicionales en topic/entorno de prueba independiente:

1. Evento válido del sample y extremos batería 0/100, velocidad 0 y GPS límite.
2. JSON malformado, campo obligatorio faltante, tipo incorrecto, estado inválido,
   fecha imposible y valores fuera de rango: todos van a descartes con motivo.
3. Desvío ausente/null: evento válido, excluido solo de KPI 3.
4. Repetir event_id: no duplicar cómputo de KPIs/alertas.
5. Reiniciar la consulta con el mismo checkpoint y publicar nuevos IDs: retoma
   offsets y procesa nuevos datos. No prometer exactly-once con un sink console;
   documentar semántica del sink definitivo y pruebas de recuperación.
6. Mantener los tests de Deimer y agregar tests propios para parseo, claves,
   descartes y recuperación. Ejecutar suite y validación Compose.

No inyectar JSON inválido en el topic del equipo sin aislar la prueba. No usar
el verificador de Daniel para contar corridas distintas: espera una semilla,
inicio y run-id específicos y está diseñado para su broker temporal.

## 6. Guardar evidencia verificable

Crear `docs/evidencias/semana2-kevin/` con fecha, commit, versiones y comandos:
Compose validado, servicios saludables, topic descrito, logs Producer con ACK,
logs de varios microbatches Spark, tabla de conteos/IDs/descartes, prueba de
reinicio y salida de tests. Añadir capturas si las pide la universidad, junto
con logs de texto que permitan verificar resultados. No incluir secretos ni
datasets reales. Distinguir pruebas ejecutadas de las pendientes.

## 7. PR y revisión

```powershell
git status --short
git diff --check
python -m pytest -v
docker compose config --quiet
git add docker-compose.yml spark/streaming.py
# Añadir explícitamente tus tests y evidencias, evitando archivos ajenos.
git commit -m "feat: integrar Kafka con Spark Structured Streaming (#14)"
git push -u origin feature/kevin-semana2-kafka-spark
```

Abrir PR hacia main, enlazar `Closes #14`, explicar comandos, versiones,
evidencias y limitaciones. Pedir revisión a Daniel o Deimer; main exige al menos
una aprobación de otro colaborador y descarta aprobaciones ante cambios nuevos.
Atender comentarios, volver a probar y pedir nueva revisión si corresponde.
No saltar protecciones ni fusionar sin aprobación. No publicar el release de
Semana 2 hasta que los tres Issues e integración acordada estén verificados.

Referencias oficiales para implementar (no sustituyen las pruebas del equipo):
- [Kafka: imagen Docker](https://kafka.apache.org/40/getting-started/docker/)
- [Spark 3.5.7: integración Kafka](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
