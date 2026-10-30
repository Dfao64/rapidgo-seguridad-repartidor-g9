# Semana 2 — Kafka → Spark (Kevin, Issue #14)

## Estado y dependencia

Implementación sobre main `4ef55e1329be2605006ccf89d4795be21e2ad116`.
El Producer está en el PR #30, pendiente de aprobación/fusión al preparar esta entrega.
No se copian sus archivos ni se fusiona su trabajo. Ejecutar los comandos del Producer
solo después de que su PR esté integrado y actualizar esta rama desde main.

Se probaron Spark local, validación y recuperación con fuente streaming de archivos.
Esto NO sustituye una prueba Kafka end-to-end. Ver evidencias y pendientes.

## Entorno y arranque (PowerShell, raíz del repositorio)

Requisitos: Docker Desktop activo, Compose v2 con `--wait`, al menos 4 GB de RAM
para el laboratorio. Host 9092 libre. Imágenes oficiales: Kafka 4.0.0, Spark
3.5.7 con Python 3 y Scala 2.12. El conector usa exactamente 3.5.7 / 2.12.
La descarga de los paquetes requiere acceso a Maven desde el contenedor.
Los contenedores de este laboratorio corren como root para inicializar volúmenes
nuevos; no es una configuración de producción. Solo se publica el puerto Kafka
en loopback. Spark ejecuta en modo local con dos hilos; no necesita un cluster master/worker.

```powershell
git switch feature/kevin-semana2-kafka-spark
# Una vez integrado el Producer: git fetch origin; git merge origin/main
docker compose config --quiet
docker compose pull
docker image inspect apache/kafka:4.0.0 apache/spark:3.5.7-python3 --format '{{json .RepoDigests}}'
docker compose up -d --wait
docker compose ps
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic rapidgo.seguridad.repartidor.v1 --partitions 3 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic rapidgo.seguridad.repartidor.v1
docker compose exec -T spark /opt/spark/bin/spark-submit --version
```

Confirmar PartitionCount=3, réplica 1, líderes e ISR. Si ya existe con otra
configuración, coordinar corrección sin eliminarlo. No aumentar particiones
mientras se verifica orden por repartidor. El controlador 9093 no es un bootstrap.
Kafka guarda datos en `kafka-data`; checkpoints y salidas en `spark-state`.
No ejecutar `down -v`: elimina datos y estado persistente.

## Consumo continuo

En una terminal, dejar activo:

```powershell
docker compose exec -T spark /opt/spark/bin/spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/spark/streaming.py
```

En otra, instalar y ejecutar el Producer siguiendo `producer/README.md` después
de integrar #30. Ejemplo acordado por su guía:

```powershell
python -m producer --bootstrap-servers 127.0.0.1:9092 --interval 5 --riders 3 --rounds 12 --seed 42 --start-time 2026-10-30T14:00:00Z --run-id kevin-integracion-01
```

Repetir con `--run-id kevin-integracion-02` sin detener Spark. Esperar 36 ACK
por corrida y comparar los IDs, claves y topic/partition/offset con Spark.
Verificar 3 repartidores y cero descartes en cada corrida. El orden es por clave,
no global, y varias claves pueden caer en una partición.

Los mensajes `RAPIDGO_PROGRESS` muestran batchId, numInputRows y offsets.
Hay dos consultas independientes: auditoría y válidos; no sumar sus numInputRows
como eventos distintos. Console imprime hasta 1000 filas por microbatch; para
verificación exhaustiva usar la salida JSON descrita abajo.

## Salidas y semántica

- `auditoria`: todos los eventos, originales, metadatos, esquema parseado,
  `valido`, `motivos` y timestamp UTC. Incluye inválidos y duplicados para auditoría.
- `validos`: solo válidos deduplicados por event_id, campos del contrato,
  flags de alertas y `incluye_kpi3`. Ausencia/null de desvío no se imputa y solo
  excluye KPI 3. No calcula ni reemplaza los indicadores de Deimer.
- Cada consulta tiene su propio checkpoint. `startingOffsets=earliest` solo aplica
  sin checkpoint; un reinicio recupera offsets y estado de deduplicación.
- Deduplicación histórica sin watermark: conserva todos los IDs mientras se mantenga
  el checkpoint. Consume estado creciente; para cargas largas se debe acordar con
  Deimer retención o almacenamiento externo. No cambiar esta política silenciosamente.
- Console no garantiza exactly-once: puede repetir impresiones durante recuperación.
  La deduplicación de IDs no convierte console en un sink transaccional.
- Un sink JSON usa el protocolo de commits del file sink de Spark para recuperación
  con el mismo destino y checkpoint. Las dos consultas no forman una transacción
  conjunta. La recuperación probada aquí corresponde al sink de archivos local.

Para conservar resultados completos en una ruta aislada, iniciar desde el principio
con estos parámetros adicionales (usar siempre el mismo par al reiniciar):

```powershell
docker compose exec -T spark /opt/spark/bin/spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/spark/streaming.py --checkpoint /state/checkpoints/kevin-json-v1 --output /state/output/kevin-json-v1
```

No reutilizar un checkpoint de console con JSON. Estos destinos no escriben en las
capas de Deimer. Acordar su integración Raw/Processed/Curated antes de modificarlas.
Auditoría conserva motivo de validación; los duplicados quedan en auditoría pero se
suprimen de válidos por estado Spark y no se etiquetan individualmente como descarte.

## Pruebas reproducibles

```powershell
python -m pip install -r requirements-streaming.txt
python -m pytest -v
python -m compileall spark tests
docker compose config --quiet
git diff --check
```

Las pruebas Spark necesitan Java 17. El archivo de requisitos adicional evita
modificar requirements.txt y CI de Deimer; usar Python 3.10–3.12 con Spark 3.5.7.
Las pruebas cubren tipos estrictos (sin booleanos/string numéricos), finitud,
fechas UTC reales y formato exacto, límites, GPS, clave, desvío opcional, parsing
Spark, deduplicación y reinicio con checkpoint. No recolectan un stream ilimitado.

## Evidencia pendiente en Docker

Guardar fecha, SHA de la rama, comandos y salidas de: imágenes/digests reales,
versiones Python/Java/Scala/Spark, Compose, salud, topic, ACK, varios microbatches,
comparación de 36 IDs y metadatos por corrida, segunda corrida sin reiniciar,
reinicio con mismo checkpoint y nuevos IDs. No presentar tests locales como prueba
Kafka. Probar eventos inválidos solo en otro topic de prueba y con checkpoints/salidas
propios usando `--topic`, `--checkpoint` y `--output`. Mantener topic del equipo limpio.

Fuentes: https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html
 y https://kafka.apache.org/40/getting-started/docker/.
