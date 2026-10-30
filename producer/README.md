# Producer de Daniel — Semana 2 / Issue #13

Ejecutar desde la raíz del repositorio con Python 3.13. No requiere Spark para
generar ni publicar. Contrato: [v1](../docs/contrato-eventos.md);
[ADR-001](../docs/decisions/ADR-001-kafka-particiones.md).

## Instalación

```powershell
python -m venv .venv
.venv/Scripts/python -m pip install -r producer/requirements.txt
.venv/Scripts/python -m pip install pytest==8.3.5
.venv/Scripts/python -m pytest -q
```

En Linux/macOS usar `.venv/bin/python` en lugar de `.venv/Scripts/python`.
La dependencia del Producer está fijada por separado; no se cambian los requisitos
ni el workflow compartidos de Deimer.

## Uso

```powershell
# JSONL sin Kafka (sin esperas; limitar rounds para evitar salida infinita)
.venv/Scripts/python -m producer --dry-run --rounds 2 --seed 42 --start-time 2026-10-30T14:00:00Z --run-id demo

# Kafka de Kevin: sustituir dirección por su listener accesible desde el host
.venv/Scripts/python -m producer --bootstrap-servers 127.0.0.1:9092 --rounds 12
```

| Opción | Variable de entorno | Predeterminado |
| --- | --- | --- |
| `--bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `--topic` | `KAFKA_TOPIC` | `rapidgo.seguridad.repartidor.v1` |
| `--interval` | `PRODUCER_INTERVAL` | 5 segundos por ronda |
| `--riders` | `PRODUCER_RIDERS` | 3 |
| `--rounds` | `PRODUCER_ROUNDS` | 0, continuo hasta Ctrl+C |
| `--seed` | `PRODUCER_SEED` | 42 |
| `--start-time` | `PRODUCER_START_TIME` | UTC actual al iniciar |
| `--run-id` | `PRODUCER_RUN_ID` | UUID nuevo por ejecución |
| `--dry-run` | — | desactivado |

CLI tiene prioridad sobre el entorno. `interval` debe ser entero >=1 porque el
contrato representa timestamps con segundos. Cada ronda emite un evento por
repartidor; espera hasta la siguiente ronda al publicar. Si Kafka tarda más que
el intervalo, la generación queda limitada por sus ACK, sin garantía de tiempo
real estricto. Timestamp es tiempo simulado: inicio + ronda × intervalo.

Para reproducir **el JSON completo**, fijar seed, start-time, run-id, riders,
rounds e interval. Los UUID de eventos se derivan de run-id + ronda + repartidor.
Reutilizar run-id reproduce IDs intencionalmente: el consumidor debe deduplicar.
Para corridas nuevas omitir run-id (evita colisiones entre ejecuciones).

GPS sigue una ruta académica de referencia norte-sur en longitud -74.264°.
Se calcula `desvio_metros` como distancia perpendicular sobre esfera de radio
6 371 000 m a ese meridiano usando las coordenadas emitidas. No son rutas reales.
Estados muestreados independientemente, batería y velocidad sintéticas;
PAUSADO/ENTREGADO tienen velocidad cero. No se modela un ciclo logístico real.

Kafka recibe clave UTF-8 `repartidor_id` y valor JSON UTF-8. Se esperan ACK de
todas las réplicas disponibles, con 5 reintentos y una petición en vuelo para
preservar orden por clave. La salida incluye topic/partición/offset **después del
ACK**. Error de conexión/entrega termina con código 1; Ctrl+C con 130. Puede
haber duplicados ante fallos/reintentos; no se promete exactly-once. Deduplicar
por event_id. El cierre drena mensajes pendientes. Este ejemplo usa PLAINTEXT
para desarrollo local; no incluye configuración SASL/TLS.

## Kafka temporal independiente

Requiere Docker Desktop activo con contenedores Linux. No ejecutar estos pasos
contra un broker del equipo. El archivo `producer/compose.test.yml` tiene nombre
de proyecto propio, puerto host 19092, red propia y ningún volumen compartido.
No modifica `docker-compose.yml`. La prueba usa un broker y factor de réplica 1;
no demuestra alta disponibilidad. No cambiar el número de particiones durante
la prueba porque puede cambiar la asignación de claves.

```powershell
docker compose -f producer/compose.test.yml up -d --wait
docker compose -f producer/compose.test.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic rapidgo.seguridad.repartidor.v1 --partitions 3 --replication-factor 1
docker compose -f producer/compose.test.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic rapidgo.seguridad.repartidor.v1
.venv/Scripts/python -m producer --bootstrap-servers 127.0.0.1:19092 --rounds 4 --seed 42 --start-time 2026-10-30T14:00:00Z --run-id evidence-week2
.venv/Scripts/python -m producer.verify_kafka
docker compose -f producer/compose.test.yml down -v
```

Comprobar código de salida de cada comando antes de continuar. `verify_kafka`
debe terminar con `verified: 12`, partitions `[0,1,2]`: comprueba cada payload
contra la generación determinista, contrato, clave y orden por repartidor.
No exige que tres repartidores ocupen las tres particiones: el hash puede
agrupar claves. Ejecutar una sola vez sobre un topic vacío; para repetir,
recrear **solo este proyecto temporal** con `down -v` y `up -d --wait`.
El verificador lee desde el principio sin grupo ni commit de offsets.

Si falla conexión, revisar `docker compose -f producer/compose.test.yml ps` y
`logs`. Usar 127.0.0.1 explícito evita resolver localhost a IPv6 en Windows.
Desde otro contenedor, el listener interno de esta prueba es `kafka:29092`;
desde host es `127.0.0.1:19092`. No intercambiarlos.

Evidencia guardada: [evidence/README.md](evidence/README.md).
Guía de integración pendiente: [Kevin, Issue #14](../docs/guia-kevin-semana2.md).
