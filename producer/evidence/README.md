# Evidencia ejecutada — Issue #13

Fecha: 30 de octubre de 2026 (America/Bogota). Base de implementación:
`4ef55e1329be2605006ccf89d4795be21e2ad116` en main. Los archivos de evidencia y
código quedan versionados juntos en el commit del PR; `SHA256SUMS.txt` permite
verificar integridad de los registros.

Entorno: Windows, Python 3.13.2, kafka-python 2.2.15, pytest 8.3.5,
Docker Engine 29.8.1, imagen oficial apache/kafka:4.0.0 (digest en image.txt).
Se utilizó únicamente `producer/compose.test.yml`, proyecto
`rapidgo-daniel-producer-test`, puerto loopback IPv4 19092, sin volúmenes
compartidos. Se retiró el contenedor y su red al terminar.

## Resultados

- `tests.txt`: 43 pruebas aprobadas (26 existentes + 17 casos nuevos).
- `topic.txt`: topic acordado con tres particiones, réplica 1, líder e ISR 1.
- `published.jsonl`: 12 ACK reales de Kafka, con event_id, clave, partición y offset.
- `consumed.jsonl`: 12 eventos leídos desde Kafka y resumen `verified: 12`.
  El verificador comparó payload completo contra la generación determinista,
  aplicó `spark.reglas.es_evento_valido`, comprobó clave/ID y orden por repartidor.
- `services.txt`: broker saludable durante la prueba; `image.txt`: digest.
- Sintaxis Python y validación del Compose temporal aprobadas, sin errores.

Las claves REP-001, REP-002 y REP-003 se asignaron a partición 1 (offsets 0..11).
No es evidencia de balance uniforme: es un resultado válido del hash de claves.
Sí se verificó la existencia de las tres particiones y el orden por repartidor.
Tiempo de evento fijo 14:00:00Z a 14:00:15Z, semilla 42, 4 rondas, 3 repartidores,
intervalo 5 segundos, run-id `evidence-week2`. Son datos sintéticos.

## Reproducir

Seguir los comandos de [../README.md](../README.md), sección Kafka temporal.
Crear un entorno vacío; ejecutar Producer y luego `python -m producer.verify_kafka`.
Los payloads e IDs deben coincidir, pero TopicId y offsets pueden cambiar si
se conserva un topic anterior. No volver a publicar la misma corrida en el
mismo topic para esta verificación: los IDs se repiten intencionalmente.

## Límites y pendientes

Esta prueba demuestra Producer → Kafka → consumidor Python de verificación.
No ejecuta Spark ni demuestra el flujo completo de Issue #14, alta
disponibilidad o exactly-once. Kevin debe completar su Compose y streaming;
Deimer conserva la integración con sus capas, reglas e indicadores.
La aprobación por otro colaborador y la fusión quedan pendientes del PR;
no se cambia la protección de main ni se cierra el Issue manualmente.

Durante la preparación, `localhost:19092` produjo NoBrokersAvailable por la
resolución local; la prueba final utiliza 127.0.0.1 explícito en cliente y
listener anunciado. Los logs de publicación corresponden a la corrida exitosa
en el broker recreado con esta configuración.
