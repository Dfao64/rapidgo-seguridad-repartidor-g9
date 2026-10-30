# Evidencias — Kevin, Semana 2

Fecha: 2026-10-30. Base: main `4ef55e1329be2605006ccf89d4795be21e2ad116`.
Los archivos probados pertenecen al commit que incorpora estas evidencias;
no se inventa un SHA autorreferencial. Consultar `git log -1` en la rama.

## Ejecutado

- Python 3.12.14, Java OpenJDK 17.0.20, PySpark/Spark 3.5.7, Scala 2.12.
- `SPARK_LOCAL_IP=127.0.0.1 python -m pytest -v`: resultado completo en pytest.txt.
  Incluye pruebas anteriores de Deimer, validación estricta, parsing con Spark y
  streaming con fuente de archivos, sink JSON y recuperación real del checkpoint.
- `python -m compileall -q spark tests`: exitoso.
- `git diff --check`: exitoso.
- Docker Compose standalone 2.39.4, `config --quiet`: exitoso, sin daemon.
- Consulta del registry Docker Hub: ambas etiquetas existen para linux/amd64 y
  linux/arm64. Digests del índice consultado en imagenes-registro.json; no son
  prueba de descarga ni de ejecución de contenedores.

## No ejecutado / pendiente

Este entorno no tiene Docker Engine. No hay evidencia de servicios saludables,
topic creado, ACK del Producer, consumo Kafka, conteo end-to-end ni reinicio con
Kafka. No se declara completado el criterio de aceptación del Issue #14.
El PR #30 del Producer sigue abierto al preparar esta rama. No se ha aprobado
ni fusionado por cuenta de otro integrante. No se publica release.

Seguir docs/streaming-kevin.md para completar en Docker Desktop. Registrar digests
descargados, versiones efectivas, comandos, logs y conteos; luego solicitar revisión.
No subir datos reales ni secretos.
