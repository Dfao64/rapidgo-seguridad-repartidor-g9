# ADR-001 — Diseño del topic y particiones Kafka

Estado: Propuesto (Semana 1)

Contexto: RapidGo 360 necesita procesar eventos de seguridad de repartidores.

Decisión:
- Topic: rapidgo.seguridad.repartidor.v1
- Clave: repartidor_id
- Particiones iniciales: 3

Justificación:
- La clave agrupa los eventos de un mismo repartidor en la misma partición,
  manteniendo su orden relativo dentro de esa partición.
- Tres particiones permiten paralelismo inicial para la simulación académica.
- El número deberá reevaluarse según carga, consumidores y pruebas.

Consecuencias:
- El orden global entre repartidores no está garantizado.
- Un repartidor con mucha actividad podría concentrar carga.
- Cambiar particiones posteriormente puede modificar la asignación de claves.

Alternativas consideradas: una partición; particionar por pedido_id.

Validación pendiente: comprobar distribución y consumo en Semana 2.
