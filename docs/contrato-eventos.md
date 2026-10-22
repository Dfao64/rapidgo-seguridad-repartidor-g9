# Contrato de datos — Evento de seguridad del repartidor

**Proyecto:** RapidGo 360 · Grupo 9 · Variante #2: Seguridad del repartidor
**Versión del contrato:** v1 (Semana 1) · **Estado:** Propuesto
**Responsable:** Deimer Hincapié · **Issue:** #3

> Todos los valores de este documento son **datos sintéticos de simulación**. No corresponden a repartidores, pedidos ni ubicaciones reales.

## 1. Propósito del evento

Cada evento describe el estado de seguridad de un repartidor en un instante: dónde está (GPS), a qué velocidad se mueve, cuánta batería le queda al dispositivo, en qué estado de entrega se encuentra y cuánto se ha alejado de su ruta de referencia. Con estos eventos el sistema calcula los KPIs y las alertas definidos en [`kpis-alertas.md`](kpis-alertas.md).

El evento se publica en el topic Kafka `rapidgo.seguridad.repartidor.v1`, usando `repartidor_id` como clave de partición (ver ADR-001).

## 2. Frecuencia simulada

**Por definir con el Producer.** Propuesta inicial: 1 evento cada 5 segundos por repartidor activo. Este valor se confirmará con quien implemente el Producer y se ajustará en la Semana 2 según la carga de la simulación.

## 3. Esquema de campos

| Campo | Tipo | Regla mínima |
| --- | --- | --- |
| `event_id` | string | Obligatorio; identificador único de evento |
| `repartidor_id` | string | Obligatorio; clave de partición |
| `pedido_id` | string | Obligatorio en esta versión |
| `timestamp` | string ISO 8601 UTC | Obligatorio; fecha/hora válida (formato `YYYY-MM-DDThh:mm:ssZ`) |
| `gps.latitud` / `gps.longitud` | number | Rangos [-90, 90] / [-180, 180] |
| `velocidad_kmh` | number | Valor >= 0 |
| `bateria_pct` | number | Entre 0 y 100 |
| `estado` | string | Ej.: `EN_RUTA`, `ENTREGADO`, `PAUSADO` |
| `desvio_metros` | number | >= 0; calculado frente a ruta de referencia |

### Notas sobre los campos

- **`estado`:** valores permitidos en esta versión: `EN_RUTA`, `ENTREGADO`, `PAUSADO`. Cualquier otro valor se considera inválido hasta ampliar el contrato.
- **`desvio_metros`:** no se obtiene solo con una coordenada GPS. Requiere una **ruta de referencia simulada** contra la cual el Producer calcula la distancia entre la posición actual y la ruta esperada.
- **`event_id`:** debe ser único; se usa para evitar procesar o alertar dos veces el mismo evento.

## 4. Reglas de validación y tratamiento de campos faltantes

| Situación | Tratamiento |
| --- | --- |
| Falta `event_id`, `repartidor_id`, `pedido_id` o `timestamp` | Evento **inválido**: se rechaza y se envía a la zona de descartes (capa Raw conservada para auditoría). No entra a KPIs ni alertas. |
| Falta `gps` o sus coordenadas, o están fuera de rango | Evento **inválido**. |
| `velocidad_kmh` ausente o negativa | Evento **inválido**. No se imputa un valor por defecto. |
| `bateria_pct` ausente o fuera de [0, 100] | Evento **inválido**. |
| `estado` ausente o fuera del catálogo | Evento **inválido**. |
| `desvio_metros` ausente o `null` | Evento **válido**, pero se **excluye únicamente del KPI 3** (no hay ruta de referencia). Sigue contando para los demás KPIs y alertas. |
| `desvio_metros` negativo | Evento **inválido**. |
| `event_id` repetido | Se conserva el primero y se descartan los duplicados. |

Principio general: **no se inventan valores**. Un dato faltante nunca se reemplaza por cero ni por un promedio, porque falsearía los KPIs.

## 5. Ejemplo válido

Archivo: [`samples/evento_repartidor.json`](../samples/evento_repartidor.json)

```json
{
  "event_id": "evt-000001",
  "repartidor_id": "REP-009",
  "pedido_id": "PED-1024",
  "timestamp": "2026-10-22T14:30:00Z",
  "gps": {"latitud": 4.7320, "longitud": -74.2640},
  "velocidad_kmh": 68.5,
  "bateria_pct": 12,
  "estado": "EN_RUTA",
  "desvio_metros": 180
}
```

Es válido porque cumple todos los tipos y rangos. Además, dispara dos alertas (velocidad 68.5 > 60 y batería 12 <= 15), pero **no** cuenta como desviación de ruta (180 m <= 300 m).

## 6. Ejemplo inválido (explicado)

```json
{
  "event_id": "evt-000002",
  "repartidor_id": "REP-009",
  "timestamp": "22/10/2026 14:30",
  "gps": {"latitud": 95.0, "longitud": -74.2640},
  "velocidad_kmh": -10,
  "bateria_pct": 150,
  "estado": "VOLANDO",
  "desvio_metros": 180
}
```

Errores detectados:

1. **Falta `pedido_id`**, que es obligatorio en esta versión.
2. **`timestamp` mal formado:** no sigue ISO 8601 en UTC.
3. **`gps.latitud = 95.0`** está fuera del rango [-90, 90].
4. **`velocidad_kmh = -10`** es negativa (debe ser >= 0).
5. **`bateria_pct = 150`** supera el máximo de 100.
6. **`estado = "VOLANDO"`** no pertenece al catálogo permitido.

Resultado: el evento se rechaza completo y no alimenta ningún KPI ni alerta.

## 7. Nota de simulación

Los eventos son generados por un Producer académico. Los valores (coordenadas, velocidades, niveles de batería, desvíos) son simulados y los umbrales asociados son didácticos, no normativos.
