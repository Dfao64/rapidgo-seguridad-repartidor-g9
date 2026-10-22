# KPIs y alertas — Seguridad del repartidor

**Proyecto:** RapidGo 360 · Grupo 9 · Variante #2: Seguridad del repartidor
**Estado:** Propuesto (Semana 1) · **Responsable:** Deimer Hincapié · **Issue:** #3

Estas definiciones usan los campos del contrato de datos ([`contrato-eventos.md`](contrato-eventos.md)). **Solo se consideran eventos válidos**; los eventos inválidos se descartan antes de calcular cualquier indicador.

## 1. KPIs (exactamente tres)

| Indicador | Definición / cálculo |
| --- | --- |
| **KPI 1: velocidad promedio** | Promedio de `velocidad_kmh` de eventos válidos por repartidor y ventana temporal definida. |
| **KPI 2: batería crítica** | Cantidad y porcentaje de eventos con `bateria_pct <= 15`, por repartidor o ventana. |
| **KPI 3: desviaciones de ruta** | Cantidad y porcentaje de eventos con `desvio_metros > 300`; requiere ruta de referencia. |

### Fórmulas

- **KPI 1** = `sum(velocidad_kmh) / count(eventos válidos)` agrupado por `repartidor_id` y ventana.
- **KPI 2 (cantidad)** = `count(eventos con bateria_pct <= 15)`
  **KPI 2 (porcentaje)** = `cantidad / count(eventos válidos) × 100`
- **KPI 3 (cantidad)** = `count(eventos con desvio_metros > 300)`
  **KPI 3 (porcentaje)** = `cantidad / count(eventos válidos con desvio_metros presente) × 100`
  Los eventos sin `desvio_metros` se excluyen del numerador y del denominador de este KPI.

### Ventanas de tiempo propuestas

| KPI | Ventana propuesta | Razón |
| --- | --- | --- |
| KPI 1 | 5 minutos (tumbling), más un resumen por turno | Suaviza picos puntuales de velocidad sin perder reacción. |
| KPI 2 | 15 minutos y resumen por turno | La batería cambia lentamente; una ventana corta sería ruidosa. |
| KPI 3 | 15 minutos y resumen por pedido | Una desviación se interpreta mejor sobre un tramo del recorrido. |

> Las ventanas son propuestas y se **validarán en la Semana 2** con datos simulados reales.

## 2. Alertas (al menos dos)

| Alerta | Condición de activación |
| --- | --- |
| **Alerta A: exceso de velocidad** | Activar si `velocidad_kmh > 60`. |
| **Alerta B: batería crítica** | Activar si `bateria_pct <= 15`. |

### Emisión y control de duplicados

- **Emisión:** la condición se evalúa **por evento**, a medida que Spark lo procesa. La alerta no espera al cierre de una ventana.
- **Evitar duplicados:**
  1. Un mismo `event_id` genera como máximo una alerta de cada tipo (idempotencia por `event_id` + tipo de alerta).
  2. Mientras la condición persista para un mismo `repartidor_id`, no se emite una alerta nueva por cada evento: se emite al **cambiar de estado** (de normal a en alerta) y se repite como máximo cada 5 minutos si sigue activa.
  3. La alerta se **cierra** cuando el valor vuelve a estar dentro del umbral.
- **Datos faltantes:** si el campo necesario (`velocidad_kmh` o `bateria_pct`) falta o es inválido, el evento se descarta según el contrato y **no se evalúa ninguna alerta** con él. No se asume un valor por defecto.

## 3. Justificación de umbrales

Los valores **60 km/h, 15 % y 300 m** son **umbrales didácticos iniciales** definidos por el equipo para la simulación académica. **No** representan límites normativos generales ni legales (los límites reales dependen de la vía, la ciudad y el tipo de vehículo).

| Umbral | Razón de uso en el proyecto |
| --- | --- |
| 60 km/h | Valor de referencia simple que permite que la simulación genere casos de exceso de velocidad para probar la alerta. |
| 15 % | Nivel bajo que deja margen al repartidor para actuar antes de perder comunicación con el dispositivo. |
| 300 m | Distancia suficiente para distinguir una desviación relevante del ruido normal del GPS. |

Los umbrales deben ser **configurables** y podrán ajustarse tras analizar los datos simulados en la Semana 2 o si el profesor indica otros valores.

## 4. Dependencia importante

El KPI 3 depende de una **ruta de referencia simulada**: `desvio_metros` no se calcula únicamente con una coordenada GPS. Hasta definir esa ruta con el Producer, este KPI se considera **provisional**.

## 5. Coherencia con otros entregables

- Los nombres de campo coinciden con el contrato y con `samples/evento_repartidor.json`.
- El evento de ejemplo (68.5 km/h, 12 % de batería, 180 m de desvío) activa las alertas A y B y no cuenta como desviación de ruta.
- Las salidas (velocidad promedio, eventos de batería crítica, desvíos de ruta y alertas) coinciden con las del diagrama de arquitectura del Issue #2.
