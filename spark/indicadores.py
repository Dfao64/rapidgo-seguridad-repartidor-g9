"""Indicadores iniciales de seguridad del repartidor (KPIs de docs/kpis-alertas.md).

"""
from collections import defaultdict

from spark.reglas import (
    es_bateria_critica, es_desvio_critico, es_evento_valido, es_exceso_velocidad,
)


def calcular_indicadores(eventos):
    """Devuelve indicadores por repartidor usando solo eventos válidos.

    - KPI 1: velocidad promedio.
    - KPI 2: cantidad y porcentaje de eventos con batería crítica.
    - KPI 3: cantidad y porcentaje de desvíos críticos (solo eventos con
      desvio_metros; los demás se excluyen del numerador y del denominador).
    """
    acum = defaultdict(lambda: {
        "total": 0, "suma_velocidad": 0.0, "bateria_critica": 0,
        "exceso_velocidad": 0, "con_desvio": 0, "desvio_critico": 0,
    })
    descartados = 0
    for ev in eventos:
        if not es_evento_valido(ev):
            descartados += 1
            continue
        a = acum[ev["repartidor_id"]]
        a["total"] += 1
        a["suma_velocidad"] += ev["velocidad_kmh"]
        a["bateria_critica"] += es_bateria_critica(ev["bateria_pct"])
        a["exceso_velocidad"] += es_exceso_velocidad(ev["velocidad_kmh"])
        if ev.get("desvio_metros") is not None:
            a["con_desvio"] += 1
            a["desvio_critico"] += es_desvio_critico(ev["desvio_metros"])

    por_repartidor = {}
    for rid, a in acum.items():
        por_repartidor[rid] = {
            "eventos": a["total"],
            "velocidad_promedio": round(a["suma_velocidad"] / a["total"], 2),
            "eventos_bateria_critica": a["bateria_critica"],
            "pct_bateria_critica": round(100 * a["bateria_critica"] / a["total"], 2),
            "eventos_exceso_velocidad": a["exceso_velocidad"],
            "eventos_desvio_critico": a["desvio_critico"],
            "pct_desvio_critico": (
                None if a["con_desvio"] == 0
                else round(100 * a["desvio_critico"] / a["con_desvio"], 2)),
        }
    return {
        "por_repartidor": por_repartidor,
        "eventos_validos": sum(a["total"] for a in acum.values()),
        "eventos_descartados": descartados,
    }
