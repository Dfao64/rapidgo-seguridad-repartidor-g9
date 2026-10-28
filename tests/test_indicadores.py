from spark.indicadores import calcular_indicadores


def evento(rep="REP-001", velocidad=40, bateria=80, desvio=100):
    return {
        "event_id": f"evt-{rep}-{velocidad}-{bateria}-{desvio}", "repartidor_id": rep,
        "pedido_id": "PED-1", "timestamp": "2026-10-22T14:30:00Z",
        "gps": {"latitud": 4.73, "longitud": -74.26},
        "velocidad_kmh": velocidad, "bateria_pct": bateria,
        "estado": "EN_RUTA", "desvio_metros": desvio,
    }


def test_velocidad_promedio_por_repartidor():
    r = calcular_indicadores([evento(velocidad=40), evento(velocidad=60)])
    assert r["por_repartidor"]["REP-001"]["velocidad_promedio"] == 50.0


def test_separa_repartidores():
    r = calcular_indicadores([evento("REP-001", 30), evento("REP-002", 50)])
    assert set(r["por_repartidor"]) == {"REP-001", "REP-002"}
    assert r["por_repartidor"]["REP-002"]["velocidad_promedio"] == 50.0


def test_cuenta_bateria_critica():
    r = calcular_indicadores([evento(bateria=10), evento(bateria=15), evento(bateria=50)])
    ind = r["por_repartidor"]["REP-001"]
    assert ind["eventos_bateria_critica"] == 2
    assert ind["pct_bateria_critica"] == 66.67


def test_cuenta_exceso_de_velocidad():
    r = calcular_indicadores([evento(velocidad=60), evento(velocidad=61)])
    assert r["por_repartidor"]["REP-001"]["eventos_exceso_velocidad"] == 1


def test_desvio_critico_y_porcentaje():
    r = calcular_indicadores([evento(desvio=300), evento(desvio=301)])
    ind = r["por_repartidor"]["REP-001"]
    assert ind["eventos_desvio_critico"] == 1
    assert ind["pct_desvio_critico"] == 50.0


def test_eventos_sin_desvio_se_excluyen_del_kpi3():
    r = calcular_indicadores([evento(desvio=None), evento(desvio=500)])
    ind = r["por_repartidor"]["REP-001"]
    assert ind["eventos"] == 2
    assert ind["pct_desvio_critico"] == 100.0


def test_sin_ningun_desvio_el_porcentaje_es_none():
    r = calcular_indicadores([evento(desvio=None)])
    assert r["por_repartidor"]["REP-001"]["pct_desvio_critico"] is None


def test_descarta_eventos_invalidos():
    r = calcular_indicadores([evento(), evento(bateria=150), evento(velocidad=-1)])
    assert r["eventos_validos"] == 1
    assert r["eventos_descartados"] == 2


def test_lista_vacia():
    r = calcular_indicadores([])
    assert r == {"por_repartidor": {}, "eventos_validos": 0, "eventos_descartados": 0}
