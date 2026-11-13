# Ejecución técnica — Semana 4 Daniel (Issue #19)

## Script de integración publicado
`scripts/semana4_e2e.py` inicia Kafka y Spark reales, crea el topic, ejecuta Spark Structured Streaming con conector Kafka, publica cuatro eventos con claves Kafka correctas, espera archivos Parquet en Processed y valida las salidas Curated de alertas.

El script **no simula** resultados: falla con código distinto de cero cuando no se cumplen las condiciones y guarda `resultado_e2e.json` y `spark.log` en `datalake/semana4-<id>/`.

### Requisitos
- Docker Engine / Docker Desktop y Docker Compose v2
- Python 3.11+ y `kafka-python`, `pandas`, `pyarrow`
- Acceso a Maven Central para descargar el conector `spark-sql-kafka-0-10_2.12:3.5.7` compatible con Spark 3.5.7.
- Puertos 9092 y 8080 libres; recursos suficientes para Kafka, Spark y Airflow.

### Ejecución
```bash
python -m pip install kafka-python pandas pyarrow pytest
python -m pytest tests/test_semana4_integracion.py -v
python scripts/semana4_e2e.py --timeout 300
```

El script requiere que `docker compose` esté disponible y que el usuario pueda ejecutar Docker. El Data Lake debe ser escribible para el contenedor Spark. **No reutilizar la misma carpeta de checkpoint entre pruebas**, pues Spark puede conservar offsets y estados.

### Validaciones esperadas
- Cuatro eventos publicados y recibidos en Processed.
- Velocidad promedio: 50 km/h.
- Batería crítica: 2 de 4 eventos = 50 %.
- Desvío crítico: 1 de 3 eventos con desvío informado = 33.33 %.
- Alertas: EXCESO_VELOCIDAD y BATERIA_CRITICA, con los IDs correctos.

### Airflow: comprobación adicional obligatoria
El ejecutor anterior **no verifica Airflow**. Para cerrar el Issue #19 se requiere ejecutar por separado el DAG `rapidgo_seguridad_semanal` con un directorio `processed` que contenga el lote Parquet validado por Spark. El DAG debe terminar con todas las tareas en verde y generar `agregados.csv`, `reporte.json` y los metadatos de auditoría. Guardar capturas y logs del DAG y comparar los agregados con los resultados esperados. No confundir un DAG que simplemente arranca con un DAG exitoso.

### Evidencias a anexar al PR
- Salida `python -m pytest tests/ -v`.
- `resultado_e2e.json` con estado satisfactorio y `spark.log` sin errores.
- Resultado de `docker compose ps`, topic y offsets Kafka.
- Listado de Parquet Raw, Processed, Curated y contenido de alertas.
- Captura o logs del DAG y sus reportes.
- URL del GitHub Actions run verde y aprobación de revisores.

### Estado actual y criterio de cierre
La integración y los tests están **publicados, no ejecutados en Docker por este asistente**. El Issue #19 permanece abierto. Solo marcarlo completo después de evidenciar el recorrido Producer → Kafka → Spark → Data Lake → Airflow, verificar KPIs y alertas, corregir defectos detectados y obtener las aprobaciones necesarias. La publicación del release final corresponde a Deimer y no se adelanta.
