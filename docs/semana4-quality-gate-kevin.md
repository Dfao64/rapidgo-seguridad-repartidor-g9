# Semana 4 — Quality Gate y GitHub Actions

**Proyecto:** RapidGo 360, grupo 9, variante 2.  
**Responsable:** Kevin Sánchez (`kevsanchez2103`).  
**Referencia:** [Issue #20](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/issues/20).  
**Base auditada:** `fa7473061ffb8df2ffd11f43f5cdc9b0d0a211a3`.

## A. Objetivo y alcance

Comprobar que la integración conserva las validaciones de datos, indicadores,
alertas, persistencia y recuperación, y que CI rechaza errores antes del release
`v1.0.0`. Esta entrega se preparó con asistencia automatizada desde la cuenta de
Kevin. La revisión humana de Daniel y Deimer es una actividad independiente.

## B. Herramientas

Python y pytest ejecutan la suite; Java permite las pruebas locales de Spark;
GitHub Actions ejecuta CI; Docker Compose valida y levanta la infraestructura;
Apache Airflow 2.10.5 verifica el DAG con reintento real. La API de GitHub permite
consultar commits, jobs, pasos, logs, artefactos y reglas del repositorio.

## C. Criterios del Quality Gate

| Criterio | Comprobación y condición de aprobación |
|---|---|
| Sintaxis | `python -m compileall producer spark airflow/dags scripts tests`, código 0. |
| Pruebas | `python -m pytest tests/ -v`, todas las pruebas obligatorias aprobadas; revisar también errores y pruebas omitidas. |
| Compose | `docker compose config --quiet`, código 0. Esto valida configuración, no salud de servicios. |
| Airflow | Las cinco tareas deben terminar en `success`; `validar_calidad` debe registrar al menos dos intentos, rechazar un evento y conservar uno; salida auditada. |
| Seguridad | Revisar los archivos versionados por credenciales y claves privadas; permisos de CI limitados a lectura. La revisión por patrones no equivale a una garantía de ausencia de secretos en todo el historial o archivos binarios. |
| Revisión e integración | PR aprobado por un colaborador autorizado y checks exitosos antes del merge. Solicitar revisión a Daniel y Deimer. |
| Pipeline real | Evidencia reproducible Producer → Kafka → Spark → Parquet → Airflow, con conteos y aserciones de KPIs y alertas. |

### Protección de `main`: diferencia observada

La [regla activa 25678101](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/rules/25678101)
exige **una aprobación**, descarta aprobaciones al publicar cambios y prohíbe
el borrado y los pushes que no sean fast-forward. Sus reglas **no incluyen
`required_status_checks`**. Por tanto, la revisión está exigida técnicamente,
pero el CI verde sigue siendo una política del equipo sin bloqueo obligatorio.

La consulta a protección clásica devolvió HTTP 403. La lectura del ruleset sí
fue exitosa; no se interpreta el 403 como ausencia de protección. Kevin tiene
`push` y `triage`, pero no `admin` ni `maintain`.

**Acción propuesta para el administrador:** editar la regla existente, conservar
sus restricciones y activar checks obligatorios para `validar`,
`Integración DAG con Airflow 2.10.5` y `e2e`, seleccionando los checks que GitHub
muestre tras el PR. Exigir rama actualizada y conservar al menos una aprobación.
No se modificaron reglas administrativas desde esta cuenta.

## D. Pruebas y evidencias

| Prueba | Esperado | Obtenido | Estado | Evidencia |
|---|---|---|---|---|
| Sintaxis local | Sin errores | Compilación correcta | Aprobado | [compileall.log](evidencias/semana4-kevin/compileall.log) |
| Suite local | 111 pruebas sin fallos ni omisiones | 111 passed, 6 warnings, 18.20 s | Aprobado | [pytest.log](evidencias/semana4-kevin/pytest.log), [JUnit](evidencias/semana4-kevin/pytest.xml) |
| Suite actual en main | Todas aprobadas | 111 passed, 6 warnings | Aprobado en GitHub | [Job validar](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions/runs/43525151567/job/132228947516) |
| Compose actual en main | Código 0 | Paso exitoso | Aprobado en GitHub | Mismo job, paso Validar Docker Compose |
| Airflow real | Fallo temporal, reintento y cinco tareas exitosas | `try_number=2`, un rechazo, un válido, auditoría verificada | Aprobado en GitHub | [Job Airflow](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions/runs/43525151567/job/132228947890) |
| Control negativo | Una prueba deliberadamente fallida conserva código 1 al pasar por tee | Código 1 | Aprobado | [control-negativo.json](evidencias/semana4-kevin/control-negativo.json) |
| YAML y disparadores | PR a main y workflow_dispatch | Ambos presentes | Aprobado | Workflow y control negativo |
| Revisión de secretos | Ningún hallazgo de credencial real en el alcance revisado | Sin hallazgos en revisión por patrones de texto | Alcance limitado | Sección F |
| Ejecución manual | Evento workflow_dispatch exitoso | No ejecutada en esta sesión | Pendiente | Sección E |
| Aprobaciones y merge | Revisiones válidas y CI verde en main | Requiere intervención de revisores | Pendiente | PR de esta entrega |

Los resultados de GitHub anteriores fueron consultados nuevamente durante esta
auditoría: no se copió su estado de una descripción histórica. Los resúmenes de
jobs y reglas están en [auditoria-github.json](evidencias/semana4-kevin/auditoria-github.json)
y los extractos relevantes de logs en [extractos-actions.log](evidencias/semana4-kevin/extractos-actions.log).

## E. GitHub Actions y reproducción

- [CI por push de main, run 43525151567](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions/runs/43525151567): `success`, SHA base indicado arriba.
- [CI automático del PR #45, run 43523485568](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions/runs/43523485568): `success`, evento `pull_request`.
- [E2E de Daniel, run 43519537450](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions/runs/43519537450): `success`, SHA `09f3307e53900a6aa0e8ba5f0e0ef1c645f65914`; artefacto `rapidgo-semana4-e2e-43519537450` (ID 14081723332, disponible al auditar).
- Airflow publica el artefacto `airflow-integracion-59` en el run de main.

Se comprobó con `git diff` que Producer, Spark, DAG, scripts, Compose y workflow
E2E no cambiaron entre el commit E2E citado y la base auditada. Los cambios de
esta entrega son CI y documentación, por lo que esa evidencia sigue aplicando
a los componentes del pipeline. El PR vuelve a ejecutar E2E automáticamente.

**Ejecución manual pendiente:** el conector disponible no expone dispatch y el
navegador está sin sesión. Kevin debe abrir [CI RapidGo](https://github.com/Dfao64/rapidgo-seguridad-repartidor-g9/actions/workflows/ci.yml),
pulsar **Run workflow**, seleccionar `feature/semana4-quality-gate-kevin` y
ejecutar. Esperar ambos jobs; registrar URL, ID, SHA y `event=workflow_dispatch`.
Después del merge aprobado, verificar también el CI por push de `main`.
Una ejecución `push`, `pull_request`, o un reintento de ellas no demuestra dispatch.
El `run_id` que Airflow llama `manual__...` tampoco demuestra dispatch de GitHub.

Reproducción local:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m compileall producer spark airflow/dags scripts tests
SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python" \
  python -m pytest tests/ -v --junitxml=pytest.xml
docker compose config --quiet
```

Para Airflow, utilizar el job aislado de CI con Python 3.12 y sus constraints
de 2.10.5. El DAG.test emplea Parquet sintético; la prueba E2E separada usa
Kafka y contenedores reales. No confundir esos dos niveles de integración.

## F. Problemas, cambios y límites

1. **Faltaban artefactos de la suite general:** CI ahora conserva JUnit XML,
   logs de sintaxis, pytest y Compose, SHA probado, Python y versiones instaladas,
   durante 30 días. La subida se intenta incluso cuando falla un paso.
2. **Propagación de errores:** se declara Bash explícitamente. GitHub lo ejecuta
   con `-eo pipefail`; las tuberías con `tee` conservan errores. Se ensayó una
   prueba fallida aislada fuera de `tests/` y se obtuvo código 1. No se eliminó
   ninguna prueba ni se agregó `continue-on-error`.
3. **Recursos:** ambos jobs de CI tienen límite de 15 minutos; el token solo
   necesita `contents: read`. Se incluyó `scripts/` en la compilación.
4. **Entorno local:** inicialmente no había pytest ni Docker. Se instalaron las
   dependencias Python en un entorno aislado. Docker Compose se verifica con
   el job real de GitHub; no se afirma haberlo ejecutado localmente.
5. **Spark local:** el primer intento no pudo enlazar la dirección del driver
   (`Cannot assign requested address`). Se conservó ese log y se repitió con
   `SPARK_LOCAL_IP=127.0.0.1`. Un segundo intento detectó que el worker usaba
   otro intérprete sin pyarrow; se fijó `PYSPARK_PYTHON` al Python del venv.
   Los dos errores quedan como extractos en `pytest-intento-inicial.log` y
   `pytest-intento-worker.log`, sin modificar los componentes del equipo.
6. **Dependencias:** `requirements.txt` no fija versiones. Las versiones locales
   quedan registradas para reproducir el entorno; el warning de compatibilidad
   de PySpark con pandas 3 merece seguimiento, sin ocultarlo ni cambiar el
   alcance de los componentes de Daniel.
7. **Seguridad:** revisión de patrones de credenciales en archivos de texto
   versionados, workflows, Compose y gitignore. Las menciones encontradas eran
   instrucciones o nombres de archivos, sin valores de credenciales reales.
   `CLUSTER_ID` de Kafka identifica el clúster; no es una contraseña. No se
   certifica un escaneo completo del historial ni de imágenes.

## G. Conclusión y cierre

La base auditada tiene pruebas, Compose, Airflow y E2E exitosos. La entrega
refuerza la trazabilidad de CI. **La responsabilidad de Kevin todavía no está
cerrada:** faltan dispatch exitoso verificable, revisiones, merge aprobado y
comprobación posterior de main. También debe resolverse o aceptarse expresamente
la diferencia de checks obligatorios por un administrador.

El Issue #20 debe permanecer abierto hasta reunir esas evidencias. No se publica
el release: corresponde a Deimer. Este documento y el PR entregan a los revisores
los enlaces y resultados necesarios para continuar.
