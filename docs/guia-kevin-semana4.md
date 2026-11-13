# Guía para Kevin — Semana 4 — Issue #20

**Responsable:** Kevin Sánchez (`kevsanchez2103`). **Objetivo:** Quality Gate y GitHub Actions verificables para `v1.0.0`.

## Paso a paso
1. Revisar PR de Daniel de Semana 4 e identificar pruebas nuevas. Coordinar con Deimer la documentación y con Daniel los escenarios de integración.
2. Actualizar `main` después del merge aprobado y crear `feature/semana4-quality-gate-kevin`.
3. Auditar `.github/workflows/ci.yml`: debe correr en PR a `main`, push y `workflow_dispatch`; instalar dependencias, compilar Python, ejecutar `pytest` y validar Compose. No eliminar pruebas fallidas ni usar `continue-on-error` para ocultar errores.
4. Definir Quality Gate: (a) sintaxis válida, (b) suite `pytest` verde, (c) `docker compose config --quiet` verde, (d) ningún secreto comprometido, (e) revisión de al menos un colaborador autorizado, (f) evidencia de integración real para cerrar Issue #19.
5. Ejecutar `python -m pytest tests/ -v` localmente. Resolver fallos reproducibles y añadir pruebas de regresión cuando corresponda.
6. Crear o actualizar workflow sin pisar otros cambios. Verificar ejecución automática al abrir el PR de Kevin.
7. Ir a GitHub → Actions → CI RapidGo → Run workflow; seleccionar rama de Kevin, ejecutar y guardar URL del run, commit SHA y captura de resultado. Esto es requisito explícito del Issue #20.
8. Si falla un job, entrar al job y al step rojo, leer logs, corregir, hacer commit y repetir hasta que el run nuevo sea verde.
9. Abrir PR a `main`, solicitar revisión de Daniel y Deimer, documentar criterios y adjuntar enlaces a runs exitosos. No fusionar sin revisión.
10. Compartir con Deimer evidencia para README y release final. Confirmar que `v1.0.0` no se publique antes de terminar las tres responsabilidades.

## Comandos orientativos
```bash
git checkout main
git pull origin main
git checkout -b feature/semana4-quality-gate-kevin
python -m pip install -r requirements.txt
python -m pytest tests/ -v
docker compose config --quiet
git add .
git commit -m "ci: quality gate semana 4"
git push -u origin feature/semana4-quality-gate-kevin
```

**Nota:** no asumir que la ejecución manual se ha realizado hasta obtener su URL en GitHub Actions. Revisar los permisos del repositorio para solicitar aprobaciones; un autor no puede aprobar su propio PR.
