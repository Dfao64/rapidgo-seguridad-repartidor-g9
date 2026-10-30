# Transferencia de GitHub Actions a RapidGo 360
Autor: Daniel Abello. Fecha: 30 de octubre de 2026.

## Variante real y regla propia
Grupo 9, variante #2: Seguridad del repartidor.
Se reutilizan las reglas existentes de spark/reglas.py, no StockGuard.
Un evento valido dispara BATERIA_CRITICA cuando bateria_pct <= 15.
Se evalua primero es_evento_valido; un dato fuera de [0,100] se rechaza
y no genera alertas. El umbral es didactico, no una norma legal.

## Tres casos minimos y sus pruebas existentes
- Normal: velocidad 40 y bateria 80 -> []; test_evento_normal_no_alerta.
- Limite: bateria 15 -> critica; bateria 16 -> no critica; test_bateria_borde.
- Invalido: velocidad -5 y bateria 150 -> []; test_evento_invalido_no_genera_alertas.
Ademas test_velocidad_borde verifica 60 sin alerta y 60.1 con alerta.

## Carpetas y archivos que se verifican
compileall: producer/, spark/, airflow/dags/ y tests/.
pytest: tests/test_alertas.py, test_contrato_evento.py y test_indicadores.py.
El contrato se comprueba sobre samples/evento_repartidor.json y variantes invalidas:
pedido_id obligatorio, timestamp UTC, GPS, bateria, velocidad y catalogo de estado.
docker compose config --quiet comprueba docker-compose.yml sin iniciar servicios.
Actualmente services: {} es una configuracion preliminar; pasar ese check no
demuestra que Kafka, Spark o Airflow funcionen.

## Tres checks, error evitado y prioridad
1. pytest: reglas y contrato. Evita cambiar <=15 por <15, alertar con datos
invalidos o alterar denominadores del KPI 3. Prioridad inicial: protege
el significado de las alertas sin levantar Kafka.
2. compileall en las cuatro carpetas. Evita integrar errores de sintaxis.
No prueba que existan todas las dependencias de un DAG ni que se importe bien.
3. docker compose config --quiet. Evita YAML de Compose mal formado y referencias
de configuracion que Compose no pueda resolver. No valida la salud del pipeline.

## Condicion de merge
Politica del equipo: no integrar un PR mientras CI RapidGo / validar falle
o siga pendiente. Para que GitHub lo impida tecnicamente, debe configurarse
ese check como obligatorio en una regla de proteccion de main.
Este documento define la condicion; no afirma que una proteccion este activa.
Las revisiones entre integrantes siguen siendo necesarias.

## Prueba costosa y evidencia complementaria
Una prueba end-to-end Producer -> Kafka -> Spark -> Data Lake -> Airflow, con
ventanas temporales y varios minutos de mensajes, es demasiado lenta y sensible
al arranque y al tiempo para cada push. Ejecutarla como smoke test separado,
manual antes de releases o programado, con datos sinteticos, tiempo maximo,
conteos esperados, logs y resultados conservados. No afirmar que se ejecuto:
esta entrega valida solo CI y reglas puras.

## Alcance de participacion
Los commits de esta actividad se realizaron con asistencia automatizada desde
la cuenta Dfao64. El actor identifica la cuenta autenticada; no demuestra por
si solo autoria manual, revision humana ni participacion de los otros miembros.
