# AUDITORIA METODOLOGICA SARIMA ROLLING ONE-STEP

Fecha: 2026-09-12

Alcance: auditoria metodologica y tecnica para definir el cierre de
GC1/SARIMA rolling one-step en la comparacion principal v2. No se generan
metricas oficiales nuevas, no se abre TEST rolling 2025, no se modifica GC2,
GC3/GE, Naive, datasets ni modelos previos.

## Actualizacion de cierre metodologico

D46-D50 quedaron **CERRADAS** el 2026-09-12 y fueron registradas en
`DECISIONES_METODOLOGICAS.md`.

Especificaciones finales por cultivar:

```text
Sutil: order=(1,1,1), seasonal_order=(1,1,0,12)
Dulce: order=(1,0,0), seasonal_order=(0,1,0,12)
```

Ambas tienen respaldo en el GC1 fixed-origin congelado previo:

```text
Sutil -> exp_002b_sarima_sutil_simple/config.yaml
Dulce -> exp_002_sarima_dulce/config.yaml
```

Pre-flight final corregido:

```text
v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_FINAL_SARIMA_ROLLING.md
v2_reentrenamiento/auditorias/sarima_rolling_preflight_final.json
status = approved
38/38 controles OK
TEST 2025 cargado = NO
forecasts TEST = NO
metricas TEST = NO
```

---

## A. Implementacion SARIMA existente

### Hecho verificado en codigo

Archivo principal:

```text
v2_reentrenamiento/notebooks/fase3_modelado/02_gc1_sarima_prophet.ipynb
```

Implementacion observada:

```text
from statsmodels.tsa.statespace.sarimax import SARIMAX
seed = 42 via np.random.seed(SEED)
dataset = master_dataset_{sutil,dulce}_v2.csv
serie = cruda, toneladas, sin StandardScaler
split = train 2016-07..2023-12 (90), val 2024-01..2024-12 (12), test 2025-01..2025-12 (12)
grid = p 0..3, d 0..1, q 0..3, P 0..2, D 0..1, Q 0..2, m=12
fit = SARIMAX(...).fit(disp=False, maxiter=200/500)
trend = 'c'
initialization = 'approximate_diffuse'
enforce_stationarity = False
enforce_invertibility = False
seleccion = top-5 por AIC en TRAIN, desempate por MAE en VAL
```

Evaluacion SARIMA fixed-origin corregida:

```text
fit en TRAIN solamente
f_val_test = fit.forecast(steps=N_VAL + N_TEST)
VAL = pasos 1..12 desde fin de TRAIN
TEST = pasos 13..24 desde fin de TRAIN
```

Archivos de experimentos existentes:

```text
v2_reentrenamiento/experimentos/exp_002b_sarima_sutil_simple/config.yaml
v2_reentrenamiento/experimentos/exp_002b_sarima_sutil_simple/metricas.json
v2_reentrenamiento/experimentos/exp_002b_sarima_sutil_simple/predicciones.csv
v2_reentrenamiento/experimentos/exp_002_sarima_dulce/config.yaml
v2_reentrenamiento/experimentos/exp_002_sarima_dulce/metricas.json
v2_reentrenamiento/experimentos/exp_002_sarima_dulce/predicciones.csv
```

No se encontro modelo SARIMA serializado ni parametros guardados para v2 GC1.
La reproducibilidad de rolling debe partir de re-ajustar el modelo desde la
serie TRAIN con configuracion congelada, no de cargar un objeto fit previo.

### Antecedente documental

`DECISIONES_METODOLOGICAS.md` registra:

```text
bug historico de horizonte SARIMA;
exp_002b_sarima_sutil_simple como SARIMA-Sutil representativo;
orden parsimonioso (1,1,1)(1,1,0,12);
limitacion del pre-filtro por AIC en Sutil;
advertencia de Delta_s con n_shock=3.
```

`resultados_v2_final/README.md` confirma que la comparativa GC1 vigente usa:

```text
SARIMA-Sutil = exp_002b_sarima_sutil_simple, order=(1,1,1), seasonal_order=(1,1,0,12)
SARIMA-Dulce = exp_002_sarima_dulce, order=(1,0,0), seasonal_order=(0,1,0,12)
```

Este punto quedo resuelto en D49: no se impone una unica especificacion a
ambos cultivares. La evaluacion rolling conserva la especificacion fixed-origin
oficial vigente de cada cultivar.

---

## B. Bug historico y estado corregido

### Hecho verificado en codigo

El notebook actual ya contiene la correccion:

```python
f_val_test = np.asarray(fit.forecast(steps=N_VAL + N_TEST))
pred_val = f_val_test[:N_VAL]
pred_test = f_val_test[N_VAL:]
```

El bug historico era:

```python
pred_val = fit.forecast(steps=12)
pred_test = fit.forecast(steps=12)
```

Ambas llamadas salian desde el mismo final de TRAIN, por lo que TEST replicaba
VAL. La correccion fixed-origin evita ese bug porque usa una sola proyeccion de
24 pasos y separa pasos 1..12 de pasos 13..24.

### Implicacion para rolling

En rolling one-step el bug no debe reaparecer si se cumple:

```text
forecast(steps=1) exactamente una vez por origen;
despues de observar y_t real, actualizar estado con append(refit=False);
no llamar dos veces forecast(steps=12) desde el mismo estado.
```

---

## C. Order/seasonal_order congelados

Especificacion solicitada para esta auditoria:

```text
order = (1,1,1)
seasonal_order = (1,1,0,12)
```

Configuracion de ajuste a preservar desde GC1:

```text
trend = 'c'
initialization = 'approximate_diffuse'
enforce_stationarity = False
enforce_invertibility = False
maxiter = 500 para ajuste final
```

Respaldo documental:

```text
Sutil fixed-origin ya usa esa especificacion via exp_002b.
Dulce fixed-origin vigente usa (1,0,0)(0,1,0,12), segun config.yaml.
```

Decision D49: conservar especificacion por cultivar. Sutil usa
`(1,1,1)(1,1,0,12)` y Dulce usa `(1,0,0)(0,1,0,12)`.

---

## D. Refit vs update-state

### Opcion A: refit mensual

Definicion:

```text
incorporar y_t real;
volver a estimar todos los parametros SARIMA cada mes;
mantener solo order/seasonal_order fijos.
```

Ventaja:

```text
maxima adaptacion del modelo estadistico a nueva informacion.
```

Riesgos:

```text
cambia parametros dentro de VAL/TEST;
usa observaciones TEST para alterar coeficientes antes de pronosticos posteriores;
da una ventaja operativa frente a XGBoost/GC3/GE, que permanecen congelados;
convierte la evaluacion en un procedimiento de reestimacion secuencial, no en un modelo congelado;
complica auditoria, trazabilidad y comparacion principal.
```

Dictamen: no recomendada para la comparacion principal v2.

### Opcion B: parametros congelados + actualizacion de estado

Definicion:

```text
fit una sola vez en periodo permitido;
mantener coeficientes congelados;
forecast(steps=1);
append nueva observacion real con refit=False;
repetir.
```

Soporte tecnico verificado en entorno local:

```text
statsmodels = 0.14.6
SARIMAXResults.append(endog, exog=None, refit=False, fit_kwargs=None, copy_initialization=False, **kwargs)
```

La documentacion instalada indica que `append` recrea el results object con
datos nuevos anexados y que `refit=False` usa los parametros del objeto actual
para filtrar/suavizar el nuevo dataset. Esto es exactamente el mecanismo de
actualizacion de estado sin reestimacion de coeficientes.

Dictamen: recomendada.

### Opcion C: fit TRAIN+VAL una vez

Definicion:

```text
ajustar parametros una sola vez con datos hasta 2024-12;
TEST queda fuera del fit;
durante TEST actualizar estado sin refit.
```

Ventaja:

```text
usa toda la informacion historica disponible antes de TEST;
metodologicamente legitima si la especificacion ya esta congelada y VAL no participa en seleccion.
```

Riesgos:

```text
GC2/XGBoost, GC3/GE y Naive ya quedaron bajo protocolos donde los modelos aprendidos no se reentrenaron con VAL para TEST;
rompe simetria de parametros congelados desde TRAIN;
puede hacer que SARIMA tenga informacion de estimacion adicional respecto de modelos ya cerrados;
requiere declarar cambio frente al protocolo GC1 fixed-origin previo.
```

Dictamen: metodologicamente defendible como analisis alternativo, pero no
recomendada para la tabla principal si se prioriza simetria con los demas
modelos congelados.

---

## E. TRAIN-only vs TRAIN+VAL fit

### Fit solo TRAIN + update VAL/TEST sin refit

Flujo:

```text
fit parametros en TRAIN 2016-07..2023-12;
VAL 2024 se usa como periodo de actualizacion de estado sin refit;
al cierre de 2024-12 se emite forecast(steps=1) para 2025-01;
durante TEST 2025 se actualiza estado con y_t real sin refit;
no se reestiman coeficientes en VAL ni TEST.
```

Ventajas:

```text
preserva separacion train/val/test historica;
no usa VAL para estimar parametros;
maximiza comparabilidad con modelos congelados;
usa la observacion real t igual que Naive/XGBoost/GC3/GE;
evita elegir configuracion por desempeno TEST.
```

Riesgos:

```text
fit con solo 90 observaciones para SARIMA estacional;
VAL no mejora parametros, solo estado;
si la serie cambia de nivel en 2024, los coeficientes no se adaptan.
```

### Fit TRAIN+VAL una vez + update TEST sin refit

Ventajas:

```text
mas datos para estimar parametros antes de TEST;
VAL ya no seria usada para seleccion si la especificacion esta cerrada.
```

Riesgos:

```text
asimetria con XGBoost/GC3/GE ya entrenados;
posible percepcion de que SARIMA recibio un protocolo posterior distinto;
requiere reapertura metodologica de GC1 para tabla principal.
```

Recomendacion: fit solo TRAIN y usar VAL 2024 unicamente para actualizar estado
sin refit antes de TEST.

Nota: la solicitud menciona una vez TRAIN `2016-01..2023-12`, pero los
artefactos GC1 v2 y el notebook definen TRAIN evaluado como
`2016-07..2023-12` con buffer `2016-01..2016-06`. Para no alterar el split
cerrado, esta auditoria recomienda mantener `2016-07..2023-12` como periodo de
estimacion.

---

## F. Comparabilidad con Naive/XGBoost/GC3/GE

La igualdad relevante no es que todos los modelos tengan el mismo mecanismo
interno, sino que compartan:

```text
mismo origen informativo;
mismo horizonte t -> t+1;
mismo periodo TEST 2025;
misma disponibilidad de y_t real;
no uso de TEST para modificar parametros/configuracion.
```

Comparacion:

```text
Naive: usa y_t real para predecir y_(t+1), sin parametros entrenables.
XGBoost: modelo congelado, X_t -> y_(t+1), puede incluir produccion observada en t como predictor.
GC3/GE: red congelada, secuencia termina en t, produccion_t conocida en el origen.
SARIMA recomendado: parametros congelados, estado actualizado con y_t real, forecast(steps=1).
```

Por comparabilidad operacional, SARIMA rolling debe usar parametros congelados
y actualizacion de estado sin refit.

---

## G. Recomendacion exacta

Recomendacion principal:

```text
Fit SARIMA una sola vez en TRAIN 2016-07..2023-12.
Usar especificacion por cultivar: Sutil (1,1,1)(1,1,0,12); Dulce (1,0,0)(0,1,0,12).
Preservar trend='c', initialization='approximate_diffuse', enforce_stationarity=False, enforce_invertibility=False.
Recorrer VAL 2024 con forecast(steps=1) + append([y_real], refit=False) para actualizar estado hasta Dec-2024.
En una etapa posterior y solo con autorizacion explicita, recorrer TEST 2025 igual: forecast(steps=1), luego append real sin refit.
No reestimar coeficientes en VAL ni TEST.
No calcular metricas TEST hasta autorizacion.
```

Esta recomendacion queda cerrada por D46-D50 y respeta el split v2 cerrado
`2016-07..2023-12`.

---

## H. API statsmodels propuesta

API local verificada:

```text
statsmodels 0.14.6
SARIMAX(...)
fit = model.fit(disp=False, maxiter=500)
pred = fit.forecast(steps=1)
fit = fit.append([y_real], refit=False)
```

Razon para `append`:

```text
conserva un results object con la historia original mas observaciones nuevas;
permite forecast posterior desde el estado actualizado;
refit=False mantiene parametros actuales.
```

`extend` tambien existe en la API, pero para este protocolo `append(refit=False)`
es mas auditable porque el objeto resultante conserva la serie completa anexada
y facilita verificar fechas/longitud. `apply(refit=False)` no es la opcion
natural aqui porque esta pensado para aplicar parametros a otro dataset.

---

## I. Fechas y flujo temporal

Pre-flight ejecutado solo sobre VAL:

```text
TRAIN fit: 2016-07..2023-12
VAL rolling dry-run: 2024-01..2024-12
TEST forecast ejecutado: NO
```

Flujo TEST futuro propuesto:

```text
estado actualizado hasta 2024-12 -> forecast 2025-01
append y_real 2025-01, refit=False -> forecast 2025-02
append y_real 2025-02, refit=False -> forecast 2025-03
...
append y_real 2025-11, refit=False -> forecast 2025-12
```

Total futuro esperado:

```text
12 predicciones por cultivar
24 predicciones SARIMA rolling total si se evalua sutil y dulce
```

---

## J. Riesgos

Riesgos metodologicos:

```text
rolling one-step no es comparable numericamente al fixed-origin de GC1, debe reportarse como evaluacion adicional preespecificada;
no debe interpretarse como rescate del modelo por desempeno posterior.
```

Riesgos tecnicos:

```text
no existe modelo SARIMA serializado, por lo que debe re-ajustarse desde TRAIN;
si statsmodels cambia de version, conviene congelar version 0.14.6 en metadata;
con enforce_stationarity=False/enforce_invertibility=False se preserva el antecedente, pero se permite frontera de estabilidad;
append requiere que endog nuevo tenga formato compatible con el endog original.
```

---

## K. Decisiones metodologicas

| ID | Decision | Alternativas | Recomendacion | Justificacion | Estado |
|---|---|---|---|---|---|
| D46 | Tipo de actualizacion rolling | A: refit mensual; B: parametros congelados + update-state; C: fit TRAIN+VAL una vez; D: otra variante tecnica | B | Misma informacion operacional t -> t+1 sin cambiar coeficientes durante VAL/TEST; comparable con modelos congelados | CERRADA |
| D47 | Periodo de estimacion de parametros | TRAIN 2016-07..2023-12; TRAIN+VAL 2016-07..2024-12; usar buffer 2016-01..2023-12 | TRAIN 2016-07..2023-12 | Respeta split v2 existente y evita redefinir muestra de estimacion | CERRADA |
| D48 | Tratamiento de VAL 2024 | Evaluar metricas oficiales; usar para refit; usar solo para actualizar estado sin refit | Usar solo para actualizar estado sin refit | Permite llegar al origen Dec-2024 sin usar VAL para reestimar ni seleccionar | CERRADA |
| D49 | Especificacion por cultivar | Orden uniforme; conservar especificaciones GC1 previas | Sutil (1,1,1)(1,1,0,12); Dulce (1,0,0)(0,1,0,12) | Ambas especificaciones estan verificadas en configs GC1 congeladas | CERRADA |
| D50 | Artefactos/reproducibilidad | Solo CSV; CSV+JSON+auditoria+hashes; serializar modelo por paso | CSV+JSON+auditoria+hashes y parametros inicial/final | No hay modelo previo guardado; la trazabilidad debe documentar config, version, params, fechas y ausencia de refit | CERRADA |

---

## Pre-flight tecnico ejecutado

Archivos creados:

```text
v2_reentrenamiento/auditorias/auditar_sarima_rolling_preflight.py
v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_SARIMA_ROLLING.md
v2_reentrenamiento/auditorias/sarima_rolling_preflight_dry_run.json
```

Resultado:

```text
status = approved
controles = 18
fallos = 0
TEST 2025 cargado = NO
metricas oficiales nuevas = NO
forecast TEST = NO
```

Controles clave:

```text
sutil TRAIN n=90, 2016-07..2023-12
sutil VAL n=12, 2024-01..2024-12
sutil no TEST loaded
sutil forecast steps=1 all VAL
sutil append refit=False all VAL
sutil coefficients unchanged
dulce TRAIN n=90, 2016-07..2023-12
dulce VAL n=12, 2024-01..2024-12
dulce no TEST loaded
dulce forecast steps=1 all VAL
dulce append refit=False all VAL
dulce coefficients unchanged
```

Conclusion: el protocolo metodologico queda cerrado por D46-D50. El pre-flight
final corregido confirma que la implementacion TRAIN+VAL es tecnicamente
correcta y queda lista para solicitar apertura unica de TEST 2025.
