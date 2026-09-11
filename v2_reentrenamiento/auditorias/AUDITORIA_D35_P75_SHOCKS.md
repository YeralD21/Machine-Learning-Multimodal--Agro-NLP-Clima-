# 1. Alcance

Auditoria de solo lectura para D35-b. Objetivo: determinar como se calcula actualmente el umbral P75 usado para definir meses shock y que ventana temporal participa en ese calculo.

Restricciones respetadas durante la auditoria: no se modifico codigo, notebooks, datasets, scalers ni resultados; no se regenero Phase 2; no se entrenaron modelos; no se ejecuto evaluacion final; no se hizo commit; no se uso validation 2024 ni test 2025 para redefinir umbrales.

Estado del repositorio al auditar:

| Item | Valor |
|---|---|
| Branch | `v2-reentrenamiento` |
| HEAD | `bfd19a7c9de7fd5f1acbc8144429169dc3c4000e` |
| Raiz v2 observada | `v2_reentrenamiento/` |

# 2. Implementaciones localizadas

## Implementacion v2 activa / vigente

| Archivo | Seccion / celda / funcion | Tipo | Fragmento logico verificado |
|---|---|---|---|
| `v2_reentrenamiento/notebooks/fase3_modelado/01_baseline_naive.ipynb` | celda 2 | Codigo activo de experimento | Define `P75 = {'sutil': 23.2, 'dulce': 33.9}` como umbrales ya validados. No calcula `quantile(0.75)`. |
| `v2_reentrenamiento/notebooks/fase3_modelado/01_baseline_naive.ipynb` | celda 10 | Codigo activo de experimento | Calcula `df['var_pct'] = 100 * df[COL_TARGET[k]].pct_change()` sobre la serie completa disponible, filtra `test`, y define `test['shock'] = test['var_pct'].abs() > P75[k]`. |
| `v2_reentrenamiento/notebooks/fase3_modelado/01_baseline_naive.ipynb` | celda 12 | Codigo activo de persistencia | Guarda `metricas.json`, `config.yaml` y `predicciones.csv`, incluyendo `umbral_p75_pct`, `shock_test`, `mae_shock` y `delta_s_pct`. |
| `v2_reentrenamiento/notebooks/fase3_modelado/01_baseline_naive_ejecutado.ipynb` | celdas equivalentes | Notebook ejecutado | Contiene la misma logica y salidas ejecutadas; reporta Sutil 23.2%, Dulce 33.9%, 3 shocks por cultivar. |
| `v2_reentrenamiento/notebooks/fase3_modelado/02_gc1_sarima_prophet.ipynb` | celda 2 | Codigo activo de experimento | Define `P75 = {'sutil': 23.2, 'dulce': 33.9}` como umbrales ya validados. No calcula `quantile(0.75)`. |
| `v2_reentrenamiento/notebooks/fase3_modelado/02_gc1_sarima_prophet.ipynb` | celdas 11 y 14 | Codigo activo de experimento | Para SARIMA y Prophet: inicializa `serie_aux['shock'] = False` y asigna shock en test con `(100 * serie_aux[COL_TARGET[k]].pct_change().abs() > P75[k])` restringido a `participacion == 'test'`. |
| `v2_reentrenamiento/notebooks/fase3_modelado/02_gc1_sarima_prophet.ipynb` | celda 16 | Codigo activo de persistencia | Escribe `shock_test` y `delta_s` en artefactos de experimento. |
| `v2_reentrenamiento/notebooks/fase3_modelado/02_gc1_sarima_prophet_ejecutado.ipynb` | celdas equivalentes | Notebook ejecutado | Misma logica que el notebook fuente. |
| `v2_reentrenamiento/notebooks/fase3_modelado/02b_diagnostico_sarima_sutil.ipynb` | celda 1 | Diagnostico v2 historico / auxiliar | Define `P75_SUTIL = 23.2`. |
| `v2_reentrenamiento/notebooks/fase3_modelado/02b_diagnostico_sarima_sutil.ipynb` | celda 19, `shocks_test` | Diagnostico v2 historico / auxiliar | Calcula `var_pct = 100 * aux[COL].pct_change().abs()`, restringe mascara a test, y usa `P75_SUTIL`. |
| `v2_reentrenamiento/experimentos/*/config.yaml` | bloque `shock` | Artefacto v2 | Registra `definicion: '|100*(y_t - y_{t-1})/y_{t-1}| > P75'`, `escala: serie real en toneladas (NUNCA z-score)`, `umbral_p75_pct: 23.2` o `33.9`, `sobre: TEST unicamente`. |
| `v2_reentrenamiento/experimentos/*/metricas.json` | bloque `test` | Artefacto v2 | Registra `n_shock`, `mae_shock` y `delta_s_pct`. |
| `v2_reentrenamiento/experimentos/*/predicciones.csv` | columna `shock_test` | Artefacto v2 | Contiene la mascara shock ya materializada para cada fila. |
| `v2_reentrenamiento/experimentos/REGISTRO_MAESTRO.csv` | columnas `umbral_p75`, `n_shocks_test`, `mae_shock_test`, `delta_s_pct` | Registro v2 | Resume umbrales P75 y metricas shock por experimento. |

Busqueda dirigida en `v2_reentrenamiento/notebooks/fase3_modelado`, `v2_reentrenamiento/src`, `v2_reentrenamiento/experimentos` y `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`: no se encontraron llamadas activas a `quantile(0.75)` ni `percentile(..., 75)` para calcular estos umbrales P75 v2. Los umbrales se usan como constantes congeladas.

## Documentacion v2

| Archivo | Seccion | Tipo | Fragmento logico verificado |
|---|---|---|---|
| `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md` | split train/val/test, lineas 66-77 aprox. | Documentacion metodologica | Declara split 2016-2023 / 2024 / 2025 y verificacion de representacion de shocks: train 28%/25%, val 0%/25%, test 25%/25%. |
| `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md` | ajuste por lags, lineas 83-93 aprox. | Documentacion metodologica | Declara que train efectivo 2016-07..2023-12 mantiene 27.8% Sutil / 24.4% Dulce; meses eliminados con shock: 2016-02 y 2016-06. |
| `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md` | seccion Delta_s con n_shock=3, lineas 487-523 aprox. | Documentacion metodologica | Define Delta_s sobre meses de test que superan P75; para Sutil test 2025 identifica 2025-01, 2025-07 y 2025-11. Declara que Delta_s debe reportarse con `n_shock`. |
| `CONTEXTO_SESION_2026-09-07.md` | seccion metrica de shocks | Handoff / contexto | Formula `Delta_s = (MAE_shock - MAE_global) / MAE_global * 100`; shock = percentil 75 de variacion mensual en toneladas reales, no z-score; umbrales 23.2% y 33.9%. |
| `CONTEXTO_SESION_2026-09-08_METRICAS.md` | seccion 7 | Handoff / contexto | Define `mes i es SHOCK <=> |delta_y_i / y_(i-1)| > P75`; umbrales congelados Sutil 23.2%, Dulce 33.9%; se evalua solo sobre test; lista meses shock esperados. |

## Implementaciones historicas fuera de v2

| Archivo | Seccion / funcion | Tipo | Fragmento logico verificado |
|---|---|---|---|
| `resultados/verificacion_delta_s_completa/verificar_delta_s.py` | lineas 94-128 aprox. | Historico v1 / antecedente citado | Desnormaliza una serie nacional, calcula `var_pct = nacional_t.pct_change().abs() * 100`, define `TEST_START=2024-09-01`, `TEST_END=2025-08-01`, `var_test = var_pct.loc[test_dates]`, y `P75_TEST = np.percentile(var_test, 75)`. |
| `resultados/verificacion_delta_s_completa/verificar_delta_s.py` | lineas 159-162 aprox. | Historico v1 / antecedente citado | Calcula `mae_global`, `mae_shock` y `delta_s = (mae_shock - mae_global) / mae_global * 100`. |
| `resultados/verificacion_delta_s_completa/README.md` | seccion criterio de shock empirico | Historico v1 / antecedente citado | Documenta P75 de ventana test 2024-09..2025-08 = 3.67%, y sensibilidad P75 serie completa = 6.44%. |
| Scripts raiz `generar_delta_shocks.py`, `generar_shap_shocks.py`, `generar_6modelos_figuras.py`, `generar_impacto_economico.py` | varias secciones | Historico/dashboard v1 | Usan umbrales fijos tipo `>20` o criterios de dashboard; no corresponden al protocolo v2 de P75 23.2/33.9. |

# 3. Formula de cambio relativo

Formula v2 documentada y usada para clasificar shocks:

```text
abs(100 * (y_t - y_(t-1)) / y_(t-1)) > P75
```

Equivalente en codigo:

```python
100 * serie[target].pct_change().abs() > P75[cultivar]
```

Detalles verificados:

| Aspecto | Estado actual |
|---|---|
| Primer mes | `pct_change()` produce `NaN` en el primer registro de la serie usada. Al comparar con P75, `NaN > P75` no marca shock. |
| Enero 2025 | En v2 se calcula `pct_change()` sobre la serie completa disponible antes de filtrar test, por lo que 2025-01 usa 2024-12 como `y_(t-1)`. Esto esta comentado explicitamente en `01_baseline_naive.ipynb`: "serie completa (2025-01 usa dic-2024)". |
| Division entre cero | No se observo guard explicito contra `y_(t-1) = 0`. Pandas devolveria `inf` o `NaN` segun el caso. |
| Signo | Para la mascara se usa valor absoluto. En Naive se conserva `var_pct` firmado para imprimir, y se aplica `.abs()` al clasificar; en GC1 se calcula directamente con `.pct_change().abs()`. |
| Multiplicacion por 100 | Se multiplica por 100 antes de comparar con constantes expresadas en porcentaje: 23.2 y 33.9. |
| Escala | Documentacion y configs v2 indican "serie real en toneladas (NUNCA z-score)". En notebooks v2 se usan columnas objetivo crudas de `master_dataset_{sutil,dulce}_v2.csv` / `real_t` en predicciones. |

# 4. Fuente temporal del P75

Hallazgo principal: en la implementacion v2 activa no se encontro codigo que calcule el P75. Los notebooks y artefactos v2 usan umbrales hardcodeados:

```python
P75 = {'sutil': 23.2, 'dulce': 33.9}
```

Por tanto, no existe en el codigo v2 actual un `DataFrame`, `Series`, `quantile(0.75)` o `np.percentile(..., 75)` que permita trazar inequivocamente la ventana temporal usada para obtener 23.2% y 33.9%.

Clasificacion solicitada:

| Categoria | Dictamen para los constantes v2 23.2/33.9 |
|---|---|
| A) TRAIN-only 2016-01..2023-12 | No demostrable con el codigo actual. |
| B) TRAIN efectivo posterior a lags 2016-07..2023-12 | No demostrable con el codigo actual. |
| C) TRAIN + validation hasta 2024-12 | No demostrable con el codigo actual. |
| D) Serie completa incluye 2025 test | No demostrable con el codigo actual. |
| E) Otra | La implementacion actual usa constantes congeladas sin calculo reproducible en v2. |

Evidencia adicional:

- La documentacion v2 afirma que los umbrales estan "ya validados" o "congelados", pero no contiene el calculo reproducible que los genera.
- `DECISIONES_METODOLOGICAS.md` documenta porcentajes de shocks por split y meses eliminados tras lags, lo que prueba que la mascara fue evaluada por particion, pero no prueba cual fue la serie usada para estimar el P75.
- El antecedente historico citado en `01_baseline_naive.ipynb` (`resultados/verificacion_delta_s_completa/verificar_delta_s.py`) si calcula un P75, pero lo hace sobre ventana test 2024-09..2025-08 y con valores historicos 3.67% / 6.44% de otra serie. Ese antecedente no reproduce los constantes v2 23.2% y 33.9%.

# 5. P75 actual por cultivar

Valores actualmente usados en v2:

| Cultivar | P75 actual usado | Fuente verificada |
|---|---:|---|
| Sutil | 23.2% | `P75['sutil']` en notebooks fase3; `umbral_p75_pct: 23.2` en configs; `umbral_p75=23.2` en `REGISTRO_MAESTRO.csv`; documentacion de contexto. |
| Dulce | 33.9% | `P75['dulce']` en notebooks fase3; `umbral_p75_pct: 33.9` en configs; `umbral_p75=33.9` en `REGISTRO_MAESTRO.csv`; documentacion de contexto. |

No se encontro mayor precision decimal que una cifra decimal para estos constantes en los artefactos v2 revisados.

# 6. Construccion de shock_mask

Construccion actual en v2:

1. Se fija un umbral P75 por cultivar.
2. Se calcula la variacion porcentual mensual de la serie objetivo en toneladas reales con `pct_change()`.
3. El calculo de `pct_change()` se hace sobre la serie completa disponible en el dataframe de evaluacion, no sobre test aislado.
4. La mascara se restringe a test:

```python
shock_test = (100 * y.pct_change().abs() > P75[cultivar]) & (particion == 'test')
```

Consecuencias verificadas:

| Punto | Estado |
|---|---|
| Umbral antes/despues de aislar test | El umbral no se recalcula: ya existe como constante. La variacion se calcula antes de aislar test; la mascara se aplica solo a test. |
| Enero 2025 usa diciembre 2024 | Si. La serie usada conserva contexto de 2024-12 antes de filtrar test. |
| Exactamente 12 meses de test | En experimentos Naive, SARIMA-Dulce, Prophet-Sutil y Prophet-Dulce los CSV tienen 120 filas con 12 meses test; en `exp_002b_sarima_sutil_simple` el CSV tiene 114 filas por train efectivo, pero la mascara test contiene 3 shocks en 2025. |
| Umbral fijo en test | Si. No hay recalibracion mensual ni recalculo dentro de test. |
| Recalibracion con informacion 2025 | No se observo recalibracion activa con informacion de 2025. El riesgo no esta en la aplicacion de la mascara sino en la proveniencia no reproducible de los constantes P75. |

# 7. Shocks actuales en test 2025

Lectura directa de `predicciones.csv` v2 existentes:

| Cultivar | Experimentos verificados | Meses `shock_test=True` |
|---|---|---|
| Sutil | `exp_001_naive_sutil`, `exp_002b_sarima_sutil_simple`, `exp_003_prophet_sutil` | 2025-01, 2025-07, 2025-11 |
| Dulce | `exp_001_naive_dulce`, `exp_002_sarima_dulce`, `exp_003_prophet_dulce` | 2025-01, 2025-02, 2025-03 |

Estos meses coinciden con el estado previo esperado.

# 8. Formula actual de Delta_s

Formula actual documentada y usada:

```text
Delta_s = ((MAE_shock - MAE_global) / MAE_global) * 100
```

Verificacion:

| Aspecto | Estado actual |
|---|---|
| `MAE_global` | MAE sobre todo el conjunto test del experimento/modelo. |
| `MAE_shock` | MAE sobre la interseccion entre test y `shock_test=True`. |
| `MAE_global` incluye shocks | Si. `MAE_global` se calcula sobre todo test, incluyendo meses shock y no shock. |
| `MAE_nonshock` | No se observo como metrica persistida estandar en los artefactos v2 actuales; aparece mencionado como contraste puntual en documentacion para `exp_002`. |
| `n_shock` | Si se reporta en `metricas.json`, `REGISTRO_MAESTRO.csv` y documentacion. |

Valores del registro maestro revisado:

| Experimento | Cultivar | Modelo | P75 | n_shock | MAE_shock_test | Delta_s_pct |
|---|---|---|---:|---:|---:|---:|
| `exp_001_naive_sutil` | SUTIL | Naive | 23.2 | 3 | 11660.16 | 147.86 |
| `exp_001_naive_dulce` | DULCE | Naive | 33.9 | 3 | 138.26 | 63.23 |
| `exp_002b_sarima_sutil_simple` | SUTIL | SARIMA | 23.2 | 3 | 755.01 | -79.25 |
| `exp_002_sarima_sutil` | SUTIL | SARIMA | 23.2 | 3 | 12066.72 | 0.29 |
| `exp_002_sarima_dulce` | DULCE | SARIMA | 33.9 | 3 | 51.66 | -3.27 |
| `exp_003_prophet_sutil` | SUTIL | Prophet | 23.2 | 3 | 3016.42 | -18.08 |
| `exp_003_prophet_dulce` | DULCE | Prophet | 33.9 | 3 | 91.51 | 41.70 |

# 9. Diagnostico de leakage

Diagnostico: **AMBIGUOUS**.

Evidencia concreta:

- La aplicacion actual de la mascara shock en v2 no recalcula P75 con validation ni test; usa constantes congeladas 23.2% y 33.9%.
- No se encontro en v2 un calculo reproducible de `quantile(0.75)` o `percentile(..., 75)` que permita demostrar si esos constantes fueron estimados con TRAIN-only, TRAIN efectivo, TRAIN+validation, serie completa o test.
- La documentacion v2 no explicita la ventana exacta que genero 23.2% y 33.9%; solo declara que son umbrales P75 ya validados/congelados sobre variacion mensual en toneladas.
- Existe un antecedente historico citado que si calcula P75 sobre una ventana de test (`resultados/verificacion_delta_s_completa/verificar_delta_s.py`), lo cual seria leakage si se tomara como regla metodologica actual. Sin embargo, ese antecedente no reproduce los valores v2 23.2% y 33.9%.

Conclusion tecnica: no puede certificarse **CLEAN** porque no existe trazabilidad TRAIN-only del calculo original; tampoco puede certificarse **LEAKAGE** para los constantes v2 porque no hay evidencia directa del dataframe que los produjo. La proveniencia de P75 queda no auditable con el estado actual del repositorio.

# 10. Riesgo metodologico

Riesgo principal: si 23.2% y 33.9% fueron estimados usando validation y/o test, la definicion de meses shock incorporaria informacion fuera de TRAIN. Esto afectaria las metricas condicionadas (`MAE_shock`, `Delta_s`) porque el umbral que decide la mascara no seria ex-ante.

Riesgos secundarios:

- La mascara shock se usa de forma consistente en test, pero depende de constantes cuya procedencia no esta reproducida.
- `Delta_s` con `n_shock=3` por cultivar sigue siendo descriptivo y de alta varianza; la documentacion vigente ya advierte que no debe presentarse como prueba de resiliencia.
- No hay guard explicito contra division por cero en `pct_change()`.
- La existencia de scripts historicos con criterios distintos (`>20`, P75 test 2024-09..2025-08, z-score historico) aumenta el riesgo de confusion si se mezclan artefactos v1 y v2.

# 11. Correccion que seria necesaria, si aplica

No se implemento ninguna correccion. Para cerrar el riesgo metodologico haria falta decidir externamente una de estas acciones:

| Alternativa | Que corregiria | Consecuencia |
|---|---|---|
| Documentar la fuente exacta de 23.2% y 33.9% | Si existe evidencia externa de que fueron TRAIN-only, incorporarla como trazabilidad. | Mantiene valores actuales si se demuestra origen limpio. |
| Recalcular P75 exclusivamente con TRAIN | Elimina ambiguedad/leakage de umbral. | Puede cambiar umbrales, meses shock, `MAE_shock` y `Delta_s`; requeriria registrar una nueva decision D35-b. |
| Mantener P75 como descriptor ex-post | Acepta que la mascara es descriptiva, no evaluacion ex-ante. | Debe declararse explicitamente y no usarse como evidencia de generalizacion/robustez. |
| Separar `MAE_shock` de `Delta_s` oficial | Reduce dependencia de una mascara con `n=3`. | Cambia la interpretacion de resultados condicionados. |

# 12. Estado de D35-b

D35-b permanece pendiente de decision externa.

Hechos cerrados por esta auditoria:

- La implementacion v2 actual usa umbrales fijos: Sutil 23.2%, Dulce 33.9%.
- La mascara test 2025 actual marca Sutil: 2025-01, 2025-07, 2025-11.
- La mascara test 2025 actual marca Dulce: 2025-01, 2025-02, 2025-03.
- `Delta_s` actual se calcula como `(MAE_shock - MAE_global) / MAE_global * 100`.
- `MAE_global` incluye todos los meses de test, incluidos los meses shock.
- No se encontro calculo v2 activo que permita trazar la ventana temporal original del P75.

Decision pendiente:

- Definir si los umbrales P75 23.2% y 33.9% pueden conservarse con evidencia externa de origen TRAIN-only, o si deben recalcularse/redefinirse bajo una regla explicitamente aprobada.
