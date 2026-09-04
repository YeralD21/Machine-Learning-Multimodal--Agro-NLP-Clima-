# resultados_v2_final — procedencia de la comparativa GC1

## gc1_comparativa_naive_sarima_prophet.csv

La columna `experimento` indica el experimento fuente de cada fila. El
mapeo vive en el dict `EXP_COMPARATIVA` de la celda que genera este CSV en
`notebooks/fase3_modelado/02_gc1_sarima_prophet.ipynb` — editar el CSV a
mano sin editar ese dict hace que la corrección se pierda en la siguiente
ejecución del notebook.

**SARIMA-Sutil está representado por `exp_002b_sarima_sutil_simple`**
(orden (1,1,1)(1,1,0,12), selección MANUAL por parsimonia).

`exp_002_sarima_sutil` (orden (1,1,3)(2,1,0,12), selección automática por
AIC) **NO** entra en la comparativa oficial: mejor AIC en train (1874.93 vs
1886.52) pero R²_test = −1.11 y MAE_test = 12031.69, es decir 2.6 veces
peor que el baseline Naive. Se conserva en `experimentos/` como registro
auditable del hallazgo de sobreajuste — ver DECISIONES_METODOLOGICAS.md,
sección "Sobreajuste por AIC en SARIMA-Sutil".

SARIMA-Dulce sigue usando `exp_002_sarima_dulce` (selección por AIC): esa
serie no presentó el problema (R²_test = 0.885).

## Dependencia de ejecución

La comparativa depende de `exp_002b_sarima_sutil_simple`, que genera
`notebooks/fase3_modelado/02b_diagnostico_sarima_sutil.ipynb`. Ese notebook
debe ejecutarse antes de la celda de comparativa de `02_gc1_*`. Si falta,
la celda lanza FileNotFoundError con el mensaje correspondiente en vez de
caer silenciosamente en el experimento equivocado.

## Advertencia sobre Δs (delta_s)

`delta_s` se calcula sobre **n_shock = 3** meses en test (2025-01, 2025-07,
2025-11). Es una muestra muy pequeña: la cifra es aritméticamente correcta
pero NO es una medida estable de resiliencia ante shocks. En particular, el
Δs = −79.25% de SARIMA-Sutil (exp_002b) no debe leerse como "el modelo
mejora 79% ante shocks" — está dominado por un único mes. Ver
DECISIONES_METODOLOGICAS.md, sección "Δs con n_shock=3".
