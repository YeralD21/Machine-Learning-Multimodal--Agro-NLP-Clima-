# AUDITORIA D35 - Estacionalidad TRAIN para MASE/RMSSE

## 1. Alcance

Esta auditoria empirica previa a D35 evalua, usando exclusivamente TRAIN, si las series mensuales de produccion de Limon Sutil y Limon Dulce muestran evidencia descriptiva relevante para comparar dos denominadores candidatos de errores escalados:

- m=1: benchmark Naive no estacional yhat_t = y_(t-1).
- m=12: benchmark Seasonal Naive anual yhat_t = y_(t-12).

No se entrenan modelos, no se selecciona un benchmark oficial y no se cierra D35. La decision metodologica final queda pendiente.

## 2. Datos utilizados

| Cultivar | Archivo fuente | Columna objetivo | Fecha inicial | Fecha final | N | NaN target | Duplicados fecha | Frecuencia mensual | Meses faltantes |
|---|---|---|---|---|---:|---:|---:|---|---|
| Sutil | v2_reentrenamiento/data/processed/master_dataset_sutil_v2.csv | produccion_t_sutil | 2016-01 | 2023-12 | 96 | 0 | 0 | regular MS | 0 |
| Dulce | v2_reentrenamiento/data/processed/master_dataset_dulce_v2.csv | produccion_t_dulce | 2016-01 | 2023-12 | 96 | 0 | 0 | regular MS | 0 |

Se reconstruyeron inequivocamente las 96 observaciones TRAIN originales para ambos cultivares: 2016-01 a 2023-12.

## 3. Verificacion anti-leakage

- Ninguna observacion posterior a 2023-12 participa en los calculos.
- No se usan estadisticas de validation 2024.
- No se usan estadisticas de test 2025.
- No se usa el scaler v2 para decidir estacionalidad.
- Todos los calculos se hacen sobre valores originales de produccion en toneladas.
- La auditoria se realiza sobre la serie objetivo TRAIN original de 96 meses, no sobre las 90 filas posteriores al drop de lags.

## 4. Estadisticos descriptivos

### Sutil

| Estadistico | Valor |
|---|---:|
| Media | 23,553.5325 |
| Mediana | 23,097.6765 |
| Desviacion estandar | 7,898.7695 |
| Minimo | 4,851.6910 |
| Maximo | 41,867.7190 |
| Coeficiente de variacion | 0.3354 |
| Tendencia lineal, pendiente t/mes | 90.1446 |
| Tendencia lineal, pendiente t/anio | 1,081.7356 |

### Dulce

| Estadistico | Valor |
|---|---:|
| Media | 395.8437 |
| Mediana | 374.9950 |
| Desviacion estandar | 145.4586 |
| Minimo | 135.7000 |
| Maximo | 663.1500 |
| Coeficiente de variacion | 0.3675 |
| Tendencia lineal, pendiente t/mes | -0.1712 |
| Tendencia lineal, pendiente t/anio | -2.0540 |

## 5. ACF/PACF

La ACF se calcula hasta lag 24 sobre TRAIN. La PACF se calcula mediante recursion Durbin-Levinson sobre la misma ACF, sin instalar dependencias nuevas.

### Sutil - lags solicitados

| Lag | ACF | PACF |
|---:|---:|---:|
| 1 | 0.8448 | 0.8448 |
| 6 | -0.2316 | 0.0742 |
| 12 | 0.5652 | -0.0270 |
| 13 | 0.4705 | -0.0849 |
| 24 | 0.3422 | -0.0606 |

### Sutil - mayores autocorrelaciones absolutas

| Rank | Lag | ACF | Abs(ACF) |
|---:|---:|---:|---:|
| 1 | 1 | 0.8448 | 0.8448 |
| 2 | 2 | 0.5751 | 0.5751 |
| 3 | 12 | 0.5652 | 0.5652 |
| 4 | 11 | 0.5356 | 0.5356 |
| 5 | 13 | 0.4705 | 0.4705 |
| 6 | 18 | -0.4468 | 0.4468 |
| 7 | 10 | 0.3942 | 0.3942 |
| 8 | 17 | -0.3735 | 0.3735 |

Figura: v2_reentrenamiento/resultados_v2_final/auditoria_d35/acf_sutil.svg

### Dulce - lags solicitados

| Lag | ACF | PACF |
|---:|---:|---:|
| 1 | 0.8118 | 0.8118 |
| 6 | -0.8201 | -0.3227 |
| 12 | 0.8437 | -0.0249 |
| 13 | 0.6646 | -0.3244 |
| 24 | 0.7137 | -0.0436 |

### Dulce - mayores autocorrelaciones absolutas

| Rank | Lag | ACF | Abs(ACF) |
|---:|---:|---:|---:|
| 1 | 12 | 0.8437 | 0.8437 |
| 2 | 6 | -0.8201 | 0.8201 |
| 3 | 1 | 0.8118 | 0.8118 |
| 4 | 11 | 0.7398 | 0.7398 |
| 5 | 5 | -0.7355 | 0.7355 |
| 6 | 24 | 0.7137 | 0.7137 |
| 7 | 18 | -0.7016 | 0.7016 |
| 8 | 7 | -0.7014 | 0.7014 |

Figura: v2_reentrenamiento/resultados_v2_final/auditoria_d35/acf_dulce.svg

## 6. Naive t-1

Benchmark in-sample: yhat_t = y_(t-1) para t=2..N.

| Cultivar | N efectivo | MAE_naive1_train | RMSE_naive1_train | MSE_naive1_train |
|---|---:|---:|---:|---:|
| Sutil | 95 | 3,374.5715 | 4,330.0810 | 18,749,601.6694 |
| Dulce | 95 | 73.3380 | 86.0511 | 7,404.7927 |

## 7. Seasonal Naive t-12

Benchmark in-sample: yhat_t = y_(t-12) para t=13..N.

| Cultivar | N efectivo | MAE_snaive12_train | RMSE_snaive12_train | MSE_snaive12_train |
|---|---:|---:|---:|---:|
| Sutil | 84 | 5,798.4723 | 7,040.1760 | 49,564,078.4262 |
| Dulce | 84 | 23.2020 | 29.3226 | 859.8148 |

## 8. Comparacion en periodo comun

Comparacion obligatoria sobre exactamente el mismo subconjunto temporal: t=13..N.

### Sutil

| Benchmark | Periodo comun | N efectivo | MAE | RMSE |
|---|---|---:|---:|---:|
| Naive t-1 | t=13..N | 84 | 3,434.6472 | 4,425.5176 |
| Seasonal Naive t-12 | t=13..N | 84 | 5,798.4723 | 7,040.1760 |

| Ratio | Valor | Lectura descriptiva |
|---|---:|---|
| MAE_snaive12_common / MAE_naive1_common | 1.6882 | Naive t-1 tuvo menor MAE TRAIN en el periodo comun. |
| RMSE_snaive12_common / RMSE_naive1_common | 1.5908 | Naive t-1 tuvo menor RMSE TRAIN en el periodo comun. |

### Dulce

| Benchmark | Periodo comun | N efectivo | MAE | RMSE |
|---|---|---:|---:|---:|
| Naive t-1 | t=13..N | 84 | 72.9575 | 86.1591 |
| Seasonal Naive t-12 | t=13..N | 84 | 23.2020 | 29.3226 |

| Ratio | Valor | Lectura descriptiva |
|---|---:|---|
| MAE_snaive12_common / MAE_naive1_common | 0.3180 | Seasonal Naive tuvo menor MAE TRAIN en el periodo comun. |
| RMSE_snaive12_common / RMSE_naive1_common | 0.3403 | Seasonal Naive tuvo menor RMSE TRAIN en el periodo comun. |

## 9. Denominadores candidatos MASE/RMSSE

No se calcula MASE/RMSSE de ningun modelo. Solo se reportan los denominadores candidatos en unidades originales.

### Sutil

| Denominador candidato | Formula en TRAIN | N efectivo | Valor |
|---|---|---:|---:|
| D_MASE_1 | mean(abs(y_t - y_t-1)), t=2..N | 95 | 3,374.5715 |
| D_MASE_12 | mean(abs(y_t - y_t-12)), t=13..N | 84 | 5,798.4723 |
| D_RMSSE_1 | mean((y_t - y_t-1)^2), t=2..N | 95 | 18,749,601.6694 |
| D_RMSSE_12 | mean((y_t - y_t-12)^2), t=13..N | 84 | 49,564,078.4262 |

### Dulce

| Denominador candidato | Formula en TRAIN | N efectivo | Valor |
|---|---|---:|---:|
| D_MASE_1 | mean(abs(y_t - y_t-1)), t=2..N | 95 | 73.3380 |
| D_MASE_12 | mean(abs(y_t - y_t-12)), t=13..N | 84 | 23.2020 |
| D_RMSSE_1 | mean((y_t - y_t-1)^2), t=2..N | 95 | 7,404.7927 |
| D_RMSSE_12 | mean((y_t - y_t-12)^2), t=13..N | 84 | 859.8148 |

## 10. Patron mensual

La amplitud estacional descriptiva se calcula como (max(media_mensual) - min(media_mensual)) / media_global.

| Cultivar | Amplitud estacional descriptiva |
|---|---:|
| Sutil | 64.19% |
| Dulce | 104.90% |

### Sutil

| Mes | N | Media | Mediana | Desv. est. |
|---|---:|---:|---:|---:|
| Enero | 8 | 27,727.0926 | 28,196.0750 | 6,588.9949 |
| Febrero | 8 | 30,938.4595 | 30,388.1915 | 6,689.3721 |
| Marzo | 8 | 30,872.7820 | 31,400.1060 | 7,143.2110 |
| Abril | 8 | 29,055.7186 | 30,488.5095 | 5,342.3391 |
| Mayo | 8 | 27,874.2685 | 27,872.3090 | 6,125.0239 |
| Junio | 8 | 22,745.1063 | 23,801.2820 | 4,365.7513 |
| Julio | 8 | 17,577.0979 | 17,833.4905 | 4,737.0285 |
| Agosto | 8 | 15,820.1675 | 16,383.3010 | 6,510.2338 |
| Septiembre | 8 | 16,489.0023 | 17,023.0080 | 6,097.8733 |
| Octubre | 8 | 17,845.3969 | 19,108.3540 | 6,295.8740 |
| Noviembre | 8 | 20,556.3394 | 22,174.7855 | 5,813.8053 |
| Diciembre | 8 | 25,140.9590 | 25,688.5255 | 5,499.8723 |

Figura: v2_reentrenamiento/resultados_v2_final/auditoria_d35/patron_mensual_sutil.svg

### Dulce

| Mes | N | Media | Mediana | Desv. est. |
|---|---:|---:|---:|---:|
| Enero | 8 | 269.4600 | 266.0650 | 20.8386 |
| Febrero | 8 | 377.1858 | 374.9950 | 27.2710 |
| Marzo | 8 | 508.4135 | 524.9750 | 60.8460 |
| Abril | 8 | 602.7970 | 607.5750 | 38.7095 |
| Mayo | 8 | 595.1995 | 599.4150 | 17.7166 |
| Junio | 8 | 569.6719 | 560.1995 | 38.0325 |
| Julio | 8 | 445.0144 | 440.7295 | 30.3295 |
| Agosto | 8 | 360.4306 | 351.6685 | 28.6807 |
| Septiembre | 8 | 339.5984 | 333.0845 | 32.0337 |
| Octubre | 8 | 290.6760 | 287.8195 | 25.1245 |
| Noviembre | 8 | 204.1025 | 200.6650 | 23.5667 |
| Diciembre | 8 | 187.5750 | 189.8900 | 31.2321 |

Figura: v2_reentrenamiento/resultados_v2_final/auditoria_d35/patron_mensual_dulce.svg

## 11. STL, si se pudo ejecutar

No se ejecuto STL. Motivo: el entorno Python/venv no esta funcional en esta sesion (py no encuentra Python instalado y python no esta en PATH), y no se instalaron ni modificaron dependencias. Para respetar la restriccion de no modificar entorno, se omite esta seccion opcional.

## 12. Resultados Sutil

- TRAIN reconstruido: 2016-01..2023-12, N=96, sin NaN, sin duplicados y sin meses faltantes.
- ACF lag 12: 0.5652.
- ACF lag 24: 0.3422.
- Naive t-1 completo: MAE=3,374.5715, RMSE=4,330.0810, N=95.
- Seasonal Naive t-12 completo: MAE=5,798.4723, RMSE=7,040.1760, N=84.
- En periodo comun t=13..N, ratio_MAE=1.6882 y ratio_RMSE=1.5908.
- Amplitud mensual descriptiva: 64.19%.

## 13. Resultados Dulce

- TRAIN reconstruido: 2016-01..2023-12, N=96, sin NaN, sin duplicados y sin meses faltantes.
- ACF lag 12: 0.8437.
- ACF lag 24: 0.7137.
- Naive t-1 completo: MAE=73.3380, RMSE=86.0511, N=95.
- Seasonal Naive t-12 completo: MAE=23.2020, RMSE=29.3226, N=84.
- En periodo comun t=13..N, ratio_MAE=0.3180 y ratio_RMSE=0.3403.
- Amplitud mensual descriptiva: 104.90%.

## 14. Interpretacion estrictamente descriptiva

- Sutil: en TRAIN comun t=13..N, el ratio MAE seasonal/naive es 1.6882. Esto describe que Naive t-1 obtuvo menor MAE TRAIN que Seasonal Naive en ese periodo.
- Sutil: en TRAIN comun t=13..N, el ratio RMSE seasonal/naive es 1.5908. Esto describe que Naive t-1 obtuvo menor RMSE TRAIN que Seasonal Naive en ese periodo.
- Dulce: en TRAIN comun t=13..N, el ratio MAE seasonal/naive es 0.3180. Esto describe que Seasonal Naive obtuvo menor MAE TRAIN que Naive t-1 en ese periodo.
- Dulce: en TRAIN comun t=13..N, el ratio RMSE seasonal/naive es 0.3403. Esto describe que Seasonal Naive obtuvo menor RMSE TRAIN que Naive t-1 en ese periodo.
- Estas observaciones no cierran D35 ni determinan automaticamente el denominador oficial.

## 15. Limitaciones

- La auditoria usa solo TRAIN, como corresponde para evitar leakage, pero por ello no evalua desempeno fuera de muestra.
- La ACF/PACF es diagnostico descriptivo, no prueba formal definitiva de estacionalidad.
- La comparacion de benchmarks in-sample no decide por si sola el denominador metodologico.
- No se ejecuto STL por falta de entorno Python funcional y por la restriccion de no modificar dependencias.
- No se probaron periodos alternativos; solo m=1 vs m=12.

## 16. Preguntas pendientes para D35

1. Si se reporta MASE, debe escalarse contra Naive t-1 o contra Seasonal Naive t-12?
2. Si se reporta RMSSE, debe compartir el mismo m que MASE o justificarse por separado?
3. El denominador sera unico por cultivar y calculado solo en TRAIN?
4. Como se explicara la tension entre benchmark oficial Baseline=Naive t-1 y posible escalado estacional m=12?
5. Se reportara RelMAE como metrica secundaria frente al baseline oficial?
6. Como se comunicara que esta auditoria aporta evidencia empirica TRAIN, pero no decide D35 automaticamente?
