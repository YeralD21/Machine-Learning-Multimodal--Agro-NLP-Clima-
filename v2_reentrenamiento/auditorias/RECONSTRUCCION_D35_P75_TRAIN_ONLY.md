# RECONSTRUCCION D35 - P75 TRAIN-ONLY PARA SHOCKS

## 1. Motivo de la reconstruccion

La auditoria `AUDITORIA_D35_P75_SHOCKS.md` determino que los umbrales historicos usados en v2, Sutil 23.2% y Dulce 33.9%, aparecen como constantes congeladas y no tienen un calculo reproducible trazable en el repositorio.

Diagnostico previo: **AMBIGUOUS**. No se demostro leakage, pero tampoco pudo demostrarse que esos umbrales heredados/no reproducibles hubieran sido estimados exclusivamente con TRAIN.

Esta reconstruccion calcula desde cero el P75 oficial candidato usando solo la serie objetivo TRAIN 2016-01..2023-12. No entrena modelos, no recalcula metricas finales, no modifica datasets, no modifica scalers y no cierra D35-b.

## 2. Fuente de datos

Fuentes usadas exclusivamente:

| Cultivar | Archivo | Target |
|---|---|---|
| Sutil | `v2_reentrenamiento/data/processed/master_dataset_sutil_v2.csv` | `produccion_t_sutil` |
| Dulce | `v2_reentrenamiento/data/processed/master_dataset_dulce_v2.csv` | `produccion_t_dulce` |

La serie utilizada es la produccion original en toneladas. No se usaron datasets supervisados posteriores al drop de lags, ni archivos escalados, ni validation 2024, ni test 2025 para estimar el umbral.

Verificaciones antes del calculo:

| Cultivar | Ventana TRAIN | Observaciones | Meses faltantes | Duplicados | NaN target | Frecuencia |
|---|---|---:|---:|---:|---:|---|
| Sutil | 2016-01..2023-12 | 96 | 0 | 0 | 0 | mensual ordenada |
| Dulce | 2016-01..2023-12 | 96 | 0 | 0 | 0 | mensual ordenada |

## 3. Ventana temporal

Ventana para estimar P75:

```text
TRAIN = 2016-01 .. 2023-12
```

Los cambios relativos efectivos empiezan en 2016-02 porque 2016-01 no tiene mes anterior dentro de TRAIN.

| Cultivar | Periodo de cambios TRAIN | N cambios |
|---|---|---:|
| Sutil | 2016-02..2023-12 | 95 |
| Dulce | 2016-02..2023-12 | 95 |

Para aplicar la mascara congelada a enero 2025, solo como auditoria de clasificacion, se conserva el contexto diciembre 2024 -> enero 2025. Diciembre 2024 no participa en la estimacion del umbral.

## 4. Formula de cambio relativo

Para cada mes efectivo:

```text
r_t = abs((y_t - y_(t-1)) / y_(t-1))
```

Detalles verificados:

| Punto | Estado |
|---|---|
| Escala | Toneladas originales |
| Unidad interna | Proporcion, no porcentaje |
| Valor absoluto | Si |
| Denominador cero | No se encontraron `y_(t-1) = 0` |
| NaN en calculo | No se encontraron |
| Primer mes TRAIN | 2016-01 no genera `r_t` |

Ejemplo de interpretacion: `0.232 = 23.2%`.

## 5. Metodo exacto de percentil

Se uso una implementacion propia en PowerShell documentada en:

```text
v2_reentrenamiento/auditorias/reconstruir_p75_shocks_train.ps1
```

Metodo:

```text
P75 = quantile(r_train, 0.75)
```

Interpolacion:

```text
Hyndman-Fan tipo 7
posicion cero-basada h = (N - 1) * p
lower = floor(h)
upper = ceil(h)
quantile = x_sorted[lower] + (h - lower) * (x_sorted[upper] - x_sorted[lower])
```

Este metodo es equivalente al cuantile lineal por defecto de pandas/numpy para una muestra ordenada. Con `N = 95` y `p = 0.75`, la posicion es:

```text
h = (95 - 1) * 0.75 = 70.5
```

Por tanto, el P75 es la interpolacion lineal entre las posiciones ordenadas cero-basadas 70 y 71.

El umbral computacional se conserva como proporcion con precision completa de doble precision. Los porcentajes redondeados son solo presentacion.

## 6. P75 Sutil

| Item | Valor |
|---|---:|
| N TRAIN original | 96 |
| N cambios relativos | 95 |
| P75 exacto, proporcion | 0.240834900212216 |
| P75 exacto, porcentaje | 24.0834900212216% |
| P75 presentacion 1 decimal | 24.1% |

## 7. P75 Dulce

| Item | Valor |
|---|---:|
| N TRAIN original | 96 |
| N cambios relativos | 95 |
| P75 exacto, proporcion | 0.320281354618397 |
| P75 exacto, porcentaje | 32.0281354618397% |
| P75 presentacion 1 decimal | 32.0% |

## 8. Comparacion con umbrales heredados

Los umbrales historicos se mantienen identificados como **umbrales heredados/no reproducibles**.

| Cultivar | P75 historico heredado | P75 TRAIN-only exacto | P75 TRAIN-only % | Diferencia pp | Diferencia relativa | Coincide exacto | Coincide redondeado a 1 decimal |
|---|---:|---:|---:|---:|---:|---|---|
| Sutil | 23.2% | 0.240834900212216 | 24.0834900212216% | +0.883490021221583 | +3.80814664319648% | No | No |
| Dulce | 33.9% | 0.320281354618397 | 32.0281354618397% | -1.87186453816033 | -5.52172430135791% | No | No |

Lectura:

- Sutil: el P75 TRAIN-only es mayor que el umbral heredado por 0.88349 puntos porcentuales.
- Dulce: el P75 TRAIN-only es menor que el umbral heredado por 1.87186 puntos porcentuales.
- En ambos cultivares el valor heredado no coincide exactamente ni despues de redondear el P75 TRAIN-only a una cifra decimal porcentual.
- Los valores difieren materialmente como umbrales, aunque en test 2025 la mascara resultante permanece igual.

## 9. Aplicacion congelada a test 2025

Una vez estimado P75 solo con TRAIN, el umbral se congelo y se aplico a los 12 cambios de test:

```text
shock_t = 1 si r_t > P75_train
shock_t = 0 en otro caso
```

Se uso estrictamente `>`, coherente con la definicion auditada en `AUDITORIA_D35_P75_SHOCKS.md`.

### Sutil

| Fecha | r_t exacto | r_t % | P75 exacto | Distancia r_t - P75 | Shock |
|---|---:|---:|---:|---:|---|
| 2025-01 | 0.443242936283832 | 44.3242936283833% | 0.240834900212216 | 0.202408036071617 | Si |
| 2025-02 | 0.0461142292535909 | 4.61142292535909% | 0.240834900212216 | -0.194720670958625 | No |
| 2025-03 | 0.0505543179292003 | 5.05543179292003% | 0.240834900212216 | -0.190280582283016 | No |
| 2025-04 | 0.0223393095646545 | 2.23393095646545% | 0.240834900212216 | -0.218495590647561 | No |
| 2025-05 | 0.0880680693651769 | 8.80680693651769% | 0.240834900212216 | -0.152766830847039 | No |
| 2025-06 | 0.0812601813061733 | 8.12601813061733% | 0.240834900212216 | -0.159574718906043 | No |
| 2025-07 | 0.321234735197535 | 32.1234735197535% | 0.240834900212216 | 0.0803998349853191 | Si |
| 2025-08 | 0.230624683388927 | 23.0624683388927% | 0.240834900212216 | -0.010210216823289 | No |
| 2025-09 | 0.0159655994904863 | 1.59655994904863% | 0.240834900212216 | -0.22486930072173 | No |
| 2025-10 | 0.159818255618872 | 15.9818255618872% | 0.240834900212216 | -0.0810166445933437 | No |
| 2025-11 | 0.571224085829124 | 57.1224085829124% | 0.240834900212216 | 0.330389185616908 | Si |
| 2025-12 | 0.0432885885015256 | 4.32885885015256% | 0.240834900212216 | -0.19754631171069 | No |

### Dulce

| Fecha | r_t exacto | r_t % | P75 exacto | Distancia r_t - P75 | Shock |
|---|---:|---:|---:|---:|---|
| 2025-01 | 0.386989534167864 | 38.6989534167864% | 0.320281354618397 | 0.066708179549467 | Si |
| 2025-02 | 0.404361721015565 | 40.4361721015565% | 0.320281354618397 | 0.0840803663971681 | Si |
| 2025-03 | 0.387344866094945 | 38.7344866094945% | 0.320281354618397 | 0.0670635114765484 | Si |
| 2025-04 | 0.109367880684062 | 10.9367880684062% | 0.320281354618397 | -0.210913473934335 | No |
| 2025-05 | 0.0627438632038661 | 6.27438632038661% | 0.320281354618397 | -0.257537491414531 | No |
| 2025-06 | 0.0429886064855391 | 4.29886064855391% | 0.320281354618397 | -0.277292748132858 | No |
| 2025-07 | 0.244425111039883 | 24.4425111039883% | 0.320281354618397 | -0.0758562435785139 | No |
| 2025-08 | 0.161020544209442 | 16.1020544209442% | 0.320281354618397 | -0.159260810408955 | No |
| 2025-09 | 0.0989357603775401 | 9.89357603775401% | 0.320281354618397 | -0.221345594240857 | No |
| 2025-10 | 0.191085695962376 | 19.1085695962376% | 0.320281354618397 | -0.129195658656021 | No |
| 2025-11 | 0.244681553911205 | 24.4681553911205% | 0.320281354618397 | -0.0755998007071916 | No |
| 2025-12 | 0.121801880603543 | 12.1801880603543% | 0.320281354618397 | -0.198479474014854 | No |

## 10. Comparacion de shock masks

| Cultivar | Shocks historicos | Shocks TRAIN-only | Permanecen | Salen | Nuevos | n historico | n TRAIN-only |
|---|---|---|---|---|---|---:|---:|
| Sutil | 2025-01, 2025-07, 2025-11 | 2025-01, 2025-07, 2025-11 | 2025-01, 2025-07, 2025-11 | ninguno | ninguno | 3 | 3 |
| Dulce | 2025-01, 2025-02, 2025-03 | 2025-01, 2025-02, 2025-03 | 2025-01, 2025-02, 2025-03 | ninguno | ninguno | 3 | 3 |

Resultado: la reconstruccion TRAIN-only cambia los valores de umbral respecto a los heredados/no reproducibles, pero no cambia la clasificacion de meses shock en test 2025.

## 11. Sensibilidad al redondeo

El shock_mask oficial debe usar el P75 exacto:

| Cultivar | P75 exacto | P75 % exacto | P75 % redondeado 1 decimal | Mes mas cercano al umbral | Distancia exacta | Cambios si se usa 1 decimal % |
|---|---:|---:|---:|---|---:|---:|
| Sutil | 0.240834900212216 | 24.0834900212216% | 24.1% | 2025-08 | -0.010210216823289 | 0 |
| Dulce | 0.320281354618397 | 32.0281354618397% | 32.0% | 2025-01 | 0.066708179549467 | 0 |

No hay meses de test 2025 cuya clasificacion cambie al usar incorrectamente el P75 redondeado a una cifra decimal porcentual. Aun asi, el umbral computacional oficial debe ser el valor exacto en proporcion, no el porcentaje redondeado de presentacion.

## 12. Reproducibilidad

Script creado:

```text
v2_reentrenamiento/auditorias/reconstruir_p75_shocks_train.ps1
```

Ejecucion usada en esta sesion:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\v2_reentrenamiento\auditorias\reconstruir_p75_shocks_train.ps1
```

La opcion `-ExecutionPolicy Bypass` se uso solo para la invocacion porque la politica local de Windows PowerShell bloqueaba la ejecucion directa de scripts. No cambia la politica del sistema ni modifica datos.

El script reproduce:

- lectura de los masters crudos v2;
- validacion de TRAIN 2016-01..2023-12;
- calculo de 95 cambios relativos TRAIN por cultivar;
- P75 TRAIN-only con interpolacion lineal tipo 7;
- aplicacion congelada a test 2025;
- comparacion contra shocks historicos;
- sensibilidad al redondeo.

## 13. Implicacion para D35-b

La ambiguedad de origen de los valores heredados/no reproducibles 23.2% y 33.9% queda resuelta operacionalmente mediante una reconstruccion reproducible TRAIN-only:

| Cultivar | Umbral computacional TRAIN-only |
|---|---:|
| Sutil | 0.240834900212216 |
| Dulce | 0.320281354618397 |

Estos valores son la fuente reproducible calculada exclusivamente con TRAIN 2016-2023. Sin embargo, D35-b no se declara cerrado en este informe. La aprobacion metodologica final queda pendiente de decision externa.
