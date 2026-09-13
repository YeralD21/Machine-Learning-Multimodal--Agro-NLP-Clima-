# AUDITORIA PREFLIGHT SARIMA ROLLING

Dry-run tecnico sobre VAL 2024 solamente. No se cargaron filas TEST 2025, no se generaron metricas oficiales nuevas y no se ejecuto busqueda de hiperparametros.

## Protocolo probado

- Fit unico en TRAIN 2016-07..2023-12.
- order=(1,1,1), seasonal_order=(1,1,0,12).
- trend='c', initialization='approximate_diffuse', enforce_stationarity=False, enforce_invertibility=False.
- Para cada mes de VAL: `forecast(steps=1)` y luego `append([y_real], refit=False)`.
- Verificacion: parametros identicos antes/despues de cada append.

## Resultado

- Estado: `approved`
- Controles: 18
- Statsmodels: 0.14.6

## Controles

| Control | Estado | Detalle |
|---|---|---|
| sutil TRAIN n=90 | OK | 90 |
| sutil TRAIN 2016-07..2023-12 | OK | 2016-07..2023-12 |
| sutil VAL n=12 | OK | 12 |
| sutil VAL 2024-01..2024-12 | OK | 2024-01..2024-12 |
| sutil no TEST loaded | OK | False |
| sutil forecast steps=1 all VAL | OK | 12 one-step forecasts |
| sutil append refit=False all VAL | OK | ['append(refit=False)'] |
| sutil coefficients unchanged | OK | [True, True, True, True, True, True, True, True, True, True, True, True] |
| sutil VAL dates exact | OK | ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12'] |
| dulce TRAIN n=90 | OK | 90 |
| dulce TRAIN 2016-07..2023-12 | OK | 2016-07..2023-12 |
| dulce VAL n=12 | OK | 12 |
| dulce VAL 2024-01..2024-12 | OK | 2024-01..2024-12 |
| dulce no TEST loaded | OK | False |
| dulce forecast steps=1 all VAL | OK | 12 one-step forecasts |
| dulce append refit=False all VAL | OK | ['append(refit=False)'] |
| dulce coefficients unchanged | OK | [True, True, True, True, True, True, True, True, True, True, True, True] |
| dulce VAL dates exact | OK | ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12'] |

## Confirmaciones

- TEST 2025 forecast ejecutado: NO.
- Metricas oficiales nuevas: NO.
- Refit durante VAL: NO.
- Coeficientes congelados: SI.
