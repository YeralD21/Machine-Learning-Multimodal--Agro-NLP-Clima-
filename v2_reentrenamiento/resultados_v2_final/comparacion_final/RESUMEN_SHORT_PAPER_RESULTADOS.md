# Resumen descriptivo de resultados v2

Resumen basado exclusivamente en artefactos TEST congelados. No contiene discusion extensa ni conclusiones finales de tesis.

## sutil

- Mejor MAE global observado: SARIMA_rolling (MAE=3985.016474).
- GC3 vs GE: GC3 MAE=7746.000211; GE MAE=9426.458661.
- Menor MAE shock observado: GC3 (MAE_shock=5594.109032).
- Menor Delta_s observado: GC3 (Delta_s=-28.104799).

| model | MAE_shock | diff vs SARIMA | diff vs XGBoost |
|---|---:|---:|---:|
| Naive | 11660.153333 | 3835.750429 | 3363.256817 |
| SARIMA_rolling | 7824.402904 | 0.000000 | -472.493612 |
| XGBoost | 8296.896516 | 472.493612 | 0.000000 |
| GC3 | 5594.109032 | -2230.293872 | -2702.787484 |
| GE | 7908.487889 | 84.084984 | -388.408628 |

## dulce

- Mejor MAE global observado: SARIMA_rolling (MAE=21.766759).
- GC3 vs GE: GC3 MAE=103.413025; GE MAE=95.584556.
- Menor MAE shock observado: SARIMA_rolling (MAE_shock=11.603702).
- Menor Delta_s observado: SARIMA_rolling (Delta_s=-46.690723).

| model | MAE_shock | diff vs SARIMA | diff vs XGBoost |
|---|---:|---:|---:|
| Naive | 138.256667 | 126.652965 | 48.152166 |
| SARIMA_rolling | 11.603702 | 0.000000 | -78.500799 |
| XGBoost | 90.104501 | 78.500799 | 0.000000 |
| GC3 | 118.944194 | 107.340492 | 28.839693 |
| GE | 99.907589 | 88.303887 | 9.803088 |

Advertencias: cada mascara shock tiene n_shock=3. Las diferencias son descriptivas del error absoluto medio; no son causalidad, prueba estadistica de resiliencia, ahorro economico ni reduccion de perdidas reales.
