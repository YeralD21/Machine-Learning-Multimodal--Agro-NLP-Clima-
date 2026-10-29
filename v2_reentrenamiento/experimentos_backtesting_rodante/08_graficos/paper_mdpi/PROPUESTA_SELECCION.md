# Propuesta de selección de figuras para el artículo MDPI (pendiente de aprobación)

No se copió ninguna figura: la selección final la aprueba el autor después de abrir las figuras.
**Sutil y Dulce van en figuras separadas** (decisión del autor): cada número de figura tiene una versión por cultivo.
La selección se hizo por la pregunta que responde cada figura, no por el resultado que muestra.

## Texto principal (7 figuras × 2 cultivos)

| Orden | Figura | Pregunta | Por qué |
|---|---|---|---|
| 1 | `comparacion_modelos/fig13_mae_normal_vs_shock_bars_{c}` | ¿Qué modelos son mejores en meses normales y en meses de shock? | Resultado principal en toneladas (P75), con IC bootstrap |
| 2 | `sensibilidad_umbral/fig07_rank_heatmap_model_percentile_{c}` | ¿El ranking en shocks depende del umbral? ¿Quién empata con el mejor? | Resume P0–P95, mensual e interanual, con los empates de D-BR7 |
| 3 | `sensibilidad_umbral/fig05_mae_shock_vs_percentile_{c}` | ¿Cómo cambia el error en shocks al endurecer el umbral? | Magnitud en toneladas detrás del ranking |
| 4 | `sensibilidad_umbral/fig04_delta_s_vs_percentile_{c}` | ¿Cómo se comporta Δs? | Necesaria para mostrar que un Δs bajo no implica un MAE_shock bajo (E-BR1) |
| 5 | `sensibilidad_umbral/fig10_n_shock_heatmap_window_percentile_{c}` | ¿Cuántos meses sostienen cada umbral y ventana? | Transparencia: ventanas no informativas y percentiles altos con 1–3 meses |
| 6 | `predicciones/fig20_real_vs_pred_2025_{c}` | ¿Cómo siguen los pronósticos a la serie en el año de test oficial? | **2025, decisión del autor** (test original del estudio v2) |
| 7 | `diagnostico_ajuste/fig02_train_val_gap_{c}` | ¿Hay sobreajuste respecto del Naive? | Diagnóstico D-BR8, con la advertencia de nivel en Sutil |

## Material suplementario

- **fig20 de un año con más shocks** (decisión del autor). Criterio: la ventana con más meses de shock P75 mensual (`03_ventanas/umbrales_y_mascaras.csv`):
  - Sutil: `predicciones/fig20_real_vs_pred_2023_sutil` (4 shocks: 08, 09, 11 y 12).
  - Dulce: `predicciones/fig20_real_vs_pred_2024_dulce` (4 shocks: 01, 02, 03 y 10).
- fig18 (ablación GE − GC3) y fig19 (matriz DM sin ajustar); fig19b (Holm, exploratoria) marcada como tal.
- fig01 y fig03 (entrenamiento LSTM), fig06, fig08, fig09, fig11 y fig12 (sensibilidad), fig14–fig17 (comparación) y fig21 (errores mes a mes).
- Las fig20 de los demás años quedan en `08_graficos/predicciones/` y no se proponen.

## Pendiente de decisión del autor

- La aprobación final, después de abrir las figuras (copia para revisión en el Escritorio, fuera del repo).
- Si fig18 o fig19 deben pasar al texto principal.
