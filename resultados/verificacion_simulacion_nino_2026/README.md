# Verificación de la simulación El Niño 2026 — signo de sensibilidad climática invertido

## Hallazgo

Los notebooks `notebooks/fase4/escenario_nino_2026.ipynb` y
`notebooks/fase4/impacto_economico_nino_2026_ejecutado.ipynb` documentan que **GE y GM_v3
reducen su predicción de producción bajo El Niño** (ajustes de −11% y −22%, interpretados
como "anticipan la caída" y por eso generan menor pérdida económica que XGBoost).

**Esa dirección está invertida.** Al ejecutar `escenario_nino_2026.ipynb` de punta a punta con
los modelos entrenados reales (`resultados/ge/ge_dual_lstm_attn.keras`,
`resultados/gm_v3/gm_v3_model.keras`, XGBoost reentrenado con los mismos hiperparámetros
documentados en `CLAUDE.md`) y el dataset maestro correcto de 56 meses, **GE y GM_v3
sobreestiman la producción bajo El Niño — igual que XGBoost, y en mayor magnitud —, no la
subestiman.**

## Comparación: paper (hardcoded) vs. ejecución real

| Modelo | Paper (`impacto_economico_nino_2026_ejecutado.ipynb`) | Ejecución real (este directorio) |
|---|---|---|
| XGBoost | +5% | **+5.72%** |
| GE | **−11%** | **+11.16%** |
| GM_v3 | **−22%** | **+22.38%** |

La magnitud casi coincide exactamente (11↔11.16, 22↔22.38, 5↔5.72) — lo que indica que el
paper sí partió de una ejecución real de este mismo notebook, pero **el signo de GE y GM_v3 se
invirtió manualmente** al pasar el resultado al notebook económico, probablemente asumiendo sin
verificar que "mayor sensibilidad climática" implicaba "anticipa la caída" (ajuste negativo),
cuando el resultado real del modelo va en sentido contrario.

## Por qué el signo es positivo, no negativo

Los tres modelos fueron entrenados solo con datos 2021–2025, un período sin El Niño extremo,
donde la relación aprendida es **más lluvia → más producción** (relación normal). Al perturbar
las variables climáticas hacia un escenario El Niño (WS2M +50%, PRECTOTCORR +200%, T2M_MAX
+2.5°C, num_emergencias ×4, nlp_index −0.6), los modelos extrapolan esa relación aprendida y
predicen **más** producción, no menos. Esto ya estaba documentado en texto en la propia celda de
conclusiones de `escenario_nino_2026.ipynb` ("Los tres modelos predicen más producción bajo El
Niño, no menos"), pero esa observación no se reflejó en los signos usados después en el cálculo
económico.

## Impacto en la conclusión económica

Con el cálculo de pérdida de la celda 25 del notebook (`CAIDA_REAL_ESTIMADA=0.40`,
`COSTO_MERMA_KG=1.80`, `COSTO_STOCKOUT_KG=3.20`, sin capacidad PYME ni precio shock):

```
PERDIDA TOTAL Q1 2026 (dataset correcto, 56 meses):
  XGBoost    S/ 40,627.25
  GE         S/ 42,510.46
  GM_v3      S/ 48,234.49
```

**GM_v3 genera la mayor pérdida proyectada, no la menor.** XGBoost —el modelo que el paper
describe como "insensible al clima y por eso más costoso"— es en realidad el que menos pierde en
esta simulación, porque su sobreestimación es la más chica de los tres. La conclusión publicada
("GM_v3 es el mejor modelo para la PYME por su sensibilidad climática") queda invertida por este
resultado: la mayor sensibilidad climática de GM_v3 amplifica el error en la dirección
equivocada (sobreestima más), no lo corrige.

Este ranking se confirmó estable en dos corridas independientes: una con un dataset parcial de
50 meses (2021-07 a 2025-08) y esta, con el dataset completo reconstruido de 56 meses
(2021-01 a 2025-08) — ver sección siguiente.

## Sobre el dataset usado

`data/processed/master_dataset_fase2_multivariado.csv`, el archivo que ambos notebooks
originales leen, **no existe en el repositorio** (no está en el working tree, ni en ningún commit
del historial local, ni en la rama remota `alex_dev`). Se reconstruyó de forma determinística
siguiendo exactamente el pipeline de `notebooks/fase2/actividad_02_cyclic_time_encoding.ipynb`
sobre `data/processed/master_dataset_fase1.csv` (que sí existe): relleno de lat/lon por
departamento + codificación cíclica de mes/trimestre (seno/coseno). Sin aleatoriedad, sin
supuestos adicionales. El resultado reproduce exactamente `n_total=56` / `n_train=44` que aparece
en el output real guardado de `actividad_15v5_ejecutado.ipynb` (el notebook que entrenó GM_v3),
confirmando que es el dataset correcto.

## Archivos en este directorio

| Archivo | Contenido |
|---|---|
| `master_dataset_fase2_multivariado_RECONSTRUIDO.csv` | Dataset maestro reconstruido (56 meses, 2021-01 a 2025-08), generado desde `master_dataset_fase1.csv` siguiendo el pipeline de `actividad_02_cyclic_time_encoding.ipynb`. |
| `run_escenario_nino.py` | Script que ejecuta `escenario_nino_2026.ipynb` completo fuera de Jupyter: carga GE (`.keras` + `BahdanauAttention`) y GM_v3 (arquitectura reconstruida + `load_weights`, por el Lambda layer no serializable documentado en `CLAUDE.md`), reentrena XGBoost con `BEST_PARAMS` fijo, aplica la perturbación climática exacta del notebook original, y corre el cálculo económico de la celda 25. Las únicas desviaciones respecto al notebook original están marcadas con comentarios `# --- PATCH vs notebook original ---` (ruta del dataset y de la raíz del proyecto, y el `STRUCT` de GM_v3 restringido a las 20 columnas exactas verificadas contra `actividad_15v5_ejecutado.ipynb` en vez de tomar todas las columnas numéricas del dataset). |
| `run_output_56m.txt` | Output completo de la ejecución con el dataset de 56 meses — predicciones normal/Niño mes a mes, ajuste % por modelo, y la tabla de pérdida económica de la celda 25. |

## Cómo reproducir

```powershell
& "C:\Users\<usuario>\AppData\Local\Programs\Python\Python311\python.exe" run_escenario_nino.py
```

Requiere Python 3.11 con `tensorflow`, `keras>=3.11` (versiones anteriores de Keras 3.x tienen un
bug de deserialización que rompe la carga de `ge_dual_lstm_attn.keras` — ver
`quantization_config` en el traceback si aparece), `xgboost`, `scikit-learn`, `pandas`, `numpy`,
`joblib`.

## Limitación pendiente

No se verificó el notebook `impacto_economico_nino_2026_ejecutado.ipynb` en sí (los escenarios
Moderado/Severo con `CAPACIDAD_PYME_KG` y precio de shock) porque sus `ajuste_pct` están
hardcodeados como constantes, no derivados de una ejecución en vivo dentro de ese notebook — el
vínculo entre ambos notebooks está documentado en la conversación que originó esta verificación,
no en un artefacto ejecutable adicional.
