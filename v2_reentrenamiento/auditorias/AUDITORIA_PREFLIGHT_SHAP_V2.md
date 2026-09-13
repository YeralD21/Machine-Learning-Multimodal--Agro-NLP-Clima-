# AUDITORIA PREFLIGHT SHAP V2

Estado: COMPLETADA COMO PRUEBA TECNICA TRAIN/VAL. No genera resultados SHAP oficiales.

## Restricciones Verificadas

- TEST cargado: False.
- Seed tecnico usado: 0; no es seleccion por desempeno ni seed oficial de interpretacion.
- Background usado en prueba: primeros 8 ejemplos TRAIN.
- Muestras explicadas en prueba: primeros 2 ejemplos VAL.
- No entrenamiento, no reentrenamiento, no HPO, no CV, no modificacion de checkpoints.

## GC3/GE

| cultivar | modelo | seed tecnico | DeepExplainer ok | GradientExplainer ok | KernelExplainer ok | train X_a/X_b | VAL X_a/X_b |
|---|---|---:|---|---|---|---|---|
| sutil | GC3 | 0 | False | False | True | [84, 6, 4] / [84, 6, 33] | [12, 6, 4] / [12, 6, 33] |
| sutil | GE | 0 | False | False | True | [84, 6, 4] / [84, 6, 39] | [12, 6, 4] / [12, 6, 39] |
| dulce | GC3 | 0 | False | False | True | [84, 6, 4] / [84, 6, 33] | [12, 6, 4] / [12, 6, 33] |
| dulce | GE | 0 | False | False | True | [84, 6, 4] / [84, 6, 39] | [12, 6, 4] / [12, 6, 39] |

## XGBoost

| cultivar | seed tecnico | TreeExplainer ok | train shape | VAL shape | SHAP shape | max additivity abs error |
|---|---:|---|---|---|---|---:|
| sutil | 0 | True | [89, 37] | [12, 37] | [2, 37] | 4.311751635732719e-07 |
| dulce | 0 | True | [89, 37] | [12, 37] | [2, 37] | 3.253059606134201e-07 |

## Archivo JSON

- `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\auditorias\shap_v2_preflight.json`
