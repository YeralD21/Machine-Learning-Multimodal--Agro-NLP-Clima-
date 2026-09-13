# AUDITORIA POST-RUN GC2/XGBOOST

Generado UTC: `2026-09-12T21:09:26.417478+00:00`

## Resultado

- Estado: `approved`
- Corridas esperadas: 20
- Corridas auditadas: 20
- TEST 2025 abierto: NO.
- HPO/CV/early stopping/best seed: NO.

## Resumen de validacion

| Cultivar | Seed | MAE val | RMSE val | R2 val |
|---|---:|---:|---:|---:|
| dulce | 0 | 46.276641 | 54.103800 | 0.877376 |
| dulce | 1 | 45.888951 | 55.369133 | 0.871574 |
| dulce | 2 | 47.083800 | 56.794107 | 0.864878 |
| dulce | 3 | 49.299618 | 57.309434 | 0.862415 |
| dulce | 4 | 48.696078 | 56.669429 | 0.865471 |
| dulce | 5 | 49.213049 | 59.649285 | 0.850951 |
| dulce | 6 | 48.824041 | 59.622149 | 0.851086 |
| dulce | 7 | 47.087619 | 56.074128 | 0.868282 |
| dulce | 8 | 55.458889 | 65.013123 | 0.822940 |
| dulce | 9 | 46.958423 | 54.334446 | 0.876329 |
| sutil | 0 | 4636.178811 | 6132.044208 | 0.438852 |
| sutil | 1 | 4719.800022 | 6166.035609 | 0.432614 |
| sutil | 2 | 4404.311305 | 5873.584637 | 0.485159 |
| sutil | 3 | 4399.868058 | 5821.870088 | 0.494185 |
| sutil | 4 | 4174.339290 | 5703.639287 | 0.514521 |
| sutil | 5 | 4519.765186 | 5941.522410 | 0.473180 |
| sutil | 6 | 4343.036136 | 5943.767078 | 0.472782 |
| sutil | 7 | 4374.932285 | 5857.100098 | 0.488045 |
| sutil | 8 | 4534.728140 | 6108.939023 | 0.443073 |
| sutil | 9 | 4706.915027 | 6077.665886 | 0.448761 |

## Controles

| Control | Estado | Detalle |
|---|---|---|
| 20 manifest records | OK | 20 |
| expected cultivar/seed pairs | OK | [('dulce', 0), ('dulce', 1), ('dulce', 2), ('dulce', 3), ('dulce', 4), ('dulce', 5), ('dulce', 6), ('dulce', 7), ('dulce', 8), ('dulce', 9), ('sutil', 0), ('sutil', 1), ('sutil', 2), ('sutil', 3), ('sutil', 4), ('sutil', 5), ('sutil', 6), ('sutil', 7), ('sutil', 8), ('sutil', 9)] |
| all subprocesses complete | OK | manifest status/returncode |
| sutil seed_00 required artifacts | OK | ok |
| sutil seed_00 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_00 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_00 no TEST | OK | False |
| sutil seed_01 required artifacts | OK | ok |
| sutil seed_01 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_01 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_01 no TEST | OK | False |
| sutil seed_02 required artifacts | OK | ok |
| sutil seed_02 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_02 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_02 no TEST | OK | False |
| sutil seed_03 required artifacts | OK | ok |
| sutil seed_03 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_03 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_03 no TEST | OK | False |
| sutil seed_04 required artifacts | OK | ok |
| sutil seed_04 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_04 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_04 no TEST | OK | False |
| sutil seed_05 required artifacts | OK | ok |
| sutil seed_05 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_05 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_05 no TEST | OK | False |
| sutil seed_06 required artifacts | OK | ok |
| sutil seed_06 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_06 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_06 no TEST | OK | False |
| sutil seed_07 required artifacts | OK | ok |
| sutil seed_07 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_07 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_07 no TEST | OK | False |
| sutil seed_08 required artifacts | OK | ok |
| sutil seed_08 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_08 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_08 no TEST | OK | False |
| sutil seed_09 required artifacts | OK | ok |
| sutil seed_09 train shape | OK | {'X': [89, 37], 'y': [89]} |
| sutil seed_09 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| sutil seed_09 no TEST | OK | False |
| dulce seed_00 required artifacts | OK | ok |
| dulce seed_00 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_00 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_00 no TEST | OK | False |
| dulce seed_01 required artifacts | OK | ok |
| dulce seed_01 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_01 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_01 no TEST | OK | False |
| dulce seed_02 required artifacts | OK | ok |
| dulce seed_02 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_02 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_02 no TEST | OK | False |
| dulce seed_03 required artifacts | OK | ok |
| dulce seed_03 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_03 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_03 no TEST | OK | False |
| dulce seed_04 required artifacts | OK | ok |
| dulce seed_04 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_04 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_04 no TEST | OK | False |
| dulce seed_05 required artifacts | OK | ok |
| dulce seed_05 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_05 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_05 no TEST | OK | False |
| dulce seed_06 required artifacts | OK | ok |
| dulce seed_06 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_06 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_06 no TEST | OK | False |
| dulce seed_07 required artifacts | OK | ok |
| dulce seed_07 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_07 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_07 no TEST | OK | False |
| dulce seed_08 required artifacts | OK | ok |
| dulce seed_08 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_08 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_08 no TEST | OK | False |
| dulce seed_09 required artifacts | OK | ok |
| dulce seed_09 train shape | OK | {'X': [89, 37], 'y': [89]} |
| dulce seed_09 validation shape | OK | {'X': [12, 37], 'y': [12]} |
| dulce seed_09 no TEST | OK | False |
