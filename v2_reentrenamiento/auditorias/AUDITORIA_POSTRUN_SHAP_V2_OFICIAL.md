# AUDITORIA POST-RUN SHAP V2 OFICIAL

Fecha UTC: 2026-09-13T19:56:33.781030+00:00

Estado: PASS

## Resumen de ejecucion

- Explicaciones totales: 720
- Explicaciones neuronales GC3/GE: 480
- Explicaciones XGBoost: 240
- Maximo error de reconstruccion SHAP: 1.11217088117e-06
- Error maximo de prediccion contra congeladas: 2.88948059035e-07
- Manifiesto SHA256: C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\official_test_2025\metadata\MANIFEST_SHA256.json

## Matriz PASS/FAIL

| Control | Estado |
| --- | --- |
| 2 cultivares | PASS |
| GC3/GE/XGBoost | PASS |
| 10 seeds cada uno | PASS |
| 12 meses cada seed | PASS |
| 480 explicaciones neuronales | PASS |
| 240 explicaciones XGBoost | PASS |
| 720 explicaciones totales | PASS |
| ningun modelo faltante | PASS |
| ningun mes faltante | PASS |
| ningun duplicado | PASS |
| background correcto | PASS |
| explainers correctos | PASS |
| parametros correctos | PASS |
| hashes congelados intactos | PASS |
| predicciones dentro de atol=1e-6 | PASS |
| shapes correctos | PASS |
| reconstruccion SHAP | PASS |
| archivos RAW presentes | PASS |
| agregaciones reproducibles | PASS |

## Top 10 features por mean |SHAP|

### sutil/GC3

| Rank | Feature | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 1 | personas_afectadas_lag3 | 0.0863824 | 0.044312 |
| 2 | num_emergencias_lag1 | 0.074047 | 0.037798 |
| 3 | produccion_t_sutil_lag6 | 0.0701527 | 0.0335301 |
| 4 | WS2M_lag3 | 0.0680073 | 0.0328666 |
| 5 | PRECTOTCORR_lag6 | 0.0650907 | 0.0321322 |
| 6 | t_index | 0.0604394 | 0.0284751 |
| 7 | personas_afectadas_lag1 | 0.0600744 | 0.0337928 |
| 8 | personas_damnificadas_lag3 | 0.0593449 | 0.0321998 |
| 9 | PRECTOTCORR_lag3 | 0.0580996 | 0.0311432 |
| 10 | produccion_t_sutil_lag3 | 0.0574853 | 0.0279777 |

### sutil/GE

| Rank | Feature | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 1 | personas_afectadas_lag3 | 0.0579509 | 0.053548 |
| 2 | WS2M_lag1 | 0.0531954 | 0.0437468 |
| 3 | produccion_t_sutil_lag6 | 0.0528612 | 0.0362555 |
| 4 | T2M_lag1 | 0.0523823 | 0.0359235 |
| 5 | T2M_MAX_lag3 | 0.0523282 | 0.0351184 |
| 6 | T2M_lag6 | 0.0522727 | 0.0426033 |
| 7 | num_emergencias_lag1 | 0.0518934 | 0.0486315 |
| 8 | PRECTOTCORR_lag3 | 0.0516228 | 0.0427913 |
| 9 | PRECTOTCORR_lag6 | 0.0508115 | 0.0437678 |
| 10 | WS2M_lag3 | 0.0506515 | 0.042772 |

### dulce/GC3

| Rank | Feature | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 1 | produccion_t_dulce_lag1 | 0.0915157 | 0.0207717 |
| 2 | produccion_t_dulce_lag6 | 0.090447 | 0.0215895 |
| 3 | produccion_t_dulce_lag3 | 0.0873097 | 0.0162051 |
| 4 | t_index | 0.0810381 | 0.0113195 |
| 5 | mes_cos | 0.077598 | 0.0156821 |
| 6 | T2M_lag1 | 0.0678281 | 0.0161192 |
| 7 | T2M_MAX_lag1 | 0.0678277 | 0.0074167 |
| 8 | T2M_lag6 | 0.0659919 | 0.0111517 |
| 9 | WS2M_lag3 | 0.0630611 | 0.00660503 |
| 10 | PRECTOTCORR_lag6 | 0.0624263 | 0.00675488 |

### dulce/GE

| Rank | Feature | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 1 | produccion_t_dulce_lag1 | 0.0927014 | 0.0160368 |
| 2 | produccion_t_dulce_lag3 | 0.0902442 | 0.0196776 |
| 3 | produccion_t_dulce_lag6 | 0.0831911 | 0.0153615 |
| 4 | t_index | 0.0820862 | 0.0138619 |
| 5 | mes_cos | 0.0790736 | 0.0150184 |
| 6 | T2M_lag6 | 0.0770317 | 0.0156944 |
| 7 | T2M_MAX_lag1 | 0.0691402 | 0.00837398 |
| 8 | WS2M_lag1 | 0.0684169 | 0.0122848 |
| 9 | T2M_lag1 | 0.0634646 | 0.0115957 |
| 10 | WS2M_lag3 | 0.0590627 | 0.00795594 |

### sutil/XGBoost

| Rank | Feature | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 1 | produccion_t_sutil | 0.665814 | 0.0281658 |
| 2 | WS2M_lag3 | 0.143613 | 0.0206562 |
| 3 | T2M_lag6 | 0.100827 | 0.0133965 |
| 4 | RH2M_lag3 | 0.0688375 | 0.00844903 |
| 5 | WS2M_lag1 | 0.0568089 | 0.0118605 |
| 6 | mes_cos | 0.0504781 | 0.00851512 |
| 7 | num_emergencias_lag3 | 0.0467838 | 0.0107549 |
| 8 | personas_afectadas_lag3 | 0.0370307 | 0.00519808 |
| 9 | T2M_MAX_lag1 | 0.0309521 | 0.00679283 |
| 10 | RH2M_lag6 | 0.027252 | 0.00363654 |

### dulce/XGBoost

| Rank | Feature | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 1 | mes_sin | 0.42159 | 0.0411808 |
| 2 | produccion_t_dulce | 0.210128 | 0.0146269 |
| 3 | produccion_t_dulce_lag6 | 0.155307 | 0.0177069 |
| 4 | RH2M_lag1 | 0.121253 | 0.0300928 |
| 5 | produccion_t_dulce_lag3 | 0.0493598 | 0.0155219 |
| 6 | T2M_MAX_lag6 | 0.0436299 | 0.00727218 |
| 7 | hectareas_cultivo_perdidas_lag3 | 0.0325181 | 0.00418698 |
| 8 | produccion_t_dulce_lag1 | 0.0294382 | 0.0104204 |
| 9 | RH2M_lag6 | 0.0256223 | 0.00449639 |
| 10 | PRECTOTCORR_lag6 | 0.0218366 | 0.00359227 |

## Importancia por grupos

### sutil/GC3

| Grupo | mean |SHAP| | Participacion | SD seeds |
| --- | --- | --- | --- |
| INDECI | 0.73722 | 0.399092 | 0.340797 |
| NASA_clima | 0.729442 | 0.394881 | 0.288841 |
| produccion_historica | 0.227383 | 0.123093 | 0.0964417 |
| temporalidad | 0.153199 | 0.0829337 | 0.0701134 |

### sutil/GE

| Grupo | mean |SHAP| | Participacion | SD seeds |
| --- | --- | --- | --- |
| INDECI | 0.456471 | 0.294255 | 0.349074 |
| NASA_clima | 0.654203 | 0.421719 | 0.472552 |
| NLP | 0.16066 | 0.103566 | 0.109369 |
| produccion_historica | 0.162317 | 0.104635 | 0.106377 |
| temporalidad | 0.117626 | 0.0758253 | 0.0829774 |

### dulce/GC3

| Grupo | mean |SHAP| | Participacion | SD seeds |
| --- | --- | --- | --- |
| INDECI | 0.616821 | 0.315084 | 0.105884 |
| NASA_clima | 0.807688 | 0.412583 | 0.0851132 |
| produccion_historica | 0.324833 | 0.165931 | 0.0389453 |
| temporalidad | 0.208298 | 0.106403 | 0.0211979 |

### dulce/GE

| Grupo | mean |SHAP| | Participacion | SD seeds |
| --- | --- | --- | --- |
| INDECI | 0.563482 | 0.272423 | 0.102796 |
| NASA_clima | 0.800883 | 0.387198 | 0.0910017 |
| NLP | 0.178589 | 0.0863413 | 0.0123129 |
| produccion_historica | 0.319141 | 0.154293 | 0.0300497 |
| temporalidad | 0.206313 | 0.0997447 | 0.0221475 |

## NLP en GE

### sutil

- Participacion NLP sobre |SHAP| total GE: 0.103566

| Rank | Variable NLP | mean |SHAP| | Participacion dentro NLP |
| --- | --- | --- | --- |
| 1 | n_noticias_lag6 | 0.0312744 | 0.194663 |
| 2 | n_noticias_lag1 | 0.0293235 | 0.18252 |
| 3 | avg_sentiment_lag6 | 0.0270834 | 0.168576 |
| 4 | avg_sentiment_lag3 | 0.0264218 | 0.164458 |
| 5 | avg_sentiment_lag1 | 0.0234099 | 0.145711 |
| 6 | n_noticias_lag3 | 0.0231466 | 0.144072 |

### dulce

- Participacion NLP sobre |SHAP| total GE: 0.0863413

| Rank | Variable NLP | mean |SHAP| | Participacion dentro NLP |
| --- | --- | --- | --- |
| 1 | n_noticias_lag1 | 0.0329551 | 0.18453 |
| 2 | avg_sentiment_lag1 | 0.0321487 | 0.180015 |
| 3 | n_noticias_lag3 | 0.0302166 | 0.169197 |
| 4 | avg_sentiment_lag3 | 0.028887 | 0.161751 |
| 5 | avg_sentiment_lag6 | 0.0273436 | 0.153109 |
| 6 | n_noticias_lag6 | 0.027038 | 0.151398 |

## Shock vs nonshock por grupos

### sutil/GC3

| Grupo | Shock mean |SHAP| | Nonshock mean |SHAP| | Diferencia |
| --- | --- | --- | --- |
| INDECI | 0.734934 | 0.737982 | -0.003048 |
| NASA_clima | 0.769007 | 0.716253 | 0.0527544 |
| produccion_historica | 0.211846 | 0.232563 | -0.0207173 |
| temporalidad | 0.146805 | 0.15533 | -0.00852495 |

### sutil/GE

| Grupo | Shock mean |SHAP| | Nonshock mean |SHAP| | Diferencia |
| --- | --- | --- | --- |
| INDECI | 0.455982 | 0.456634 | -0.000651982 |
| NASA_clima | 0.682409 | 0.6448 | 0.0376089 |
| NLP | 0.137274 | 0.168455 | -0.0311811 |
| produccion_historica | 0.136759 | 0.170836 | -0.0340769 |
| temporalidad | 0.133515 | 0.11233 | 0.0211854 |

### dulce/GC3

| Grupo | Shock mean |SHAP| | Nonshock mean |SHAP| | Diferencia |
| --- | --- | --- | --- |
| INDECI | 0.668875 | 0.599469 | 0.0694054 |
| NASA_clima | 0.9057 | 0.775017 | 0.130682 |
| produccion_historica | 0.37329 | 0.30868 | 0.0646098 |
| temporalidad | 0.182117 | 0.217025 | -0.0349079 |

### dulce/GE

| Grupo | Shock mean |SHAP| | Nonshock mean |SHAP| | Diferencia |
| --- | --- | --- | --- |
| INDECI | 0.613476 | 0.546818 | 0.0666579 |
| NASA_clima | 0.887457 | 0.772026 | 0.115432 |
| NLP | 0.166149 | 0.182736 | -0.0165866 |
| produccion_historica | 0.371127 | 0.301812 | 0.0693149 |
| temporalidad | 0.172096 | 0.217719 | -0.0456227 |

## Importancia por timestep

La posicion 5 es el origen de prediccion t; la posicion 0 corresponde a t-5.

### sutil/GC3

| Timestep | Offset | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 0 | -5 | 0.531448 | 0.211172 |
| 1 | -4 | 0.366754 | 0.166709 |
| 2 | -3 | 0.332149 | 0.152421 |
| 3 | -2 | 0.255284 | 0.111162 |
| 4 | -1 | 0.2159 | 0.0965164 |
| 5 | 0 | 0.145708 | 0.0690108 |

### sutil/GE

| Timestep | Offset | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 0 | -5 | 0.431664 | 0.314226 |
| 1 | -4 | 0.298966 | 0.223316 |
| 2 | -3 | 0.259138 | 0.193366 |
| 3 | -2 | 0.242397 | 0.173711 |
| 4 | -1 | 0.19106 | 0.137486 |
| 5 | 0 | 0.128051 | 0.0913387 |

### dulce/GC3

| Timestep | Offset | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 0 | -5 | 0.875517 | 0.112009 |
| 1 | -4 | 0.311397 | 0.0515653 |
| 2 | -3 | 0.273281 | 0.0444632 |
| 3 | -2 | 0.221566 | 0.0364968 |
| 4 | -1 | 0.170256 | 0.0285199 |
| 5 | 0 | 0.105623 | 0.0177761 |

### dulce/GE

| Timestep | Offset | mean |SHAP| | SD seeds |
| --- | --- | --- | --- |
| 0 | -5 | 0.954313 | 0.083794 |
| 1 | -4 | 0.322125 | 0.0561449 |
| 2 | -3 | 0.264892 | 0.0467283 |
| 3 | -2 | 0.230302 | 0.042193 |
| 4 | -1 | 0.178597 | 0.0317591 |
| 5 | 0 | 0.11818 | 0.0216736 |

## Variabilidad entre seeds

| Cultivar/modelo | Mean | Median | SD | Min | Max |
| --- | --- | --- | --- | --- | --- |
| sutil/GC3 | 1.84724 | 2.14185 | 0.771221 | 0.497316 | 2.66975 |
| sutil/GE | 1.55128 | 1.48068 | 1.10825 | 0.278762 | 3.59501 |
| sutil/XGBoost | 1.54966 | 1.55263 | 0.0248944 | 1.5037 | 1.58514 |
| dulce/GC3 | 1.95764 | 1.95024 | 0.168493 | 1.66805 | 2.20309 |
| dulce/GE | 2.06841 | 2.04968 | 0.182927 | 1.77243 | 2.31323 |
| dulce/XGBoost | 1.33224 | 1.33405 | 0.0127151 | 1.30934 | 1.35416 |

## Incidencias tecnicas

- La primera invocacion sandbox fallo antes de entrar al script por ruta de interprete base del venv; se reintento con el prefijo aprobado del venv, sin cambiar protocolo.
- No se detectaron warnings de explainer en metadata.
- Fallback SamplingExplainer usado: False

## Restricciones interpretativas

Estos resultados son descriptivos de contribuciones del modelo respecto a su baseline. No se registran conclusiones causales, inferencia de significancia, beneficios economicos ni interpretaciones de attention.

Estado final: SHAP V2 OFICIAL COMPLETADO Y CONGELADO
