# MATRIZ DE AFIRMACIONES SHAP V2

| AFIRMACION | SOPORTE | NIVEL DE EVIDENCIA | REDACCION PERMITIDA | REDACCION NO PERMITIDA |
| --- | --- | --- | --- | --- |
| Clima en Sutil | NASA/clima es grupo de alta participacion en GC3/GE y aparece con diferencia shock-nonshock positiva. | Descriptiva SHAP post-hoc | En los modelos SHAP, las variables NASA/clima concentran una fraccion relevante de |SHAP| para Sutil. | El clima causo los shocks o explica causalmente la produccion. |
| Produccion historica en Dulce | Produccion historica tiene mayor participacion relativa en Dulce que en Sutil dentro de redes. | Descriptiva comparativa | La produccion historica aporta una fraccion descriptiva mayor en Dulce que en Sutil en estas atribuciones. | La produccion pasada determina causalmente el desempeno futuro. |
| NLP Sutil | GE Sutil: NLP participa 0.103566 del |SHAP| total. | Descriptiva SHAP | NLP tiene una participacion no nula y acotada en GE Sutil. | Las noticias causaron cambios productivos en Sutil. |
| NLP Dulce | GE Dulce: NLP participa 0.086341 del |SHAP| total. | Descriptiva SHAP | NLP aporta menos del 10% del |SHAP| total en GE Dulce. | NLP explica causalmente la mejora de GE. |
| Shocks | n_shock=3 y n_nonshock=9; diferencias por grupos son descriptivas. | Baja para inferencia; util descriptiva | Durante meses shock, algunos grupos muestran mayor o menor |SHAP| medio. | Los shocks fueron causados por esas variables o hay significancia estadistica. |
| Resiliencia | Comparacion final muestra errores shock, pero SHAP no prueba resiliencia. | Contextual, no causal | Se puede hablar de desempeno observado en meses shock. | SHAP demuestra resiliencia o beneficio economico. |
| Effective lag | Mapeo separa timestep, lag interno y effective lag. | Metodologica descriptiva | La antiguedad efectiva combina posicion secuencial e indicador lag interno. | El timestep por si solo equivale a antiguedad real de la variable. |
| Comparacion con SARIMA | Comparacion final congelada: SARIMA mejor global; casos shock segun cultivar. | Predictiva congelada | SHAP contextualiza modelos neuronales/XGB; SARIMA se compara por metricas congeladas. | SHAP explica SARIMA o prueba superioridad causal. |
| Causalidad | D59 y auditorias prohiben causalidad. | Restriccion metodologica | Contribucion positiva/negativa del modelo respecto a baseline. | La variable causo aumento/disminucion real de produccion. |
