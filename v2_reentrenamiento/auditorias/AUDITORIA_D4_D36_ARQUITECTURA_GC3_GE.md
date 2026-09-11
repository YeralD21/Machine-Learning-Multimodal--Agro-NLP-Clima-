# AUDITORIA D4/D36 - ARQUITECTURA GC3/GE

## 1. Alcance

Auditoria documental y matematica para preparar el cierre externo de D4 y D36
en v2. No se entrenaron modelos, no se ejecuto HPO, no se uso validation 2024
ni test 2025 para seleccionar arquitectura, no se modificaron datasets,
scalers, notebooks existentes ni `DECISIONES_METODOLOGICAS.md`.

Objetivo: reconstruir la evidencia tecnica disponible para GC3 y GE, auditar
los inputs reales v2, calcular conteos exactos de parametros para arquitecturas
alternativas y dejar preguntas abiertas para decision externa.

Taxonomia auditada:

| Etiqueta | Modelo | Rol |
|---|---|---|
| Baseline | Naive t-1 | Piso de referencia |
| GC1 | SARIMA | Control univariado |
| GC2 | XGBoost | Control tabular |
| GC3 | DualLSTM + Bahdanau Attention sin NLP | Control estructural |
| GE | DualLSTM + Bahdanau Attention con NLP rezagado | Grupo experimental |
| Prophet | Benchmark secundario/historico | Secundario |

SARIMAX-LSTM no forma parte del experimento principal v2 auditado aqui.

## 2. Decisiones vigentes relacionadas

Fuentes leidas:

- `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`
- `CONTEXTO_SESION_2026-09-07.md`
- `CONTEXTO_SESION_2026-09-08_METRICAS.md`
- `v2_reentrenamiento/auditorias/AUDITORIA_D35_ESTACIONALIDAD_TRAIN.md`
- `v2_reentrenamiento/auditorias/AUDITORIA_D35_P75_SHOCKS.md`
- `v2_reentrenamiento/auditorias/RECONSTRUCCION_D35_P75_TRAIN_ONLY.md`

Estado observado:

| Tema | Estado | Evidencia / lectura |
|---|---|---|
| D35-a metricas globales | CERRADO | `DECISIONES_METODOLOGICAS.md`, seccion D35 |
| D35-b shocks | CERRADO | P75 oficiales TRAIN-only ya formalizados |
| Split 2016-2023 / 2024 / 2025 | CERRADO | Documentacion metodologica y handoffs |
| D6 rolling one-step-ahead | CERRADO | No exogenas del mes objetivo; solo rezagos |
| Lookback = 6 | CERRADO como regla de Fase 2/secuencias | 84 train / 12 val / 12 test |
| `n_provincias` eliminado | CERRADO | D20-a |
| `precio_chacra_kg` eliminado | CERRADO | D20-b/c |
| `total_afectados` excluido como predictor | Aprobado / pendiente de documentacion formal | D20-d en handoff 2026-09-08; no requiere regenerar Fase 2 |
| D4 hiperparametros LSTM/callbacks | PENDIENTE | Abierta y bloqueante antes de entrenar |
| D7 shuffle | PENDIENTE | Abierta; debe ser identico en GC3 y GE |
| D10 numero de seeds | PENDIENTE | Reservada para decision externa |
| D36 definicion limpia GC3 -> GE | PENDIENTE | Principio propuesto; no cerrar aqui |
| v1 GE / GM | HISTORICO | Fuente de arquitectura y defectos de comparabilidad, no decision v2 |
| C1/C2/C3 del handoff 2026-09-07 | HISTORICO / SUPERSEDED | El handoff 2026-09-08 lo marca obsoleto |
| P75 23.2% / 33.9% | HISTORICO / SUPERSEDED | No son oficiales v2 tras D35-b |

## 3. Inputs reales v2

Fuente inspeccionada:

```text
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_features.csv
v2_reentrenamiento/data/processed/master_dataset_dulce_v2_features.csv
```

Ambos archivos tienen 48 columnas:

- `año`, `mes`
- target contemporaneo de la fila: `produccion_t_{cultivar}`
- calendario: `mes_sin`, `mes_cos`
- tendencia: `t_index`
- 3 lags del target
- 15 NASA
- 18 INDECI, de los cuales `total_afectados_lag{1,3,6}` se excluye por D20-d
- 6 NLP

Tras excluir `año`, `mes` y `total_afectados_lag{1,3,6}`, los inputs reales son:

| Bloque | Conteo | Columnas |
|---|---:|---|
| AR | 4 | `produccion_t_{c}`, `produccion_t_{c}_lag1`, `produccion_t_{c}_lag3`, `produccion_t_{c}_lag6` |
| CAL | 2 | `mes_sin`, `mes_cos` |
| TEND | 1 | `t_index` |
| NASA | 15 | 5 variables x lag 1/3/6 |
| INDECI | 15 | 5 variables x lag 1/3/6; excluye `total_afectados` |
| NLP | 6 | `avg_sentiment` y `n_noticias` x lag 1/3/6 |

Conteo resultante:

| Modelo | AR | CAL | TEND | NASA | INDECI | NLP | Total |
|---|---:|---:|---:|---:|---:|---:|---:|
| GC3 | 4 | 2 | 1 | 15 | 15 | 0 | 37 |
| GE | 4 | 2 | 1 | 15 | 15 | 6 | 43 |

Discrepancia historica: los handoffs previos mencionan 36 no-NLP y 42 con NLP.
Ese conteo excluye `produccion_t_{c}` contemporaneo de la fila como predictor y
retiene solo los 3 lags explicitos del target. Para una secuencia que termina en
mes `t` y predice `y_(t+1)`, la columna `produccion_t_{c}` de las filas de
contexto es informacion autoregresiva pasada permitida. Por eso el conteo real
coherente con Rama A = AR(4) es 37/43, no 36/42.

## 4. Rama A / Rama B

Rama A propuesta para GC3 y GE: informacion autoregresiva.

Para Sutil:

```text
produccion_t_sutil
produccion_t_sutil_lag1
produccion_t_sutil_lag3
produccion_t_sutil_lag6
```

Para Dulce:

```text
produccion_t_dulce
produccion_t_dulce_lag1
produccion_t_dulce_lag3
produccion_t_dulce_lag6
```

Rama B GC3, 33 inputs:

```text
mes_sin
mes_cos
t_index
T2M_lag1
T2M_lag3
T2M_lag6
T2M_MAX_lag1
T2M_MAX_lag3
T2M_MAX_lag6
WS2M_lag1
WS2M_lag3
WS2M_lag6
PRECTOTCORR_lag1
PRECTOTCORR_lag3
PRECTOTCORR_lag6
RH2M_lag1
RH2M_lag3
RH2M_lag6
num_emergencias_lag1
num_emergencias_lag3
num_emergencias_lag6
personas_afectadas_lag1
personas_afectadas_lag3
personas_afectadas_lag6
personas_damnificadas_lag1
personas_damnificadas_lag3
personas_damnificadas_lag6
hectareas_cultivo_perdidas_lag1
hectareas_cultivo_perdidas_lag3
hectareas_cultivo_perdidas_lag6
hectareas_cultivo_afectadas_lag1
hectareas_cultivo_afectadas_lag3
hectareas_cultivo_afectadas_lag6
```

Rama B GE, 39 inputs: los 33 de GC3 mas:

```text
avg_sentiment_lag1
avg_sentiment_lag3
avg_sentiment_lag6
n_noticias_lag1
n_noticias_lag3
n_noticias_lag6
```

La diferencia estructural GC3 -> GE queda acotada a +6 inputs NLP rezagados en
Rama B.

## 5. Alineacion temporal

Datos v2 verificados en `master_dataset_sutil_v2_features.csv`:

| Concepto | Valor |
|---|---:|
| Filas post-lag | 114 |
| TRAIN rows | 90 |
| Validation rows | 12 |
| Test rows | 12 |
| Lookback | 6 |
| TRAIN sequences | 84 |
| Validation targets | 12 |
| Test targets | 12 |

Construccion correcta:

- Construir secuencias sobre las 114 filas ordenadas.
- Cada target se asigna por la fecha del mes objetivo, no por las fechas del
  contexto.
- Una secuencia que termina en el mes `t` predice `y_(t+1)`.
- El contexto son las 6 filas estrictamente anteriores al objetivo.
- El target a predecir no aparece en su propia ventana de inputs.

Ejemplos:

| Target | Contexto permitido |
|---|---|
| 2024-01 | 2023-07..2023-12 |
| 2025-01 | 2024-07..2024-12 |

Esto permite evaluar los 12 meses de validation y los 12 meses de test sin
padding artificial.

## 6. Arquitectura historica v1

Notebook principal:

```text
notebooks/fase3/actividad_14_ge_lstm_attention.ipynb
```

Hecho observado en codigo v1:

| Elemento | Valor v1 GE |
|---|---|
| Canal A | `produccion_t`, 1 feature |
| Canal B | 23 exogenas |
| Secuencia | `SEQ_LEN = 6` |
| LSTM por rama | `LSTM(64, return_sequences=True)` |
| Regularizacion LSTM | `kernel_regularizer=l2(0.001)`, `recurrent_regularizer=l2(0.001)` |
| Dropout por rama | `Dropout(0.30)` tras cada LSTM |
| Attention | `BahdanauAttention(64)` independiente por rama |
| Fusion | `Concatenate` de dos contextos de 64 -> 128 |
| Dense | `Dense(64, relu, L2=0.001)` -> `Dropout(0.15)` -> `Dense(16, relu)` -> `Dense(1)` |
| Optimizer | Adam, learning rate `1e-3` |
| Loss | MSE |
| Metric | MAE |
| Batch | 8 |
| Max epochs | 300 |
| EarlyStopping | monitor `val_loss`, patience 15, `restore_best_weights=True` |
| ReduceLROnPlateau | monitor `val_loss`, factor 0.5, patience 8, min_lr 1e-6 |
| ModelCheckpoint | `save_best_only=True`, monitor `val_loss` |
| Seeds | `tf.random.set_seed(42)`, `np.random.seed(42)` |
| oneDNN | `TF_ENABLE_ONEDNN_OPTS=0` |
| Shuffle | no especificado en `model.fit` -> Keras default `shuffle=True` |
| Validation v1 | 15% final de las secuencias de train 80/20 historico |
| Parametros GE v1 | 65,249 |

El GM original v1 (`notebooks/fase4/actividad_15_multimodal_nlp.ipynb`) uso la
misma arquitectura e hiperparametros, con Canal B = 25 (23 + 2 NLP
contemporaneas) y 65,761 parametros. GM_v2/v3/v4 son historicos y no
equivalentes: cambian arquitectura, PCA, rama NLP y protocolo, por lo que no
son base limpia para v2.

## 7. Conteo de parametros v1 aplicado a v2

Formulas usadas:

```text
LSTM(input_dim=d, units=u) = 4*u*(d + u + 1)
BahdanauAttention(u, a) = u*a + u*a + a       # Dense sin bias
Dense(d_in, d_out) = d_in*d_out + d_out
```

La formula reproduce exactamente:

| Modelo historico | A | B | Parametros observados/reproducidos |
|---|---:|---:|---:|
| GE v1 | 1 | 23 | 65,249 |
| GM original v1 | 1 | 25 | 65,761 |

Aplicada a v2 con Rama A = 4 y Rama B = 33/39:

| Configuracion | A | B | Parametros |
|---|---:|---:|---:|
| GC3 heredada 64 | 4 | 33 | 68,577 |
| GE heredada 64 | 4 | 39 | 70,113 |

El aumento GE-GC3 es `4 * 6 * 64 = 1,536` parametros: solo el kernel de entrada
del LSTM de Rama B por las seis variables NLP adicionales.

## 8. Riesgo Small Data

Tamano disponible para redes:

| Concepto | N |
|---|---:|
| TRAIN target raw | 96 meses |
| TRAIN feature rows | 90 |
| Lookback | 6 |
| TRAIN sequences | 84 |
| Validation sequences | 12 |
| Test sequences | 12 |

Complejidad de arquitectura heredada aplicada a v2:

| Modelo | Parametros | Parametros / 84 secuencias |
|---|---:|---:|
| GC3 heredada | 68,577 | 816.39 |
| GE heredada | 70,113 | 834.68 |

Esto no invalida automaticamente la red, pero senala riesgo de sobreajuste,
alta varianza, dependencia fuerte de regularizacion, sensibilidad a seeds y
dificultad para justificar busquedas amplias de hiperparametros con solo 12
meses de validation.

## 9. Candidatos compactos

Todos los candidatos preservan:

- dos ramas recurrentes funcionales;
- LSTM;
- BahdanauAttention independiente por rama;
- lookback = 6;
- misma topologia GC3/GE salvo las seis NLP adicionales;
- salida escalar `y_(t+1)`;
- Dense posterior a fusion.

| Candidato | LSTM por rama | Attention | Dense |
|---|---:|---:|---|
| A HEREDADA | 64 | 64 | 64 -> 16 -> 1 |
| B COMPACTA-32 | 32 | 32 | 32 -> 8 -> 1 |
| C COMPACTA-16 | 16 | 16 | 16 -> 8 -> 1 |
| D MUY COMPACTA | 8 | 8 | 8 -> 4 -> 1 |

En el codigo historico, "Attention N" significa que `W_query` y `W_values`
proyectan desde el hidden size del LSTM hacia `N` unidades, y `V` proyecta de
`N` a un score escalar por timestep. No es multi-head attention.

## 10. Conteo exacto de parametros

Desglose:

| Candidato | Modelo | LSTM A | LSTM B | Att A | Att B | Dense posteriores | Output | Total |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| HEREDADA | GC3 | 17,664 | 25,088 | 8,256 | 8,256 | 9,296 | 17 | 68,577 |
| HEREDADA | GE | 17,664 | 26,624 | 8,256 | 8,256 | 9,296 | 17 | 70,113 |
| COMPACTA-32 | GC3 | 4,736 | 8,448 | 2,080 | 2,080 | 2,344 | 9 | 19,697 |
| COMPACTA-32 | GE | 4,736 | 9,216 | 2,080 | 2,080 | 2,344 | 9 | 20,465 |
| COMPACTA-16 | GC3 | 1,344 | 3,200 | 528 | 528 | 664 | 9 | 6,273 |
| COMPACTA-16 | GE | 1,344 | 3,584 | 528 | 528 | 664 | 9 | 6,657 |
| MUY COMPACTA | GC3 | 416 | 1,344 | 136 | 136 | 172 | 5 | 2,209 |
| MUY COMPACTA | GE | 416 | 1,536 | 136 | 136 | 172 | 5 | 2,401 |

Resumen:

| Candidato | GC3 params | GE params | Delta params | Delta % | Params/84 GC3 | Params/84 GE |
|---|---:|---:|---:|---:|---:|---:|
| HEREDADA | 68,577 | 70,113 | 1,536 | 2.24% | 816.39 | 834.68 |
| COMPACTA-32 | 19,697 | 20,465 | 768 | 3.90% | 234.49 | 243.63 |
| COMPACTA-16 | 6,273 | 6,657 | 384 | 6.12% | 74.68 | 79.25 |
| MUY COMPACTA | 2,209 | 2,401 | 192 | 8.69% | 26.30 | 28.58 |

El delta absoluto disminuye con `units`: `4 * 6 * units`.

## 11. Bahdanau Attention

Implementacion historica:

```python
self.W_query  = layers.Dense(units, use_bias=False)
self.W_values = layers.Dense(units, use_bias=False)
self.V        = layers.Dense(1, use_bias=False)

q_exp  = tf.expand_dims(self.W_query(query), axis=1)
energy = self.V(tf.nn.tanh(self.W_values(values) + q_exp))
alpha  = tf.nn.softmax(energy, axis=1)
ctx    = tf.reduce_sum(alpha * values, axis=1)
```

Auditoria:

| Punto | Observacion |
|---|---|
| Tensor `values` | Estados LSTM completos `(batch, seq_len, lstm_units)` |
| Tensor `query` | Ultimo estado `h[:, -1, :]` |
| Dimension de scores | Temporal, `axis=1` sobre `seq_len` |
| Context vector | Suma ponderada de `values`, shape `(batch, lstm_units)` |
| Requiere `return_sequences=True` | Si, porque necesita todos los estados |
| Atencion por rama | Si, una instancia independiente para A y otra para B |
| Parametros por bloque | `2*lstm_units*attn_units + attn_units`; sin bias |
| Equivalencia conceptual | Atencion aditiva tipo Bahdanau simplificada; query es el ultimo estado, no un decoder separado |

Ambiguedad: el nombre "BahdanauAttention" es conceptualmente razonable por la
forma aditiva `V(tanh(W_values(values)+W_query(query)))`, pero no reproduce un
decoder seq2seq completo. Es una variante temporal simplificada para resumen de
secuencias.

## 12. Regularizacion historica

| Componente | Valor v1 | Problema potencial Small Data | Mantener / revisar | Motivo |
|---|---|---|---|---|
| Dropout LSTM | 0.30 tras cada LSTM | Puede estabilizar, pero con pocos datos puede subajustar | Revisar | D4 abierto |
| Dropout head | 0.15 | Menor regularizacion en cabeza | Revisar | Depende de tamano elegido |
| recurrent_dropout | No observado | Sin regularizacion interna recurrente | Revisar | Podria afectar reproducibilidad/velocidad si se activa |
| L2 LSTM kernel/recurrent | 0.001 | Regularizacion fuerte con 84 secuencias | Revisar | Debe ser a priori, no ajustada por test |
| L2 Dense_1 | 0.001 | Penaliza fusion | Revisar | Coherente con v1, pero D4 abierto |
| EarlyStopping | patience 15, restore_best_weights=True | Validation de 12 meses puede ser ruidosa | Revisar | D3-bis/D4 abiertos |
| ReduceLROnPlateau | factor 0.5, patience 8, min_lr 1e-6 | Puede reaccionar a ruido de validation | Revisar | D4 abierto |
| ModelCheckpoint | save_best_only val_loss | Correcto para trazabilidad, pero depende de val_loss | Mantener/revisar | Debe ser identico GC3/GE |

No se propone HPO. Cualquier ajuste posterior deberia ser minimo, a priori y
documentado antes de entrenar.

## 13. Shuffle

V1:

- `model.fit(...)` no especifica `shuffle`.
- En Keras, el default de `Model.fit` para arrays es `shuffle=True`.
- Las secuencias ya estaban construidas antes del fit y separadas
  cronologicamente en train/validation.

Distincion metodologica:

| Caso | Riesgo |
|---|---|
| Mezclar observaciones antes de crear ventanas | Puede destruir estructura temporal y crear ventanas incoherentes |
| Mezclar secuencias ya construidas durante SGD | No implica automaticamente leakage si cada ventana esta bien construida, pero cambia dinamica de entrenamiento |

D7 no se cierra aqui. GC3 y GE deben usar el mismo valor cuando se decida.

## 14. Seeds y determinismo

V1 fijo:

- `tf.random.set_seed(42)`
- `np.random.seed(42)`
- `TF_ENABLE_ONEDNN_OPTS=0`

No observado:

- `PYTHONHASHSEED`
- `TF_DETERMINISTIC_OPS=1`
- `keras.utils.set_random_seed`
- control de determinismo de operaciones TensorFlow
- multi-seed

Conclusion de auditoria: `seed=42` en v1 fue reproducibilidad parcial, no
determinismo completo garantizado. D0/D10 no se cierran aqui.

## 15. Uso de validation

V1:

- Split historico 80/20.
- Validation interna: ultimas ~15% secuencias de train.
- EarlyStopping, ReduceLROnPlateau y checkpoint monitorean `val_loss`.
- No habia validation 2024 explicita.

V2 vigente:

- Validation 2024 existe como 12 targets cronologicos.
- No debe usarse test 2025 para seleccionar arquitectura.
- Probar muchas arquitecturas/hyperparams sobre 12 meses de validation inflaria
  el riesgo de seleccion oportunista.

Esta auditoria compara A/B/C/D solo por complejidad, coherencia arquitectonica,
parsimonia y capacidad de preservar la pregunta cientifica. No usa validation
para decidir.

## 16. Limpieza de la ablacion GC3 -> GE

Cada candidato preserva la afirmacion:

```text
GE mantiene arquitectura y protocolo de GC3 e incorpora unicamente seis
variables NLP rezagadas adicionales en la rama exogena.
```

Condiciones que deben permanecer identicas:

- lookback;
- unidades LSTM;
- attention;
- Dense;
- regularizacion;
- dropout;
- optimizer;
- learning rate;
- batch size;
- epochs maximas;
- callbacks;
- seeds;
- shuffle;
- split;
- scaler;
- protocolo de evaluacion.

La diferencia de parametros GE-GC3 es natural y aceptable si se reporta:

| Candidato | Delta parametros GE-GC3 | Origen |
|---|---:|---|
| HEREDADA | 1,536 | 6 inputs NLP en LSTM B con 64 unidades |
| COMPACTA-32 | 768 | 6 inputs NLP en LSTM B con 32 unidades |
| COMPACTA-16 | 384 | 6 inputs NLP en LSTM B con 16 unidades |
| MUY COMPACTA | 192 | 6 inputs NLP en LSTM B con 8 unidades |

No se exige igualdad exacta de parametros entre GC3 y GE.

## 17. Ventajas/desventajas de cada candidato

| Candidato | Ventajas | Desventajas |
|---|---|---|
| HEREDADA | Maxima continuidad con v1; capacidad alta; reproduce arquitectura historica | 68k-70k parametros para 84 secuencias; riesgo alto de varianza/sobreajuste |
| COMPACTA-32 | Reduce ~70% parametros vs heredada; preserva dos ramas y atencion con capacidad moderada | Sigue teniendo ~20k parametros; aun sensible a validation pequena |
| COMPACTA-16 | Mucho mas parsimoniosa; ~6k parametros; mantiene pregunta cientifica | Menor capacidad; podria subajustar patrones no lineales complejos |
| MUY COMPACTA | Muy defendible por small data; ~2.2k-2.4k parametros | Capacidad muy limitada; attention con 8 unidades puede ser demasiado estrecha |

## 18. Preguntas que deben decidirse externamente

1. Cerrar D36: confirmar Rama A=4 y Rama B=33/39 como definicion oficial, o
   excluir `produccion_t_{c}` contemporaneo de la fila-contexto y volver a 36/42.
2. Cerrar D4: elegir familia de unidades/densas sin HPO amplio.
3. Decidir valores finales de dropout, L2 y callbacks.
4. Decidir `shuffle` para secuencias ya construidas.
5. Decidir numero de seeds y protocolo de agregacion.
6. Decidir flags de determinismo antes del primer entrenamiento.
7. Decidir si se permitira un control placebo de NLP permutado, fuera del alcance
   de esta auditoria.
8. Documentar formalmente D20-d si aun no se ha incorporado a
   `DECISIONES_METODOLOGICAS.md`.

## 19. Recomendacion tecnica NO vinculante

**RECOMENDACION TECNICA NO VINCULANTE:** COMPACTA-16 parece el candidato mas
defendible como punto de partida metodologico: mantiene dos ramas recurrentes,
Bahdanau Attention, la ablacion limpia GC3 -> GE y reduce la complejidad a
6,273/6,657 parametros. Es sustancialmente mas parsimoniosa que la heredada,
sin caer en la capacidad extremadamente estrecha de MUY COMPACTA.

Esta recomendacion no cierra D4 ni D36. La decision final debe tomarse
externamente antes de cualquier implementacion o entrenamiento.
