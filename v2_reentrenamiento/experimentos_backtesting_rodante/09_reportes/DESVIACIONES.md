# Desviaciones respecto del preregistro (D-BR12)

Cada cambio posterior al preregistro D-BR1..D-BR12 (`DECISIONES_METODOLOGICAS.md`, commit `db32108`) se registra aquí.
Tipos: **implementación** (no cambia qué se estima ni cómo se evalúa) o **protocolo** (cambia el análisis).

---

## DESV-01 — Hash SHA-256 con fin de línea normalizado

- **Fecha:** 2026-10-28. **Tipo:** cambio de implementación, no de protocolo. **Aprobado por:** el autor, antes de la Fase C.
- **Afecta a:** D-BR11 (verificación de integridad). No cambia datos, código oficial, modelos ni métricas.
- **Motivo:** el repositorio usa `core.autocrlf=true`. Git guarda los archivos de texto con LF y en Windows los escribe con CRLF, así que el hash de los bytes depende de la máquina donde se clona. Con el método anterior, un clon en otro sistema abortaría la verificación sin que hubiera cambiado nada.
- **Método nuevo (versión 2):**
  - Archivos de texto, por extensión (`.py`, `.csv`, `.json`, `.md`, `.txt`, `.mjs`, `.cfg`, `.toml`, `.yaml`, `.yml`, `.ini`): se reemplaza CRLF por LF y luego se calcula el SHA-256.
  - Cualquier otra extensión (por ejemplo `.joblib`, `.safetensors`) es binaria: SHA-256 de los bytes tal cual. `model.safetensors` contiene bytes 0D 0A propios del formato y por eso no se normaliza.
  - El método queda escrito en `01_configuracion/hashes_congelados.json`, clave `_metodo`.
- **Procedimiento (una sola vez):** `02_scripts/00_verificar_entorno.py --normalizar-hashes`.
  1. Verificó los 93 archivos byte a byte contra el archivo anterior: 0 diferencias.
  2. Guardó el archivo anterior como `01_configuracion/hashes_congelados_v1_bytes.json`.
  3. Regeneró `hashes_congelados.json` con el método 2 y volvió a verificar: 93 archivos, 0 diferencias.
  4. El script se niega a repetir la operación si el archivo ya tiene `_metodo`.
- **Archivos cuyo hash cambió de valor:** 41 que tenían CRLF en disco (23 de `v2_reentrenamiento/src`, 18 de datos y referencias). El contenido no cambió.
- **Comprobación:** para los 89 archivos que están en `HEAD`, el hash normalizado coincide con el SHA-256 del contenido guardado en git (`git show HEAD:<ruta>`). Los 2 que no están en `HEAD` y los 2 de la caché de Hugging Face no se pueden comprobar así.

---

## DESV-02 — Enmienda E-BR1 a D-BR7 (definición de "robusto")

- **Fecha:** 2026-10-28. **Tipo:** cambio de protocolo (criterio de interpretación), hecho antes de ejecutar la Fase C. **Aprobado por:** el autor.
- **Texto:** al final de `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md` ("Enmienda E-BR1 a D-BR7 del preregistro D-BR1..D-BR12"). El texto original de D-BR7 (línea 1818) no se reescribe.
- **Cambio:** se anula solo la cláusula "se exige un MAE_shock bajo en toneladas"; se conserva "Un Δs bajo por sí solo no es evidencia de resiliencia". Robusto = primero, o empatado con el primero, en al menos 3 de P50, P60, P75 y P90 (variación mensual); interanual aparte; por cultivo.
- **Motivo:** la cláusula no formaba parte del criterio pedido por el autor.
- **Efecto en el código:** ninguno. `01_configuracion/umbrales.json` ("robustez") ya definía solo la regla de 3 de 4 umbrales.
- **Textos de planificación que conservan la frase anterior** (históricos, no se editan): `00_plan/plan_fase_A.md:266` y `00_plan/borrador_preregistro.md:118`.

---

## DESV-03 — Pruebas b02 y b04 de la Fase B leen `control_2025_v3`

- **Fecha:** 2026-10-28. **Tipo:** cambio de implementación, no de protocolo.
- **Problema:** al revisar DA4 (α reelegido por ventana), `hiperparametros_congelados.json` pasó a guardar los valores de v3 en `ridge.control_2025_v3.<cultivo>` y `elasticnet.control_2025_v3.<cultivo>`. `b02_reproducir_elasticnet_v3.py` (l. 85) y `b04_medir_tiempos.py` (l. 89-90) seguían leyendo `HP["elasticnet"][cultivo]` y `HP["ridge"][cultivo]`, que ya no existen, y fallaban con `KeyError`. Así, `run_all.py --fase B` no se podía repetir.
- **Corrección:** las tres lecturas usan ahora `["control_2025_v3"][cultivo]`. Los valores son los mismos (Ridge α Sutil 10, Dulce 100; ElasticNet Sutil 0.1/0.5, Dulce 1/0.2).
- **Comprobación:** se reejecutaron solo esas dos pruebas (código de salida 0 en ambas). `elasticnet_predicciones_2025.csv` es idéntico byte a byte al de la Fase B, y `elasticnet_reproduccion.json` es idéntico salvo los tiempos medidos. `tiempos_fase_b.json` se reescribió con tiempos nuevos, del mismo orden que los anteriores.

---

## DESV-04 — SARIMA-sel: órdenes con AIC no finito

- **Fecha:** 2026-10-28. **Tipo:** cambio de implementación (precisión de un caso que D-BR10 no cubre).
- **D-BR10, paso 2:** se descartan y se cuentan los órdenes cuyo ajuste lanza una excepción.
- **Caso no previsto:** un ajuste que **no** lanza excepción pero devuelve AIC NaN o ±inf. No se puede ordenar por AIC.
- **Tratamiento:** esos órdenes quedan fuera del ranking por AIC (paso 3), se cuentan aparte en `info.json` (`n_aic_no_finito`) y siguen en `rejilla.csv` con su AIC tal como salió. No se cuentan como "fallidos" (`n_fallidos` = solo excepciones).
- **Efecto:** si quedan al menos 5 órdenes con AIC finito, el top-5 se forma solo con ellos; si quedaran menos de 5, se toman los que haya y se registra. En la prueba de la ventana 2021 no hubo ninguno: 0 fallidos y 0 AIC no finitos de 576 en ambos cultivos (78 y 30 sin convergencia en la rejilla, que no se excluyen, como en GC1).
- **Regla de elección completa (D-BR10, pasos 3-5), tal como está implementada en `lib/modelos_series.py::sarima_sel`:**
  1. Top-5: AIC de train con maxiter 200 ascendente; empate → orden de la rejilla (`itertools.product(p, d, q, P, D, Q)`).
  2. Cada uno de los 5 se reajusta en train con maxiter 500 y se calcula su MAE de validación a 1 paso sobre Y−1.
  3. Elegido: menor MAE de validación; empate → menor AIC (el de la rejilla, maxiter 200, que es el que define la posición 1-5); empate → orden de la rejilla.
  4. Se registra también el AIC con maxiter 500 de cada candidato (`candidatos.csv`), pero no se usa para elegir.

---

## DESV-05 — Criterio de equivalencia 2025 de la Fase D1 ajustado para Δs (decidido después de ver el resultado)

- **Fecha:** 2026-10-28. **Tipo:** cambio de implementación (criterio de una prueba), **decidido después de ver el resultado**.
- **Prueba:** `02_scripts/tests/test_equivalencia_metricas_2025.py` compara la ventana 2025 del backtesting con `resultados_v2_final` (GC3, GE y XGB-37 por semilla; SARIMA rolling).
- **Criterio original** (fijado al escribir la prueba): diferencia relativa ≤ 1e-6 en todas las métricas. **No se cumplió** en Dulce GC3 (1,18e-6) y GE (1,43e-6), y solo en Δs (p. ej. GE semilla 4: 3,08605 % frente a 3,08605 % oficial; diferencia absoluta 4,4e-6 puntos porcentuales).
- **Causa:** Δs es un porcentaje cercano a 0, así que la diferencia relativa se amplifica. El origen es la diferencia ya conocida desde la Fase B: las predicciones de GC3/GE difieren ≤ 5e-4 t de las oficiales por redondeo float32; los pesos entrenados son idénticos bit a bit.
- **Criterio aplicado:** relativa ≤ 1e-6 en todas las métricas salvo Δs, y absoluta ≤ 1e-4 puntos porcentuales en Δs. Con él, los 8 casos cultivo×modelo dan OK. El veredicto con el criterio original también queda guardado (`OK_criterio_original: false` en `06_metricas/reproducibilidad/metricas_2025_vs_oficial.json`).

---

## DESV-06 — Decisiones de implementación de la Fase D1 no fijadas en D-BR5/D-BR7

- **Fecha:** 2026-10-28. **Tipo:** implementación. Se fijaron al escribir `lib/evaluacion.py`, `08_metricas.py` y `09_barrido_umbral.py`, antes de calcular las tablas; ninguna depende de los resultados.
- **Estado:** PENDIENTE DE APROBACIÓN del autor (texto presentado el 2026-10-28).

1. **RMSE con semillas:** RMSE de cada semilla sobre los meses del conjunto, y media entre semillas. Paralelo a "MAE = media de los MAE por semilla" (A3).
2. **Bootstrap iid:** MAE global sobre todos los meses; MAE_shock y MAE normal remuestreando solo dentro de los meses de shock o de normalidad de cada ventana (A5). Mismos índices para todos los modelos; generador nuevo con semilla 42 en cada llamada.
3. **Bootstrap por bloques (sensibilidad):** bloques móviles circulares de 3 meses dentro de cada ventana (4 bloques = 12 meses). MAE_shock y MAE normal se calculan como cociente sobre los meses remuestreados de cada tipo; los remuestreos sin ningún mes de ese tipo se descartan y se cuentan (`bloq_shock_remuestreos_sin_shock`).
4. **Empates y "primero":** con el IC iid de la diferencia con el mejor (principal). La columna "robusto men (bloques)" repite la regla con bloques. **Exploratorio:** D-BR5 prevé los bloques para los intervalos, no para la regla de robustez.
5. **Diebold-Mariano-HLN:** h = 1, pérdida absoluta sobre el error promediado entre semillas, ventanas agrupadas (n = 60 por cultivo en la principal, 36 en la sensibilidad), t(n−1) bilateral. "Solo shocks" usa la máscara P75 mensual. **Sin corrección por comparaciones múltiples** (no preregistrada); se reportan los p-valores sin ajustar, con la advertencia de baja potencia. La corrección de Holm se añadió aparte como análisis exploratorio (DESV-07).
6. **Model Confidence Set:** opcional en D-BR5; **no se calculó**.
7. **Cuartiles y tercios de |Δy/y|:** cortes = cuantiles del cambio mensual sobre los 60 meses de test agrupados de cada cultivo (`pd.qcut`); 15 o 20 meses por grupo.
8. **Descripción de las series:** train desde 2016-07 (inicio de features y SARIMA, D-BR1). ACF, ADF y KPSS solo si n ≥ 24 (por eso no hay valores en las ventanas 2018 y 2019). ADF con constante y retardos por AIC; KPSS con constante y `nlags="auto"`; el p de KPSS está acotado a [0,01; 0,10] por la tabla de statsmodels.
9. **D-BR8:** el MAE de train y de validación del Naive de referencia se calcula en los mismos meses objetivo de cada modelo (tabulares desde 2016-08; LSTM desde 2017-01).
10. **MASE:** en la ventana 2018, D_w no está definido (el train no tiene meses con y_(t−12)) y MASE queda vacío.
11. **Δs quitando un mes:** el mes sale a la vez de los shocks y del MAE global.
12. **Estabilización del barrido:** "primer lugar estable desde k" = menor k tal que el modelo de menor MAE_shock es el mismo en todos los percentiles ≥ k; "todos empatan desde k" = menor k tal que en todos los percentiles ≥ k todos los modelos son "primero". Con pocos shocks (P95: 1–3 meses) el resultado es degenerado y se interpreta con esa cautela.
13. **Formato:** los CSV tienen precisión completa; las vistas en texto y LaTeX se redondean.

---

## DESV-07 — Corrección de Holm sobre Diebold-Mariano (EXPLORATORIO)

- **Fecha:** 2026-10-28. **Tipo:** análisis adicional **exploratorio**, pedido por el autor después de ver los resultados de la Fase D1. No forma parte del preregistro (D-BR5 no prevé corrección por comparaciones múltiples) ni de la regla de "mejor" (D-BR7), que se basa en el bootstrap.
- **Qué se calcula:** Holm (`statsmodels.stats.multitest.multipletests`, `method="holm"`) sobre los p-valores DM-HLN de cada familia = conjunto (2021–2025 o 2023–2025) × cultivo × alcance (global o solo shocks P75 mensual). Cada familia tiene 45 pares (10 modelos); se excluyen los pares sin p-valor (no hubo ninguno).
- **Salida:** columna `p_holm_exploratorio` en `06_metricas/significancia/T09_diebold_mariano_hln.csv`; matrices `T09_DM_holm_matriz_*`; resumen `T09_holm_resumen` (pares con p < 0,05 antes y después). Los p-valores sin ajustar no cambian.
- **Model Confidence Set:** sigue sin calcularse, por decisión del autor.

---

## DESV-08 — Implementación de las figuras de la Fase D2 frente a `00_plan/mapa_evidencia.md`

- **Fecha:** 2026-10-28. **Tipo:** implementación (presentación). Ninguna cifra cambia: todas las figuras se dibujan desde CSV ya guardados.
- **Estado:** pendiente de aprobación del autor.

1. **fig01 (curvas de pérdida):** el mapa decía "media ± banda entre semillas". Se dibuja **una línea por semilla** (y su mejor época), porque las semillas paran en épocas distintas y una media por época sería una cifra nueva que no está en ningún CSV.
2. **fig20:** los modelos con semilla se muestran con una línea por semilla, no con la media de predicciones (el protocolo no usa ensambles, A3).
3. **fig11:** IC marginales de MAE_shock solo en P50, P60, P75 y P90. Para los demás percentiles no hay IC por modelo guardados (el barrido guarda solo el IC de la diferencia con el mejor).
4. **fig21:** la escala de color logarítmica empieza en el percentil 2 de los errores de cada cultivo; los errores menores se pintan con el color mínimo (`extend="min"`). Es solo escala de color; el CSV conserva los valores.
5. **fig02:** la razón del Naive dibujada es la de los meses de los modelos tabulares; en los meses de las LSTM (desde 2017-01) difiere como máximo en 0,030 (Sutil) y 0,015 (Dulce). Se indica en el pie.
6. **No generadas** (fuera de la especificación de la Fase D2): fig22 y fig23 (residuos) y fig24–fig28 (figuras independientes). fig18 y fig19 se generaron después, a pedido del autor (puntos 9 y 10).
7. **XGB-43** (exploratorio) no se incluye en ninguna figura.
8. **Leyendas** fuera del área de datos, lo que corrige lo señalado en la figura de prueba de la Fase B.
9. **fig18 (ablación GE − GC3):** desde `06_metricas/ablacion_nlp/T11_ablacion_GE_menos_GC3.csv`. Muestra las 10 diferencias por semilla emparejada (puntos) y la media con su IC bootstrap pareado guardado (rombo), por ventana y agregado (2021–2025 y 2023–2025), en tres paneles: todos los meses, shocks P75 y meses normales. "n/a" donde no hay meses de shock (Sutil 2021 y 2024).
10. **fig19 (matriz Diebold-Mariano-HLN):** desde `06_metricas/significancia/T09_diebold_mariano_hln.csv`, solo con el conjunto principal 2021–2025 (la sensibilidad 2023–2025 queda en la tabla). Bajo la diagonal: todos los meses; sobre la diagonal: solo shocks P75. Color por tramos de p (< 0,01; < 0,05; < 0,10; ≥ 0,10) y signo "+" si el modelo de la fila tiene más error. **La versión con Holm (exploratoria, DESV-07) va en un archivo aparte**, `fig19b_dm_pvalue_matrix_holm_{cultivo}`, marcada como EXPLORATORY en la leyenda y el pie. Así fig19 tiene dos archivos por cultivo.
