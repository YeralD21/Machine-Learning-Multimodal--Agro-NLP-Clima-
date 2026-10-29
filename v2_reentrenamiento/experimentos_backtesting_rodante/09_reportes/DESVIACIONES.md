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
