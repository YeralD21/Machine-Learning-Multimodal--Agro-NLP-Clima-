# Diagnóstico de estructura y propuesta de reorganización

Fecha: 2026-08-10
Estado: **Solo diagnóstico — ningún cambio fue ejecutado.**

---

## 1. Hallazgos de la investigación

### 1.1 `./20%` — archivo vacío accidental

```
-rw-r--r-- 1 YERALD 197121 0 Jun  2 23:59 ./20%
```

- Es un **archivo de 0 bytes** (no carpeta), rastreado por git (`git ls-files` lo lista como `20%`).
- Se añadió en el commit `16bab43` — *"feat: dashboard web completo con landing, resultados, SHAP, attention, NLP y simulación animada"*. Casi seguro es un artefacto accidental (p. ej. una redirección de shell mal escrita tipo `algo 20% > "20%"` durante ese commit), sin relación real con el dashboard.
- **Nada en el código lo referencia** (verificado por grep en todo el repo).

**Veredicto: seguro de eliminar.** Es basura de commit, 0 bytes, sin ningún consumidor. Como está trackeado, hay que hacer `git rm "./20%"`, no solo borrar el archivo del disco.

### 1.2 `notebooks/fase2/notebooks/fase2/` — duplicados obsoletos, NO idénticos

```
notebooks/fase2/actividad_03_rezagos_temporales.ipynb                168 KB
notebooks/fase2/actividad_04_normalizacion_escalado.ipynb            172 KB
notebooks/fase2/notebooks/fase2/actividad_03_rezagos_temporales.ipynb 11 KB
notebooks/fase2/notebooks/fase2/actividad_04_normalizacion_escalado.ipynb 14 KB
```

- No son copias byte-a-byte: el `diff` da 622 y 748 líneas de diferencia respectivamente. Los archivos anidados son **versiones más antiguas y más pequeñas** (probablemente sin outputs de ejecución completos).
- Ambos están trackeados en git, añadidos en el commit `5e6f51f` — *"fase 2 completa el dataset final esta en data/procesed data final fase 2"*, **anterior** al commit que dejó las versiones buenas en `notebooks/fase2/` directamente.
- Causa probable: un script generador se ejecutó con el cwd ya dentro de `notebooks/fase2/` pero usando una ruta relativa `notebooks/fase2/actividad_XX...ipynb`, duplicando el prefijo.
- **Nada en el código (`*.py`, `*.json`) referencia la ruta anidada** (verificado por grep).

**Veredicto: seguro de eliminar.** Son versiones obsoletas y huérfanas, superadas por los archivos reales en `notebooks/fase2/`. `git rm -r notebooks/fase2/notebooks/`.

---

## 2. Correcciones importantes a la propuesta original

Antes de la estructura propuesta, dos correcciones que cambian el riesgo de los cambios sugeridos:

### 2.1 `resultados/*_ext` y `resultados/*_final` NO son experimentos fallidos

El propio `CLAUDE.md` los documenta como etapas intencionales:

> `_ext` = retrained on extended 2019-2025 data (n_train=68), `_final` = final version.

Y están **activamente referenciados** por notebooks vigentes:
- `notebooks/fase4/actividad_16_reentrenamiento_extendido.ipynb` escribe en `resultados/{ge,gm_v3,xgboost}_ext/`
- `notebooks/fase4/actividad_17_reentrenamiento_final.ipynb` escribe en `resultados/{ge,gm_v3,xgboost}_final/`
- `notebooks/fase4/evaluar_shocks_extendido.ipynb` **lee** `resultados/ge_ext/predicciones.csv` y `resultados/gm_v3_ext/predicciones.csv`

Eliminarlos rompe la trazabilidad del reentrenamiento (n_train=44 → 68) y las comparativas que ya están en `resultados/comparativa_extendido.json` y `resultados/ranking_final_44vs68.json`. **No deben eliminarse ni archivarse fuera de `resultados/`.**

### 2.2 `pipeline/` está más acoplado de lo que parece — mover es de riesgo medio, no trivial

- `run_pipeline.py` tiene **10 rutas hardcodeadas** (`pipeline/actividad_01_...ipynb` … `_10_...ipynb`, líneas 37-46).
- `pipeline/config/pipeline_config.json` (generado por la Actividad 1) contiene **rutas de salida auto-referenciadas** dentro del propio JSON: `pipeline/output/02_lectura/`, `.../03_eda/`, etc. Si se mueve la carpeta, ese JSON queda apuntando al lugar viejo hasta que se regenere.
- `README.md` documenta explícitamente `pipeline/` tres veces (dónde colocar los CSV de INDECI, dónde están los notebooks de Fase 1, en el árbol de carpetas final).
- **Hallazgo adicional no pedido pero relevante:** existe una segunda vía de pipeline, ya rota, que usa una ruta de config distinta y obsoleta:
  - `main_fase1.py`, `src/data_processing/actividad_0{2,3,4,5,6_07,8,9,10}.py`, y los scripts sueltos `gen_nb_03_04.py`, `gen_nb_05_06.py`, `gen_nb_07_08.py`, `gen_nb_09_10.py`, `exec_nb_*.py` leen `data/02_interim/pipeline_config.json`.
  - **Esa carpeta (`data/02_interim/`) no existe en el repo actual.** El pipeline "vivo" y documentado en `CLAUDE.md` usa `pipeline/config/pipeline_config.json` (con `02_interim` → sustituido por las carpetas por-fuente dentro de `pipeline/output/`). Es decir, `main_fase1.py` tal como está hoy probablemente **no corre** (fallaría al abrir el config), y los `gen_nb_*.py`/`exec_nb_*.py` de la raíz son scripts generadores de un solo uso ya ejecutados (última modificación 2026-04-30), cuyo propósito (crear los `.ipynb`) ya se cumplió — los notebooks resultantes ya están commiteados.
  - Esto no cambia la recomendación de moverlos a `scripts/`, pero sí significa que **no hay que preservar esas rutas relativas rotas** al moverlos — es limpieza de código muerto, no una migración funcional.

### 2.3 `dashboard/app.py` tiene 9 rutas de `resultados/` hardcodeadas, relativas a la raíz del repo

```python
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent  # project root
P_GE_PREDS = BASE_DIR / "resultados/ge/ge_predicciones.csv"
...
```

Usa: `resultados/ge`, `resultados/gc1`, `resultados/gc2`, `resultados/shap`, `resultados/validacion`, `resultados/xgboost`, `resultados/gm_v2`.

**No usa `resultados/gm_v3`** (el modelo ganador según `CLAUDE.md`) — el dashboard actual muestra GM_v2 en las figuras de NLP/PCA, no GM_v3. Esto es una discrepancia entre el modelo recomendado y lo que el dashboard exhibe, pero es un problema aparte de la reorganización — lo señalo porque cualquier reestructuración de `resultados/` debe tener en cuenta que **BASE_DIR es siempre la raíz del repo**, así que mover o renombrar cualquiera de esas 7 subcarpetas rompe el dashboard salvo que se actualicen esas líneas.

### 2.4 `docs/` es un espejo manual de `dashboard/`, no un build automatizado

`docs/` contiene copias 1:1 de `dashboard/*.py` y `dashboard/*.html` (incluidos los `app_backup*.py`), creadas en el commit *"feat: GitHub Pages — copiar dashboard a /docs para despliegue"*. No hay script ni CI que sincronice ambas carpetas — es una copia manual. El usuario pidió no tocar `docs/`, lo respeto, pero dejo constancia de que si se reorganiza `dashboard/` (p. ej. limpiando los 5 `app_backup*.py`), `docs/` quedará desincronizado hasta que alguien repita la copia manual.

---

## 3. Estructura propuesta (evaluada archivo por archivo)

```
/
├── CLAUDE.md
├── README.md
├── requirements.txt
├── run_pipeline.py
├── .gitignore
│
├── src/                    (código fuente — sin cambios)
│
├── notebooks/
│   ├── fase1/              (ETL + pipeline actual de pipeline/)
│   ├── fase2/              (NLP, lags, scaling — solo los no duplicados)
│   ├── fase3/              (GC1, GC2, GE)
│   └── fase4/              (GM, evaluación, simulaciones)
│
├── resultados/             (todas las carpetas de modelo, incl. _ext/_final)
│
├── data/
│   ├── raw/
│   ├── interim/
│   └── processed/
│
├── docs/                   (GitHub Pages — no tocar)
└── dashboard/              (fuente del dashboard)
```

### 3.1 Eliminar

| Ítem | ¿Seguro? | Por qué | ¿Rompe rutas? | ¿Requiere actualizar docs? |
|---|---|---|---|---|
| `./20%` | ✅ Sí | 0 bytes, sin referencias, artefacto de commit | No | No |
| `notebooks/fase2/notebooks/fase2/` | ✅ Sí | Versiones obsoletas (11-14 KB) superadas por las reales (168-172 KB), sin referencias | No | No |
| `resultados/*_ext`, `resultados/*_final` | ❌ **NO eliminar** | Documentados en `CLAUDE.md`, generados y leídos activamente por `actividad_16`, `actividad_17`, `evaluar_shocks_extendido.ipynb` | **Sí rompe** `evaluar_shocks_extendido.ipynb` y la trazabilidad 44→68 | Sí — pero para explicar por qué se conservan, no para archivarlos |

### 3.2 Mover

| Ítem | ¿Seguro? | Por qué | ¿Rompe rutas? | ¿Requiere actualizar docs? |
|---|---|---|---|---|
| `pipeline/` → `notebooks/fase1/` | ⚠️ Riesgo medio | Coherente con el resto de fases, pero `pipeline/` tiene acoplamiento real | **Sí**: 10 rutas en `run_pipeline.py` (líneas 37-46) + rutas auto-referenciadas dentro de `pipeline/config/pipeline_config.json` (regenerado por Actividad 1, tendría que reflejar el nuevo prefijo `notebooks/fase1/output/...`) | Sí: `README.md` (3 menciones) y `CLAUDE.md` (sección "Running the Project" y "Data Flow") |
| `gen_nb_*.py`, `exec_nb_*.py` (raíz) | ✅ Sí, pero como limpieza de código muerto | Ya cumplieron su función (generar los `.ipynb` ya commiteados); su ruta de config `data/02_interim/pipeline_config.json` ya no existe — no funcionan hoy | No rompen nada activo porque nada más los invoca (verificado por grep) | Sí: `CLAUDE.md` menciona `generate_notebooks.py` y `gen_nb_fase2.py` (nombres distintos) como los generadores vigentes — aclarar cuáles siguen vivos |
| `generar_*.py` (raíz: figuras del dashboard) | ⚠️ Verificar primero | No se encontró que sean invocados por otro script, pero producen los PNG que sí usa `dashboard/app.py` (p. ej. `fig_shap_shocks.png`) | Posible, si algún flujo manual (no capturado en grep) los ejecuta desde la raíz asumiendo rutas relativas de salida a `dashboard/` | Sí si se documenta el flujo de generación de figuras |

### 3.3 No mencionado en la propuesta pero relevante

- `dashboard/app_backup*.py` (5 archivos, duplicados también en `docs/`) — candidatos a limpieza real, pero fuera del alcance de esta propuesta salvo que se pida explícitamente.
- Discrepancia GM_v3 vs GM_v2 en el dashboard — no es un problema de estructura de carpetas, pero afecta si se decide "promover" `resultados/gm_v3` como carpeta canónica del modelo ganador.

---

## 4. Recomendación de orden de ejecución (cuando se autorice)

1. **Bajo riesgo, ejecutar primero:** `git rm "./20%"` y `git rm -r notebooks/fase2/notebooks/`.
2. **Medio riesgo, requiere tocar 2-3 archivos de código:** mover `pipeline/` → `notebooks/fase1/`, actualizando `run_pipeline.py`, regenerando `pipeline_config.json` y actualizando `README.md`/`CLAUDE.md`.
3. **Limpieza de código muerto, verificar antes:** mover o eliminar `gen_nb_*.py`/`exec_nb_*.py` de la raíz — confirmar con el usuario si prefiere eliminarlos (ya no funcionan) o archivarlos en `scripts/legacy/`.
4. **No tocar:** `resultados/*_ext`, `resultados/*_final`, `docs/`.

No se ha ejecutado ningún cambio. Quedo a la espera de qué partes de esta propuesta autorizas.

---

## 5. Auditoría exhaustiva carpeta por carpeta

Fecha: 2026-08-12
Estado: **Solo diagnóstico ampliado — ningún cambio fue ejecutado.**

### 5.1 Carpetas XGBoost — corrección: son 3, no 4

```
$ find . -iname "*xgboost*" -not -path "./.git/*"
./notebooks/fase4/actividad_15v3_xgboost_competidor.ipynb   ← notebook fuente, no carpeta
./resultados/xgboost
./resultados/xgboost_ext
./resultados/xgboost_final
```

Solo hay **3 carpetas** (el cuarto resultado del `find` es el notebook que las genera, no una carpeta). Es el mismo patrón `base / _ext / _final` que ya documenta `CLAUDE.md` para GE y GM_v3 (ver §2.1) — **no es una duplicación accidental**, es una convención repetida 3 veces en el proyecto:

| Carpeta | Contenido | Commit / fecha | Referenciada por | Veredicto |
|---|---|---|---|---|
| `resultados/xgboost/` | `xgb_metricas.json`, `xgb_predicciones.csv`, `xgb_feature_importance.png`, `xgb_predicciones_vs_real.png` (201 KB) | `16bab43` — 2026-06-02 (dashboard completo) | `dashboard/app.py` (4 rutas hardcodeadas, líneas 30-31, 881, 884) y **los 4 scripts raíz** `generar_6modelos_figuras.py`, `generar_impacto_economico.py`, `generar_delta_shocks.py`, `visualizar_shocks_comparativo.py` (todos leen `resultados/xgboost/xgb_predicciones.csv`) | **Versión "viva"/canónica** — es la que alimenta el dashboard y las figuras. Conservar tal cual. |
| `resultados/xgboost_ext/` | Solo `metricas.json` + `predicciones.csv` (6 KB, sin PNG ni modelo) | `2ef26bc` — 2026-06-24 (Jornada Científica: Fase 4 + figuras) | Escrita por `actividad_16_reentrenamiento_extendido.ipynb`; leída por `evaluar_shocks_extendido.ipynb` | Etapa intermedia intencional (n_train=68). Conservar — coherente con `ge_ext`, `gm_v3_ext`. |
| `resultados/xgboost_final/` | Solo `metricas.json` + `predicciones.csv` (6 KB) | `2ef26bc` — 2026-06-24 | Escrita por `actividad_17_reentrenamiento_final.ipynb`. **No se encontró ningún script/notebook que la lea** (a diferencia de `xgboost_ext`, que sí es leída por `evaluar_shocks_extendido.ipynb`) | Versión final intencional, coherente con `ge_final`, `gm_v3_final` — pero es la única de las 3 tríadas cuya salida "final" no tiene un consumidor downstream verificado. No es basura (el propio `CLAUDE.md` la documenta como versión final del reentrenamiento), pero su propósito hoy es solo de archivo/trazabilidad, no de alimentar el dashboard. |

**Conclusión sobre XGBoost:** ninguna de las 3 carpetas es prescindible ni "una versión vieja de otra" — son 3 etapas distintas del mismo pipeline de reentrenamiento (44 → 68 muestras → final), igual que en GE y GM_v3. **No fusionar ni eliminar.** La única duda real (no una carpeta a eliminar) es si `xgboost_final` debería también alimentar el dashboard en vez de quedar solo como registro — eso es una decisión de producto, no de estructura de carpetas.

### 5.2 Tabla completa de carpetas raíz

| Carpeta | Archivos | Tipos principales | Estado | Veredicto |
|---|---|---|---|---|
| `.claude/` | 1 | json (config del CLI) | Activa (config de Claude Code, no de datos) | Mantener, fuera del alcance de la reorganización |
| `dashboard/` | 22 | 8 png, 7 html, 6 py | Activa (`app.py`, `app_dash.py` son código vivo); **4 de los 6 `.py` son backups** (`app_backup.py`, `app_backup_.py`, `app_backup_20260601_142202.py`, `app_backup_dash_migration.py`) | Mantener carpeta; **archivar o eliminar los 4 backups** (ya señalado en §3.3 del diagnóstico original, aquí confirmado: no son importados por nada, solo duplican `app.py`) |
| `data/` | 65 | 42 png, 14 csv, 6 txt | Activa pero **internamente fragmentada**: `02_interim_nasa/`, `03_processed/`, `03_processed_nasa/`, `04_reports/`, `external/`, `interim/`, `processed/` — 7 subcarpetas con convenciones de nombre distintas (`02_...`/`03_...` numeradas al estilo pipeline vs. `interim`/`processed` al estilo cookiecutter) | **Fusionar internamente** — no con otra carpeta raíz, sino consolidar las 7 subcarpetas en la jerarquía `raw/interim/processed` que ya propone la estructura objetivo en §3 |
| `database/` | 3 | sql (DDL) | Activa — referenciada por `CLAUDE.md`, `main_fase1.py`, `main_nasa_pipeline.py`, 5 notebooks de `pipeline/` y `src/data_processing/` | Mantener, sin cambios |
| `docs/` | 24 | 8 png, 7 html, 6 py, 2 md | Activa (GitHub Pages) — espejo manual de `dashboard/` (mismos 4 backups duplicados) + 2 `.md` que **no existen en `dashboard/`** (`pipeline_fases_234.md`, `nasa_power_pipeline_documentacion.md`) | No tocar (pedido explícito del usuario), pero nota: esos 2 `.md` son documentación real, no copias — si se reorganiza `docs/` no son candidatos a eliminar como los backups |
| `models/` | 4 | pkl (scalers) | Activa — `models/scalers/*.pkl` referenciado por `main_nasa_pipeline.py`, `pipeline/actividad_09_etl.ipynb`, `pipeline/actividad_10_reexploracion.ipynb`, `src/data_processing/nasa_pipeline/actividad_09_etl_nasa.py` (vivos) y también por `main_fase1.py`, `src/data_processing/fase2_nlp_lags.py`, `src/data_processing/master_unification.py` (parte del "segundo pipeline roto" de §2.2) | Mantener — es un directorio pequeño y activo, pero convive con otros 2 directorios de scalers (`notebooks/fase2/scalers/`, `resultados/*/​*_scalers.pkl`) ya documentados como intencionalmente distintos en `CLAUDE.md` §"Scalers and Normalization". No fusionar — son etapas distintas, no duplicados |
| `notebooks/` | 61 (+ subcarpetas) | 37 ipynb, 7 py, 7 png, 5 csv, 3 joblib | Activa — core del proyecto (`fase2/`, `fase3/`, `fase4/`) | Mantener como raíz de fases; internamente tiene 2 problemas nuevos, ver §5.3 |
| `pipeline/` | 139 | 96 png, 28 ipynb, 8 csv | Activa — Fase 1 ETL, invocada por `run_pipeline.py` (10 rutas hardcodeadas) | Mantener en raíz por ahora (riesgo medio de mover, ver §2.2 del diagnóstico original — sin cambios en esta auditoría) |
| `resultados/` | 112 | 45 png, 24 json, 19 csv, 9 keras, 6 pkl | Activa — 18 subcarpetas, backbone de datos del dashboard | Mantener; ver tríadas `base/_ext/_final` en §5.1 y §5.3 |
| `scratch/` | 5 | 4 py, 1 txt | **No referenciada por ningún otro archivo** (`check_nlp_results.py`, `debug_cols.py`, `execute_nb.py`, `repro_act06.py`, `img_b64.txt`) — son scripts de debug personal | Residual. `.gitignore` línea 132 ya tiene `scratch/`, pero los 5 archivos se commitearon **antes** de esa regla y `git` los sigue rastreando (`git check-ignore` no los detecta como ignorados porque un tracked file ignora la regla). Candidato a `git rm -r --cached scratch/` si se confirma que ya no se usan, o simplemente dejar tal cual — no estorba a nada activo |
| `sources/` | 17 | 12 csv, 4 txt, 1 json | Mayormente activa (`agraria-pe/`, `nasa/nasapower*`), pero **contiene un duplicado exacto** — ver hallazgo nuevo en §5.3 | Mantener carpeta; eliminar la subcarpeta duplicada `sources/nasa/proceso-data-engineering-nasa/` |
| `src/` | 41 | 41 py | Activa — código de clases (`agro/`, `weather/`, `features/`, `models/`, `scraping/`, `data_processing/`) | Mantener, sin cambios — es la única carpeta ya organizada según la regla de `CLAUDE.md` ("todo código nuevo va en `src/`") |

Carpetas raíz que **no** son directorios (archivos sueltos relevantes, ya cubiertos por el diagnóstico original salvo lo nuevo):
- `20%` (0 bytes) — eliminar, ver §1.1.
- `gen_nb_*.py`, `exec_nb_*.py`, `generate_notebooks.py`, `setup_project_structure.py` — generadores de un solo uso, código muerto o ya ejecutado, ver §2.2 y §3.2.
- `generar_*.py` (6 scripts) y `visualizar_shocks_comparativo.py` — **confirmado en esta auditoría que SÍ están activos**: todos escriben directamente a `dashboard/fig_*.png` y leen de `resultados/xgboost/`, `resultados/gm_v3/`, etc. usando rutas relativas asumiendo que se ejecutan desde la raíz del repo. Esto **actualiza el veredicto "⚠️ Verificar primero"** del diagnóstico original (§3.2) a: confirmados como vivos, pero **si se mueven de la raíz hay que ajustar sus rutas relativas** (`'dashboard/fig_...'`, `'resultados/xgboost/...'`), igual que con `run_pipeline.py`.

### 5.3 Sinergias y duplicados detectados (hallazgos nuevos)

**a) `notebooks/fase4/resultados/` — carpeta huérfana por el mismo bug de ruta relativa que en §1.2**

```
notebooks/fase4/resultados/impacto_economico_nino_2026.png
notebooks/fase4/resultados/impacto_nino_desglose.png
notebooks/fase4/resultados/impacto_nino_pred_vs_real.png
```

`notebooks/fase4/impacto_economico_nino_2026.ipynb` (y su versión `_ejecutado`) guarda las figuras con la ruta relativa `resultados/impacto_*.png`. Como el notebook se ejecuta con cwd = `notebooks/fase4/`, en vez de caer en la carpeta raíz `resultados/` crea una carpeta `resultados/` **anidada dentro de `notebooks/fase4/`**. Verificado por grep: ningún otro script o notebook lee `notebooks/fase4/resultados/`, por lo que no rompe nada activo.

**Propuesta:** mover esas 3 imágenes a `resultados/nino_2026/` (o similar) en la raíz, y corregir la ruta de guardado en el notebook a `../../resultados/nino_2026/...` para que no vuelva a ocurrir. Carpeta destino sugerida: `resultados/`.

**b) `sources/nasa/proceso-data-engineering-nasa/` — duplicado byte a byte de `data/02_interim_nasa/`**

```
$ diff -q data/02_interim_nasa/nasa_long_clean.csv        sources/nasa/proceso-data-engineering-nasa/nasa_long_clean.csv
$ diff -q data/02_interim_nasa/nasa_long_raw.csv           sources/nasa/proceso-data-engineering-nasa/nasa_long_raw.csv
$ diff -q data/02_interim_nasa/nasa_mensual_integrado.csv  sources/nasa/proceso-data-engineering-nasa/nasa_mensual_integrado.csv
$ diff -q data/02_interim_nasa/nasa_pipeline_config.json   sources/nasa/proceso-data-engineering-nasa/nasa_pipeline_config.json
(sin diferencias en los 4 archivos comparados)
```

Los 4 archivos comparados son **idénticos**. `data/02_interim_nasa/nasa_pipeline_config.json` está referenciado activamente por `main_nasa_pipeline.py` y los 10 scripts de `src/data_processing/nasa_pipeline/actividad_0{1..10}_*_nasa.py`. **`sources/nasa/proceso-data-engineering-nasa/` no está referenciada por ningún script ni notebook** — es una copia manual olvidada (probablemente de cuando se guardó una copia de seguridad de un output intermedio dentro de `sources/`, que semánticamente debería contener solo datos crudos de entrada, no salidas del pipeline).

**Propuesta:** eliminar `sources/nasa/proceso-data-engineering-nasa/` y conservar `data/02_interim_nasa/` como única copia (es la canónica y la que usa el pipeline vivo).

**c) Backups de `dashboard/app*.py` duplicados también en `docs/`**

`dashboard/app_backup.py`, `app_backup_.py`, `app_backup_20260601_142202.py`, `app_backup_dash_migration.py` existen **también** en `docs/` (copia manual, ver §2.4). Son 8 archivos (4+4) sin ningún import ni referencia activa. Ya señalado como candidato de limpieza en el diagnóstico original (§3.3); esta auditoría confirma que ninguno de los 8 es importado por `app.py`, `app_dash.py` ni por notebooks.

**Propuesta:** eliminar los 4 de `dashboard/` y, si se decide sincronizar `docs/` en algún momento, sus 4 copias también — pero esto último requiere tocar `docs/`, que el usuario pidió no tocar por ahora.

**d) Tríadas `base / _ext / _final` en `resultados/` — no son duplicados, son 3 pipelines paralelos**

```
resultados/ge/        resultados/ge_ext/        resultados/ge_final/
resultados/gm_v3/      resultados/gm_v3_ext/      resultados/gm_v3_final/
resultados/xgboost/    resultados/xgboost_ext/    resultados/xgboost_final/
```

Mismo patrón para 3 familias de modelos (GE, GM_v3, XGBoost) — confirma que es una convención deliberada del reentrenamiento (44 → 68 muestras → versión final), no una duplicación accidental. `resultados/gm/` y `resultados/gm_v2/` son familias distintas (no tienen `_ext`/`_final`), igual que `resultados/gc1/`, `gc2/`, `tcn/`, `gm_v4/`, `shap/`, `validacion/` que son de un solo estadio.

**No hay sinergia real que fusionar aquí** — están todas ya bajo el mismo padre `resultados/`, que es justamente la estructura objetivo que propone §3. Lo único accionable es la carpeta huérfana del punto (a).

**e) Figuras dispersas en 6 ubicaciones distintas — NO centralizar, cada una tiene un rol distinto**

| Ubicación | # PNG | Origen | ¿Centralizar en `dashboard/`? |
|---|---|---|---|
| `pipeline/output/{03_eda,04_calidad,07_dwh,10_reexploracion}/` | 96 | Salida de las 10 actividades ETL de Fase 1 | No — son output de auditoría/EDA del pipeline, consumidas solo como evidencia dentro de los propios notebooks de `pipeline/` |
| `resultados/**/*.png` | 45 | Métricas/predicciones por modelo | No — ya están en su carpeta de resultado por modelo, consistente con la tabla de `CLAUDE.md` |
| `data/{03_processed_nasa/reports,04_reports,processed}/` | 42 | Reportes de calidad/EDA de datos | No — igual que `pipeline/output`, son evidencia del propio proceso de datos |
| `notebooks/fase2/output/01_nlp_sentimiento/` | 4 | Gráficas de sentimiento NLP | No |
| `notebooks/fase4/resultados/` | 3 | Ver punto (a) — huérfanas | **Sí**, mover a `resultados/nino_2026/` |
| `dashboard/` (+ espejo en `docs/`) | 8 | Figuras ya compuestas para el dashboard, generadas por los `generar_*.py` de la raíz | Ya están centralizadas — es el destino final |

La única centralización pendiente real es la del punto (a); el resto de las figuras están donde deben estar dado que cada fase produce su propia evidencia visual junto a su código, y moverlas rompería las rutas relativas de los notebooks que las generan (mismo riesgo que `pipeline/` en §2.2).

**f) Notebooks de la misma fase — ya están agrupados correctamente**

`notebooks/fase2/`, `notebooks/fase3/`, `notebooks/fase4/` ya contienen únicamente notebooks de su propia fase; no se encontraron notebooks de Fase 2/3/4 sueltos en la raíz ni en otras carpetas. La única fase sin agrupar bajo `notebooks/` es **Fase 1**, que vive en `pipeline/` (28 ipynb) — ya cubierto por la propuesta de mover `pipeline/` → `notebooks/fase1/` en §2.2 y §3.2, sin cambios adicionales de esta auditoría.

### 5.4 Resumen de veredictos accionables (nuevos, no incluidos en el diagnóstico original)

| Acción | Ítem | Riesgo | Rompe algo activo |
|---|---|---|---|
| Eliminar | `sources/nasa/proceso-data-engineering-nasa/` (duplicado exacto) | Bajo | No — sin referencias |
| Mover | `notebooks/fase4/resultados/*.png` → `resultados/nino_2026/` | Bajo | No — sin referencias, pero corregir la ruta de guardado en el notebook para que no se regenere la carpeta huérfana |
| Eliminar | 4 backups de `dashboard/app_backup*.py` (y opcionalmente sus copias en `docs/`) | Bajo | No — sin imports |
| Confirmar y no mover sin ajustar rutas | `generar_*.py` + `visualizar_shocks_comparativo.py` (raíz) | Medio si se mueven sin actualizar paths | Si se mueven, rompen sus lecturas de `resultados/...` y escrituras a `dashboard/...` (rutas relativas a la raíz) |
| No tocar | Las 3 tríadas `base/_ext/_final` en `resultados/` (incl. las 3 de XGBoost) | — | Es la convención documentada en `CLAUDE.md`, no duplicación |
| Considerar (opcional, bajo prioridad) | `git rm -r --cached scratch/` | Bajo | No — nada la referencia; ya está en `.gitignore` para archivos futuros |

No se ha ejecutado ningún cambio de esta sección. Quedo a la espera de qué partes autorizas antes de tocar cualquier carpeta.
