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
