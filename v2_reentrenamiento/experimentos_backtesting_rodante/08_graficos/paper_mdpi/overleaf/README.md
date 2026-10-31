# Paquete LaTeX para Overleaf (artículo MDPI)

Generado por `02_scripts/12_paquete_overleaf.py`. No calcula ninguna cifra: copia figuras, pies y tablas ya guardados, y escribe las tablas del texto principal leyendo los CSV de `06_metricas/` (solo elige columnas y da formato). **Estado:** paquete aprobado por el autor; los ajustes del 2026-10-29 están pendientes de aprobación (DESVIACIONES.md, DESV-08, puntos 28 y 29). fig_workflow y la generación del archivo único y del zip: puntos 30 y 31 (aprobados).

## Contenido

| Archivo | Qué es |
|---|---|
| `figures/` | 56 PDF vectoriales, con el nombre original de cada figura; además `fig_workflow.png` para el main.tex antiguo (no se usa en el paquete) |
| `resultados_v2_completo.tex` | todo el paquete en un solo archivo, para pegar en el cuerpo del main.tex: macros, tablas y figuras, sin `\input` ni `\usepackage` |
| `figures_overleaf.zip` | solo los 56 PDF, dentro de `figures/` |
| `lista_figuras.md` | figura → PDF → `\label` → pie en una línea |
| `tables/` | 3 tablas del texto principal (`TM1`–`TM3`, generadas desde los CSV) y 10 suplementarias (copias) |
| `captions.tex` | `\midagridate` (fecha de consulta de MIDAGRI–SISAGRI) y un `\newcommand` por figura con su pie en inglés, tomado del `.txt` de la figura sin referencias internas; `\figcap{<nombre de archivo>}` lo imprime |
| `figures_main.tex`, `figures_supp.tex` | un entorno `figure` por figura, en orden de aparición |
| `tables_main.tex`, `tables_supp.tex` | las tablas con `\input`, en orden de aparición |
| `test_article.tex` | documento mínimo de prueba (clase `article`, no la plantilla MDPI) que compila todo el paquete, tablas incluidas |

## Orden de aparición

**Texto principal: figuras**

1. `fig29_study_area_map` (figura única)
2. `fig24_series_with_shocks`
3. `fig_workflow` (figura única; diagrama del flujo de trabajo, `13_figura_workflow.py`)
4. `fig30_rolling_origin_scheme` (figura única)
5. `fig25_architecture_dual_lstm` (figura única)
6. `fig13_mae_normal_vs_shock_bars`
7. `fig07_rank_heatmap_model_percentile`
8. `fig05_mae_shock_vs_percentile`
9. `fig04_delta_s_vs_percentile`
10. `fig10_n_shock_heatmap_window_percentile`
11. `fig20_real_vs_pred_2025`
12. `fig18_nlp_ablation_paired`
13. `fig02_train_val_gap`

**Texto principal: tablas** (generadas desde los CSV)

| Tabla | Fuente | Columnas elegidas |
|---|---|---|
| `TM1_descriptive_series` (`tab:main_descriptive_series`) | T03 (`T03_descripcion_particiones.csv`, partición *test*) y T04 (`T04_acf_adf_kpss_train.csv`) | Para cada ventana 2021–2025: año de test; meses de train; media, DE y CV del año de test; ACF(1) y ACF(12), ADF *p* y KPSS *p* en nivel, del train. KPSS *p* acotado por la tabla del test se muestra como "> 0.10". |
| `TM2_aggregate_accuracy_P75` (`tab:main_aggregate_accuracy`) | T06 (`T06_agregadas_IC.csv`: principal 2021–2025, mensual, P75, `exploratorio = False`) | 10 modelos; MAE global y MAE en meses de shock, cada uno con IC 95 % bootstrap iid (el principal); Δs. El n de meses (60) y de meses de shock (10 Sutil, 15 Dulce) va en el título de cada bloque. **Sin IC para Δs** (ver "Pendiente"). |
| `TM3_robust_ranking` (`tab:main_robust_ranking`) | T08 (`T08_ranking_shock_principal_2021_2025_{c}.csv`) | Posición por MAE en meses de shock en P50, P60, P75 y P90 (MoM; `*` = primero o empatado); veces primero o empatado (MoM, de 4); robusto MoM (E-BR1: ≥ 3 de 4); veces primero o empatado y robusto YoY. |

Formato: toneladas sin decimales en Sutil y con uno en Dulce; Δs con un decimal; *p* con tres.

**Suplementario: figuras**

1. `fig20`, año con más meses de shock P75: Sutil 2023 y Dulce 2024
2. `fig19_dm_pvalue_matrix`
3. `fig19b_dm_pvalue_matrix_holm`, **EXPLORATORY** (marca añadida al inicio del `\caption`)
4. `fig18b_xgb43_vs_xgb37_exploratory`, **EXPLORATORY** (el pie del TXT ya empieza así)
5. `fig01_loss_curves_gc3`
6. `fig01_loss_curves_ge`
7. `fig03_early_stopping_epoch`
8. `fig06_ratio_shock_normal_vs_percentile`
9. `fig08_rank_bump_chart`
10. `fig09_n_shock_vs_percentile`
11. `fig11_bootstrap_ci_mae_shock_report_points`
12. `fig12_yoy_vs_mom_threshold`
13. `fig14_mae_shock_heatmap_model_window`
14. `fig15_seed_boxplots`
15. `fig16_mae_by_change_quartile`
16. `fig17_leave_one_shock_out_delta_s`
17. `fig21_abs_error_monthly`

**Suplementario: tablas** (copias de `.tex` existentes)

1. `TS1_provinces` (apaisada)
2. `T16_exploratorio_xgb43`
3. `T05_MAE_P75_por_ventana_{sutil,dulce}`
4. `T05_MAE_shock_P75_por_ventana_{sutil,dulce}`
5. `T09_DM_matriz_principal_2021_2025_{sutil,dulce}` (sin ajustar)
6. `T11_ablacion_{sutil,dulce}`

**Subfiguras.** Cada pareja Sutil/Dulce va en un solo `figure` con dos subfiguras **sin letra**: `\captionsetup[subfigure]{labelformat=empty}`, al inicio de `figures_main.tex` y de `figures_supp.tex`. Los subtítulos son "Sour lime" y "Sweet lime"; en la fig20 suplementaria llevan además el año ("Sour lime, 2023", "Sweet lime, 2024"). Así no chocan con las letras (a), (b), (c) de los paneles internos. El `\caption` contiene los dos pies: "*Sour lime:* <pie Sutil> *Sweet lime:* <pie Dulce>". Las figuras de más de 4 pulgadas de ancho se apilan en vertical; fig09 va lado a lado.

**Etiquetas.** `fig:<nombre sin _sutil/_dulce>` para la figura; `fig:<nombre de archivo>` para cada subfigura; `tab:main_*` para las tablas principales. Las suplementarias conservan las suyas.

## Cambios respecto de los originales (los TXT y los `.tex` de `06_metricas/` no se modifican)

- **Pies sin referencias internas** (solo en `captions.tex`):
  - fig02: "(D-BR8)" → "(pre-registered)"
  - fig05: "(D-BR6)" → "(pre-registered)"
  - fig19b: "(exploratory, DESV-07; family…" → "(exploratory; family…"
  - fig25: se quita "(v2 official implementation)"
- **T16:** título y cabeceras en inglés. Las celdas no cambian (sutil/dulce, global/shock\_P75/normal\_P75).
- **TS1:** la fecha de MIDAGRI pasa a `\midagridate{}`. El "%" ya está escapado también en el original.

## Paquetes LaTeX necesarios

`graphicx`, `caption` y `subcaption` (subfiguras y `\captionsetup`), `booktabs` (`\toprule`, `\addlinespace`), `longtable` (TS1), `float` (`[H]` de las tablas copiadas), `pdflscape` (TS1 apaisada) y `fontenc` con `T1`, para que `>` se imprima bien en texto. `\textminus` y `\textbar` vienen en el núcleo de LaTeX (2020 o posterior). Cargar los paquetes antes de `\input{captions}`.

## Cómo importar a Overleaf

1. En Overleaf: **New Project → Upload Project** y elegir `overleaf_mdpi.zip`. Los archivos están en la raíz del zip.
2. Para probar: en **Menu → Main document**, elegir `test_article.tex` y compilar con pdfLaTeX.
3. Para usar la plantilla MDPI: subir el paquete al proyecto de la plantilla y, en su preámbulo, añadir `\graphicspath{{figures/}}` y `\input{captions}`. En el cuerpo: `\input{figures_main}` y `\input{tables_main}`; en el suplemento: `\input{figures_supp}` y `\input{tables_supp}`.

## Pendiente (completar o decidir antes de enviar)

- **Fecha de consulta de MIDAGRI–SISAGRI.** Se cambia una sola vez en `captions.tex`: `\newcommand{\midagridate}{[____]}`.
- **IC de Δs en TM2:** ningún CSV guarda un IC para Δs (T06 solo tiene el valor), así que la tabla lo muestra sin IC. Calcularlo sería una cifra nueva.
- **T05, T09 y T11** están en español (títulos y cabeceras), como sus `.tex` originales. **T05 incluye una fila de XGB-43.**
- **Ancho de las tablas:** TS1 (12 columnas, apaisada) y TM3 (9 columnas con cabeceras largas) no se comprobaron, porque no se compiló.

## Compilación

No hay `pdflatex` ni `latexmk` en este equipo, así que **el paquete no se compiló**. El script verificó lo siguiente:

- Cada `\includegraphics` y cada `\input` apunta a un archivo existente; no hay PDF ni tablas sin usar.
- Los nombres y las etiquetas no se repiten.
- Cada pie de `captions.tex` coincide con su TXT, salvo los reemplazos declarados.
- fig18b y fig19b llevan EXPLORATORY.
- No queda "D-BR", "DESV" ni "v2 official" en `captions.tex` ni en las tablas.
- Ninguna tabla tiene un "%" sin escapar y las llaves están balanceadas.
- Las 270 celdas numéricas de TM1–TM3 coinciden con su CSV.
