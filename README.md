# Modelo Híbrido Dual-LSTM + Bahdanau Attention para Pronóstico Agroindustrial (Agro + NLP + Clima)

Sistema multimodal de pronóstico de demanda/precio agroindustrial para el Perú (cultivo objetivo: **limón/lima**). Integra cuatro fuentes heterogéneas —**MIDAGRI** (estadística de cultivos), **NASA POWER** (clima), **INDECI** (desastres) y **Agraria.pe** (noticias)— mediante un pipeline ETL de 10 actividades hacia un **Data Warehouse en Star Schema**, y aplica **NLP (BETO)** + **LSTM-Attention** con explicabilidad **SHAP**.

---

## Reproducibilidad

### Requisitos

- **Python 3.11 (estricto)** — TensorFlow no es compatible con 3.13+.
  - Windows: [Python 3.11.9 (64-bit)](https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe) — marcar **"Add python.exe to PATH"** en el instalador.
  - Linux/WSL2: `sudo apt install python3.11 python3.11-venv`.
- GPU NVIDIA **opcional** (acelera el entrenamiento LSTM; el proyecto corre en CPU).
- PostgreSQL **opcional** — solo necesario para reconstruir el DWH físico (Fase 1, actividad 08). El resto del pipeline funciona con los CSV procesados.
- Todas las librerías están en [`requirements.txt`](requirements.txt).

### Instalación

```bash
# 1. Clonar
git clone https://github.com/YeralD21/Machine-Learning-Multimodal--Agro-NLP-Clima-.git
cd Machine-Learning-Multimodal--Agro-NLP-Clima-

# 2. Entorno virtual con Python 3.11
python -m venv venv
#   Windows (PowerShell):
.\venv\Scripts\Activate.ps1
#   Linux / WSL2 / macOS:
source venv/bin/activate

# 3. Dependencias
pip install --upgrade pip
pip install -r requirements.txt

# 4. (Reproducibilidad exacta) Congela TUS versiones para terceros
pip freeze > requirements.lock.txt

# 5. Demo rápida de integración (genera mock data si faltan los crudos)
python main.py
```

### Datos

| Fuente | Acceso | Cómo obtenerla |
|---|---|---|
| **NASA POWER** (clima) | 🟢 **Automático** vía API | Se descarga por API REST en el pipeline NASA (`python main_nasa_pipeline.py`). No requiere intervención manual. |
| **Agraria.pe** (noticias) | 🟡 **Semi-automático** (scraping) | Se obtiene con `src/scraping/news_scraper.py` (respeta `robots.txt` y aplica delays). Sujeto a la estructura HTML vigente del sitio (ver *Limitaciones*). |
| **MIDAGRI / Sisagri** (cultivos) | 🔴 **Descarga manual** | Descargar el Excel oficial desde el portal de [SISAGRI / MIDAGRI](https://www.gob.pe/midagri) y colocarlo en `sources/midagri/`. Archivo >100 MB, **gitignored**. |
| **INDECI / SINPAD** (desastres) | 🔴 **Descarga manual** | Descargar desde [SINPAD – INDECI](https://www.indeci.gob.pe/) y colocar en `pipeline/fuentes/indeci/`. |

> Para una primera ejecución sin los crudos pesados, el repo incluye datos de muestra en `data/` y los **datasets procesados** ya generados (`data/processed/`), de modo que las Fases 2–4 son reproducibles sin descargar MIDAGRI.

### Pipeline completo (orden de ejecución)

Ejecuta automáticamente todo con el orquestador (usa papermill):

```bash
python run_pipeline.py            # todas las fases en orden
python run_pipeline.py --fase 2   # solo una fase
python run_pipeline.py --dry-run  # ver el orden sin ejecutar
```

Orden manual (notebooks, ejecutar secuencialmente — "Run All"):

1. **Fase 1 — ETL / Data Engineering** → `pipeline/actividad_01_*.ipynb` … `actividad_10_*.ipynb`
   *(configuración → lectura 4 fuentes → EDA → calidad → limpieza → integración DWH → Star Schema → PostgreSQL → ETL → re-exploración)*
2. **Fase 2 — Features + NLP** → `notebooks/fase2/actividad_01…04`
   *(sentimiento BETO → encoding temporal cíclico sin/cos → lags t-1/t-3/t-6 → normalización). Salida: `data/processed/master_dataset_fase2_multivariado.csv`*
3. **Fase 3 — Modelos clásicos (baselines)** → `notebooks/fase3/actividad_11…14`
   *(GC1: SARIMA, Prophet · GC2: SARIMAX+LSTM · GE: Dual-LSTM + Bahdanau Attention)*
4. **Fase 4 — Multimodal, competidores, XAI y simulación** → `notebooks/fase4/actividad_15…17` + escenarios
   *(GM/GM_v2/GM_v3 con NLP · XGBoost y TCN competidores · SHAP · reentrenamiento extendido 2019-2025 · escenario El Niño 2026 · impacto económico)*

### Resultados esperados

Métricas de referencia en el **conjunto de test** (`n_train≈44`, `n_test=12`), tomadas de `resultados/ranking_final_44vs68.json` y la comparativa del proyecto. Quien reproduzca con la misma partición y `random_state=42` debería obtener valores equivalentes:

| Modelo | Arquitectura | MAE (test) | Notas |
|---|---|---|---|
| Naive | Baseline (persistencia) | **0.0161** | Alta autocorrelación en periodos sin shock |
| XGBoost | 29 features (exóg+lags+rolling) | **0.0471** | Mejor MAE absoluto; dominado por lags, baja sensibilidad climática |
| **GM_v3** ⭐ | LSTM+Attention+PCA+NLP (corpus 600 noticias) | **0.0645** | Modelo recomendado: captura dependencia temporal + shocks vía NLP+clima |
| GM_v2 | LSTM+Attention+NLP (528 noticias) | 0.0646 | NLP original |
| GE | Dual-LSTM sin NLP (23 features) | 0.0673 | Estructural, acceso directo al clima en Canal B |

**Sensibilidad climática** (de la simulación El Niño): GM_v3 ≈ 22 %, GE ≈ 11 %, XGBoost ≈ 5 %.

> ⭐ **GM_v3 es el modelo recomendado del sistema**: aunque XGBoost lidera en MAE puntual, GM_v3 equilibra precisión con sensibilidad a shocks externos (clima + noticias), lo que importa para escenarios de El Niño.

### Dashboard

```bash
# Opción A — App interactiva Streamlit (local)
streamlit run dashboard/app.py

# Opción B — Versión estática (HTML) publicada en GitHub Pages
# Fuente: rama main, carpeta /docs
```

URL pública (GitHub Pages): `https://yerald21.github.io/Machine-Learning-Multimodal--Agro-NLP-Clima-/`

---

## Estructura del código

```
sources/          CSV/Excel crudos (MIDAGRI, NASA, INDECI, Agraria) — pesados, gitignored
pipeline/         Fase 1: notebooks ETL actividad_01..10 + DWH
notebooks/fase2/  Fase 2: NLP (BETO), encoding cíclico, lags, escalado
notebooks/fase3/  Fase 3: modelos clásicos (SARIMA, Prophet, SARIMAX+LSTM, GE)
notebooks/fase4/  Fase 4: multimodal (GM*), competidores, SHAP, simulaciones
src/              Lógica núcleo orientada a clases (ver tabla)
data/processed/   Datasets maestros generados
resultados/       Modelos entrenados, métricas, predicciones por modelo
database/         DDL PostgreSQL del Star Schema
dashboard/ docs/  Front-end (Streamlit + HTML estático para Pages)
```

| Módulo | Clase | Responsabilidad |
|---|---|---|
| `src/agro/processor.py` | `AgroProcessor` | Cargar MIDAGRI, filtrar cultivo, remover outliers (IQR), validar ≥24 meses |
| `src/weather/processor.py` | `WeatherProcessor` | Cargar NASA/SENAMHI, estandarizar distritos, imputar faltantes |
| `src/features/builder.py` | `FeatureBuilder` | Merge por ANHO+MES+UBIGEO, lags y estacionalidad cíclica |
| `src/models/lstm_attention.py` | `LSTMDemandForecaster` | Construir/entrenar/predecir LSTM+Attention |
| `src/scraping/news_scraper.py` | `NewsScraper` | Scraping ético de Agraria.pe (robots.txt + delays) |

> ⚠️ **Regla del proyecto:** toda lógica núcleo nueva debe ser **basada en clases** y ubicarse en `src/`. Los notebooks son sandboxes.

---

## Limitaciones de reproducibilidad

- **Scraping de Agraria.pe**: depende de la estructura HTML vigente del sitio al momento de ejecución. Si el sitio cambia su maquetado, `news_scraper.py` puede requerir ajuste de selectores. El corpus de noticias usado en los experimentos está versionado para permitir reproducir los resultados sin re-scrapear.
- **MIDAGRI**: requiere descarga manual del Excel oficial (>100 MB, no versionable en git). Sin él no se puede regenerar la Fase 1 desde cero, pero los datasets procesados sí están incluidos.
- **Versiones de librerías**: `requirements.txt` no fija versiones (el entorno se desarrolló en Windows + WSL2). Para reproducción exacta, genera y comparte `requirements.lock.txt` con `pip freeze`.
- **Artefactos no guardados**: el modelo XGBoost y los scalers/PCA de GM_v3 no se serializaron a disco; se reconstruyen de forma determinista re-entrenando con la misma partición y `random_state=42` (ver `CLAUDE.md`).
- **GPU/CPU**: pequeñas diferencias numéricas (orden de reducción en GPU) pueden producir variaciones marginales en la última cifra del MAE respecto a las tablas.
