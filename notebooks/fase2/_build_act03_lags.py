
import nbformat as nbf
import os


def create_notebook():
    nb = nbf.v4.new_notebook()
    nb.metadata['kernelspec'] = {
        'display_name': 'Python 3',
        'language': 'python',
        'name': 'python3'
    }

    cells = []

    # ── Markdown: Título ────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "# ⏪ Actividad 03: Generación de Rezagos Temporales (Lags)\n"
        "---\n"
        "**Módulo 2: Feature Engineering para LSTM**\n\n"
        "Las redes LSTM aprenden patrones en secuencias, pero necesitan que esas "
        "secuencias estén *explícitamente* representadas como columnas de entrada. "
        "Al agregar rezagos (`t-1`, `t-3`, `t-6`, `t-12`) convertimos nuestro "
        "dataset en una **ventana deslizante** que le muestra al modelo qué ocurrió "
        "1, 3, 6 y 12 meses atrás para cada variable clave."
    ))

    # ── Markdown: Objetivos ─────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "## 🎯 Objetivos\n"
        "1. **Ordenar** el dataset cronológicamente por provincia/departamento.\n"
        "2. **Generar rezagos** (`t-1`, `t-3`, `t-6`, `t-12`) para variables "
        "objetivo y variables exógenas (clima NASA y sentimiento NLP), "
        "**agrupando por provincia** para no mezclar series distintas.\n"
        "3. **Eliminar NaNs** introducidos por el rezago en las primeras filas "
        "de cada provincia (estrategia `dropna`).\n"
        "4. **Exportar** el dataset enriquecido con lags."
    ))

    # ── Code: Setup ─────────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_code_cell(
        """import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# Configuración estética
sns.set_theme(style='whitegrid', palette='viridis')
%matplotlib inline

# ── Rutas ──────────────────────────────────────────────────────────────────────
# Fuente: dataset_integrado.csv (Fase 1) + nlp_sentiment + codificación cíclica
INPUT_FILE  = '../../data/processed/dataset_fase2_base_completo.csv'
OUTPUT_FILE = '../../data/processed/master_dataset_fase2_lags.csv'

print(f"Cargando dataset desde: {INPUT_FILE}")
"""
    ))

    # ── Markdown: Paso 1 ────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "## 1. Carga y Ordenamiento del Dataset\n"
        "Cargamos el dataset de la Actividad 02 y lo ordenamos por provincia y "
        "fecha. **Este orden es crítico**: el `shift()` de Pandas tomará la "
        "posición relativa de las filas, por lo que si no ordenamos primero, "
        "los rezagos serán incorrectos."
    ))

    # ── Code: Carga ─────────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_code_cell(
        """df = pd.read_csv(INPUT_FILE)
# fecha_evento viene como 'YYYY-MM', convertir a datetime primer día del mes
df['fecha_evento'] = pd.to_datetime(df['fecha_evento'].astype(str) + '-01')

print(f"Dataset cargado: {df.shape[0]:,} filas, {df.shape[1]} columnas")
print(f"Rango temporal: {df['fecha_evento'].min().date()} → {df['fecha_evento'].max().date()}")
print(f"Columnas disponibles:\\n{df.columns.tolist()}")
"""
    ))

    cells.append(nbf.v4.new_code_cell(
        """# Columna de agrupación: se usa 'provincia' si existe, si no 'departamento'
GROUP_COL = 'provincia' if 'provincia' in df.columns else 'departamento'
print(f"Columna de agrupación: '{GROUP_COL}'")
print(f"Grupos únicos: {df[GROUP_COL].nunique()}")

# Ordenamos cronológicamente dentro de cada grupo
df = df.sort_values([GROUP_COL, 'fecha_evento']).reset_index(drop=True)
df.head(3)
"""
    ))

    # ── Markdown: Paso 2 ────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "## 2. Definición de Variables Objetivo y Exógenas\n\n"
        "| Grupo | Variables | Justificación |\n"
        "|---|---|---|\n"
        "| **Objetivo** | `produccion_t`, `cosecha_ha`, `precio_chacra_kg` | Variables objetivo de producción agrícola |\n"
        "| **Climáticas NASA** | `T2M`, `PRECTOTCORR`, `RH2M`, `WS2M`, `ALLSKY_SFC_SW_DWN` | Drivers climáticos NASA POWER |\n"
        "| **NLP** | `nlp_sentiment` | Polaridad de noticias agroindustriales (BETO) |\n\n"
        "Los rezagos son: **t-1** (mes anterior), **t-3** (trimestre), "
        "**t-6** (semestre), **t-12** (año anterior — el predictor más potente "
        "para ciclos biológicos anuales según [54])."
    ))

    # ── Code: Definición de columnas ────────────────────────────────────────────
    cells.append(nbf.v4.new_code_cell(
        """# ── Variables para las que se generarán rezagos ──────────────────────────────
TARGET_COLS = [c for c in ['produccion_t', 'cosecha_ha', 'precio_chacra_kg'] if c in df.columns]

NASA_COLS = [c for c in [
    'T2M', 'PRECTOTCORR', 'RH2M', 'WS2M', 'ALLSKY_SFC_SW_DWN'
] if c in df.columns]

NLP_COLS = [c for c in ['nlp_sentiment'] if c in df.columns]

LAG_COLS  = TARGET_COLS + NASA_COLS + NLP_COLS
LAG_STEPS = [1, 3, 6, 12]

print(f"Columnas objetivo  : {TARGET_COLS}")
print(f"Columnas NASA      : {NASA_COLS}")
print(f"Columnas NLP       : {NLP_COLS}")
print(f"Total columnas con lag: {len(LAG_COLS)}")
print(f"Rezagos a generar  : t-{LAG_STEPS}")
print(f"Nuevas columnas a crear: {len(LAG_COLS) * len(LAG_STEPS)}")
"""
    ))

    # ── Markdown: Paso 3 ────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "## 3. Generación de Rezagos por Grupo (Provincia/Departamento)\n\n"
        "Usamos `groupby(...)[col].shift(n)` para que el rezago de la primera "
        "fila de una provincia **nunca** tome el valor de la última fila de "
        "la provincia anterior. Este es el punto más crítico del proceso."
    ))

    # ── Code: Generación de lags ────────────────────────────────────────────────
    cells.append(nbf.v4.new_code_cell(
        """lag_frames = []

for col in LAG_COLS:
    for lag in LAG_STEPS:
        new_col = f"{col}_lag{lag}"
        df[new_col] = df.groupby(GROUP_COL)[col].shift(lag)
        lag_frames.append(new_col)

print(f"✅ Rezagos generados: {len(lag_frames)} nuevas columnas")
print(f"Shape con rezagos  : {df.shape}")

# Vista de NaNs por columna lag (esperado: filas iniciales de cada grupo)
nan_summary = df[lag_frames].isna().sum()
print(f"\\nNaNs por columna lag (primeras 10):\\n{nan_summary.head(10)}")
"""
    ))

    # ── Markdown: Paso 4 ────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "## 4. Limpieza: Eliminación de Filas con NaN por Rezago\n\n"
        "Las primeras `max_lag` filas de cada grupo (12 en este caso) "
        "no tienen valores históricos disponibles. **Las eliminamos** con "
        "`dropna()` ya que las LSTM no pueden procesar valores faltantes y "
        "la imputación artificial distorsionaría la secuencia temporal.\n\n"
        "> ℹ️ Se perderán hasta 12 filas por provincia/departamento. "
        "Esto es esperado y aceptable."
    ))

    # ── Code: Limpieza de NaNs ──────────────────────────────────────────────────
    cells.append(nbf.v4.new_code_cell(
        """filas_antes = len(df)

# Eliminamos sólo filas donde alguna columna LAG tenga NaN
# (garantiza que TODAS las ventanas de historia estén completas)
df_clean = df.dropna(subset=lag_frames).reset_index(drop=True)

filas_eliminadas = filas_antes - len(df_clean)
print(f"Filas antes  : {filas_antes:,}")
print(f"Filas después: {len(df_clean):,}")
print(f"Filas eliminadas (NaNs de rezago): {filas_eliminadas:,}")
print(f"Porcentaje retenido: {len(df_clean)/filas_antes*100:.1f}%")

# Verificación final: no deben quedar NaNs en columnas lag
assert df_clean[lag_frames].isna().sum().sum() == 0, "¡Aún hay NaNs en columnas lag!"
print("\\n✅ Dataset limpio. Sin NaNs en columnas de rezago.")
"""
    ))

    # ── Markdown: Paso 5 ────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "## 5. Análisis Exploratorio de los Rezagos\n\n"
        "Visualizamos la autocorrelación de la variable objetivo principal "
        "(`produccion_t`) para validar que los rezagos elegidos (t-1, t-3, "
        "t-6, t-12) capturan la dependencia temporal real del dataset."
    ))

    # ── Code: Visualización ─────────────────────────────────────────────────────
    cells.append(nbf.v4.new_code_cell(
        """if 'produccion_t' in df_clean.columns:
    from pandas.plotting import autocorrelation_plot

    fig, axes = plt.subplots(1, 2, figsize=(16, 5))

    # Gráfico 1: Scatter lag-1 vs t
    axes[0].scatter(
        df_clean['produccion_t_lag1'],
        df_clean['produccion_t'],
        alpha=0.3, s=10, color='steelblue'
    )
    axes[0].set_title('Producción t vs Producción t-1', fontsize=12)
    axes[0].set_xlabel('produccion_t_lag1')
    axes[0].set_ylabel('produccion_t')

    # Gráfico 2: Scatter lag-12 vs t
    axes[1].scatter(
        df_clean['produccion_t_lag12'],
        df_clean['produccion_t'],
        alpha=0.3, s=10, color='darkorange'
    )
    axes[1].set_title('Producción t vs Producción t-12 (Ciclo Anual)', fontsize=12)
    axes[1].set_xlabel('produccion_t_lag12')
    axes[1].set_ylabel('produccion_t')

    plt.suptitle('Análisis de Autocorrelación de Rezagos', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('../../data/processed/visualizacion_lags.png', dpi=150, bbox_inches='tight')
    plt.show()
    print("Gráfico guardado en data/processed/visualizacion_lags.png")
else:
    print("Columna 'produccion_t' no encontrada. Verifica el nombre en tu dataset.")
"""
    ))

    # ── Markdown: Paso 6 ────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_markdown_cell(
        "## 6. Exportar Dataset con Rezagos\n"
        "Exportamos el dataset final con todas las columnas de rezago. "
        "Este archivo será la entrada de la **Actividad 04: Normalización y Escalado**."
    ))

    # ── Code: Exportar ──────────────────────────────────────────────────────────
    cells.append(nbf.v4.new_code_cell(
        """df_clean.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Dataset exportado exitosamente a: {OUTPUT_FILE}")
print(f"   Shape final: {df_clean.shape[0]:,} filas × {df_clean.shape[1]} columnas")
print(f"\\nResumen de columnas generadas:")
print(f"  - Originales      : {df.shape[1] - len(lag_frames)}")
print(f"  - Nuevas (lag)    : {len(lag_frames)}")
print(f"  - Total final     : {df_clean.shape[1]}")
df_clean.head(3)
"""
    ))

    # ── Ensamblar y guardar ─────────────────────────────────────────────────────
    nb.cells = cells

    # Se ejecuta desde notebooks/fase2/, así que guardamos en el mismo directorio
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, 'actividad_03_rezagos_temporales.ipynb')
    with open(output_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)

    print(f"[OK] Notebook generado: {output_path}")


if __name__ == "__main__":
    create_notebook()
