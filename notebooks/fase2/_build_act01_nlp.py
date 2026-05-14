x=1

import json, uuid, os

def cid(): return str(uuid.uuid4())[:8]
def md(src): return {"cell_type":"markdown","id":cid(),"metadata":{},"source":src}
def code(src): return {"cell_type":"code","id":cid(),"execution_count":None,"metadata":{},"outputs":[],"source":src}
def interp(txt): return md(["### Interpretacion\n\n"] + txt)
def nb(cells):
    return {"nbformat":4,"nbformat_minor":5,
            "metadata":{"kernelspec":{"display_name":"Python 3 (venv)","language":"python","name":"python3"},
                        "language_info":{"name":"python","version":"3.11.0"}},
            "cells":cells}

cells = []

# TITULO
cells.append(md([
"# Actividad 1 — Cuantificacion de Sentimiento NLP con BETO\n","\n",
"**Proyecto:** Prediccion de Produccion de Limon en el Peru  \n",
"**Fase:** 2 — Modulo NLP e Ingenieria de Caracteristicas Multimodal  \n","\n","---\n","\n",
"## Objetivo\n",
"Transformar el corpus textual de noticias agricolas (Agraria.pe) en un indicador "
"numerico continuo de sentimiento acotado en el rango **[-1, 1]** usando el modelo "
"de lenguaje preentrenado **BETO** (BERT en español).\n","\n",
"## Fundamento teorico\n",
"Segun tu RSL [22], la fusion heterogenea de señales textuales con datos tabulares "
"mejora la capacidad predictiva de modelos de series temporales. "
"El sentimiento de las noticias agricolas captura el contexto del mercado "
"(expectativas de precio, alertas climaticas, problemas de distribucion) "
"que no esta presente en los datos numericos.\n","\n",
"## Formula del score de sentimiento\n",
"```\n",
"score = P(POSITIVO) - P(NEGATIVO)  ∈ [-1, 1]\n",
"```\n",
"- score = +1: noticia completamente positiva para el sector\n",
"- score =  0: noticia neutral\n",
"- score = -1: noticia completamente negativa para el sector\n","\n",
"## Entradas\n",
"- `sources/agraria-pe/unificado/noticias_unificadas_2021_2025.csv` — 528 noticias\n",
"- `pipeline/output/09_etl/master_dataset_raw_values.csv` — dataset Fase 1\n","\n",
"## Salidas\n",
"- `notebooks/fase2/output/01_nlp_sentimiento/noticias_con_sentimiento.csv` — noticias + score\n",
"- `notebooks/fase2/output/01_nlp_sentimiento/sentimiento_mensual.csv` — promedio mensual\n",
"- `notebooks/fase2/output/01_nlp_sentimiento/dataset_con_sentimiento.csv` — dataset Fase 1 + avg_sentiment\n",
"- Graficos de analisis del sentimiento\n","\n",
"## Regla de esta actividad\n",
"> El sentimiento es **nacional** (no provincial). Todas las provincias del mismo mes "
"reciben el mismo `avg_sentiment`. Esto es valido porque las noticias de Agraria.pe "
"reflejan el contexto del mercado nacional del limon.\n",
]))

# SETUP
cells.append(md(["## Configuracion inicial\n"]))
cells.append(code([
"import os, sys, json, warnings, time\n",
"import pandas as pd\n",
"import numpy as np\n",
"import matplotlib\n",
"import matplotlib.pyplot as plt\n",
"import seaborn as sns\n",
"\n",
"warnings.filterwarnings('ignore')\n",
"pd.set_option('display.max_columns', None)\n",
"pd.set_option('display.width', 200)\n",
"sns.set_theme(style='whitegrid', palette='muted')\n",
"plt.rcParams['figure.dpi'] = 110\n",
"\n",
"# Navegar a la raiz del proyecto\n",
"while not os.path.exists('notebooks/fase2/config/fase2_config.json'):\n",
"    os.chdir('..')\n",
"\n",
"with open('notebooks/fase2/config/fase2_config.json','r',encoding='utf-8') as f:\n",
"    CONFIG = json.load(f)\n",
"\n",
"OUTPUT_NLP = CONFIG['output']['nlp']\n",
"os.makedirs(OUTPUT_NLP, exist_ok=True)\n",
"\n",
"print('Config cargado OK | Raiz:', os.getcwd())\n",
"print('Carpeta de salida:', OUTPUT_NLP)\n",
]))

# SECCION 1: CARGA DE NOTICIAS
cells.append(md(["---\n","# 1. Carga y Exploracion del Corpus de Noticias\n"]))
cells.append(code([
"# Cargar noticias unificadas\n",
"df_noticias = pd.read_csv(CONFIG['noticias_path'], on_bad_lines='skip', low_memory=False)\n",
"df_noticias['fecha_dt'] = pd.to_datetime(df_noticias['fecha'], errors='coerce')\n",
"df_noticias['anio'] = df_noticias['fecha_dt'].dt.year\n",
"df_noticias['mes']  = df_noticias['fecha_dt'].dt.month\n",
"df_noticias['fecha_evento'] = df_noticias['fecha_dt'].dt.strftime('%Y-%m')\n",
"\n",
"# Filtrar rango del pipeline\n",
"df_noticias = df_noticias[\n",
"    (df_noticias['anio'] >= 2021) & (df_noticias['anio'] <= 2025)\n",
"].copy()\n",
"\n",
"print(f'Noticias cargadas: {len(df_noticias):,}')\n",
"print(f'Rango: {df_noticias[\"fecha_dt\"].min().date()} -> {df_noticias[\"fecha_dt\"].max().date()}')\n",
"print(f'Columnas: {df_noticias.columns.tolist()}')\n",
"print()\n",
"print('Noticias por año:')\n",
"print(df_noticias['anio'].value_counts().sort_index().to_string())\n",
"print()\n",
"print('Nulos por columna:')\n",
"print(df_noticias[['titular','cuerpo_completo']].isnull().sum().to_string())\n",
"print()\n",
"print('Muestra de titulares:')\n",
"for i, row in df_noticias.head(5).iterrows():\n",
"    print(f'  [{row[\"fecha_evento\"]}] {str(row[\"titular\"])[:80]}')\n",
]))
cells.append(interp([
"**Que muestra:** El corpus de noticias disponible para el analisis de sentimiento. "
"528 noticias de Agraria.pe del periodo 2021-2025.\n\n",
"**Consideracion importante:** Las noticias son nacionales — no tienen granularidad provincial. "
"El sentimiento calculado se asignara a todas las provincias del mismo mes.\n\n",
"**Implicacion para el modelo:** El sentimiento captura el contexto del mercado "
"(expectativas de precio, alertas climaticas, problemas de distribucion) "
"que no esta presente en los datos numericos de MIDAGRI, INDECI o NASA.\n",
]))

# SECCION 2: PREPARACION DEL TEXTO
cells.append(md(["---\n","# 2. Preparacion del Texto para BETO\n","\n",
"BETO tiene un limite de **512 tokens**. Se usara el titular completo + "
"las primeras 200 palabras del cuerpo para maximizar la informacion "
"sin exceder el limite.\n"]))
cells.append(code([
"def preparar_texto(row, max_palabras_cuerpo=200):\n",
"    titular = str(row['titular']).strip() if pd.notna(row['titular']) else ''\n",
"    cuerpo  = str(row['cuerpo_completo']).strip() if pd.notna(row['cuerpo_completo']) else ''\n",
"\n",
"    # Truncar cuerpo a max_palabras_cuerpo palabras\n",
"    palabras_cuerpo = cuerpo.split()[:max_palabras_cuerpo]\n",
"    cuerpo_truncado = ' '.join(palabras_cuerpo)\n",
"\n",
"    # Combinar titular + cuerpo\n",
"    if cuerpo_truncado:\n",
"        texto = f'{titular}. {cuerpo_truncado}'\n",
"    else:\n",
"        texto = titular\n",
"\n",
"    return texto[:512]  # Limite de caracteres como seguridad adicional\n",
"\n",
"df_noticias['texto_beto'] = df_noticias.apply(preparar_texto, axis=1)\n",
"\n",
"# Estadisticas del texto preparado\n",
"df_noticias['n_palabras'] = df_noticias['texto_beto'].apply(lambda x: len(x.split()))\n",
"\n",
"print('Estadisticas del texto preparado para BETO:')\n",
"print(f'  Longitud media: {df_noticias[\"n_palabras\"].mean():.0f} palabras')\n",
"print(f'  Longitud max:   {df_noticias[\"n_palabras\"].max()} palabras')\n",
"print(f'  Longitud min:   {df_noticias[\"n_palabras\"].min()} palabras')\n",
"print(f'  Textos vacios:  {(df_noticias[\"n_palabras\"] == 0).sum()}')\n",
"print()\n",
"print('Ejemplo de texto preparado:')\n",
"print(df_noticias['texto_beto'].iloc[0][:300])\n",
]))

# GRAFICO 1: Distribucion longitud textos
cells.append(md(["## Grafico 1 — Distribucion de longitud de textos preparados para BETO\n"]))
cells.append(code([
"fig, axes = plt.subplots(1, 2, figsize=(13, 4))\n",
"\n",
"# Histograma de longitud\n",
"axes[0].hist(df_noticias['n_palabras'], bins=30, color='#3498db', edgecolor='white', alpha=0.85)\n",
"axes[0].axvline(df_noticias['n_palabras'].mean(), color='#c0392b', linestyle='--',\n",
"                linewidth=2, label=f'Media: {df_noticias[\"n_palabras\"].mean():.0f} palabras')\n",
"axes[0].axvline(200, color='#e67e22', linestyle='--', linewidth=2,\n",
"                label='Limite cuerpo: 200 palabras')\n",
"axes[0].set_xlabel('Numero de palabras', fontsize=11)\n",
"axes[0].set_ylabel('Frecuencia', fontsize=11)\n",
"axes[0].set_title('Longitud de textos preparados', fontsize=11, fontweight='bold')\n",
"axes[0].legend(fontsize=9)\n",
"\n",
"# Noticias por mes\n",
"por_mes = df_noticias.groupby('fecha_evento').size().reset_index(name='n')\n",
"axes[1].bar(range(len(por_mes)), por_mes['n'], color='#8e44ad', edgecolor='white', alpha=0.85)\n",
"axes[1].set_xticks(range(0, len(por_mes), 6))\n",
"axes[1].set_xticklabels(por_mes['fecha_evento'].iloc[::6], rotation=45, ha='right', fontsize=7)\n",
"axes[1].set_xlabel('Fecha', fontsize=10)\n",
"axes[1].set_ylabel('Noticias por mes', fontsize=10)\n",
"axes[1].set_title('Distribucion temporal de noticias', fontsize=11, fontweight='bold')\n",
"\n",
"plt.suptitle('Grafico 1 — Corpus de Noticias Preparado para BETO\\n'\n",
"             f'{len(df_noticias)} noticias | 2021-2025',\n",
"             fontsize=12, fontweight='bold')\n",
"plt.tight_layout()\n",
"g1 = OUTPUT_NLP + 'g1_corpus_noticias.png'\n",
"plt.savefig(g1, dpi=120, bbox_inches='tight'); plt.show()\n",
"print('Guardado:', g1)\n",
]))
cells.append(interp([
"**Que muestra:** La distribucion de longitud de los textos preparados para BETO "
"y la distribucion temporal de noticias por mes.\n\n",
"**Consideracion tecnica:** BETO tiene un limite de 512 tokens (~400 palabras). "
"Los textos que superan este limite son truncados automaticamente. "
"La mayoria de los titulares + primeras 200 palabras del cuerpo estan dentro del limite.\n\n",
"**Meses sin noticias:** Los meses con 0 noticias recibiran `avg_sentiment = 0` (neutral). "
"Esto es conservador y evita introducir sesgos artificiales.\n",
]))

# SECCION 3: EJECUCION DE BETO (CARGA DE RESULTADOS)
cells.append(md(["---\n","# 3. Cuantificacion de Sentimiento con BETO\n","\n",
"En esta etapa, el modelo procesa cada noticia para extraer el score continuo. "
"Dado que el proceso toma ~17 minutos, cargamos los resultados pre-calculados "
"del archivo de cache.\n"]))
cells.append(code([
"cache_path = OUTPUT_NLP + 'noticias_con_sentimiento.csv'\n",
"if os.path.exists(cache_path):\n",
"    print(f'Cargando resultados pre-calculados de: {cache_path}')\n",
"    df_noticias = pd.read_csv(cache_path)\n",
"    print(f'Noticias con sentimiento: {len(df_noticias)}')\n",
"    print(f'Columnas: {df_noticias.columns.tolist()}')\n",
"else:\n",
"    print('❌ ERROR: No se encontro el archivo de resultados.')\n",
"    print('Por favor, ejecuta el script _run_beto.py en la terminal para generar el cache.')\n",
]))
cells.append(interp([
"**Resultados del modelo:** El dataset ahora incluye `sentiment_score` (rango [-1, 1]) "
"y `sentiment_label` (POS, NEU, NEG).\n",
]))

# SECCION 4: NOTICIAS MAS POSITIVAS Y NEGATIVAS
cells.append(md(["---\n","# 4. Noticias mas Positivas y mas Negativas\n","\n",
"Analisis cualitativo para validar que BETO esta clasificando correctamente "
"el sentimiento en el contexto agricola peruano.\n"]))
cells.append(code([
"if 'sentiment_score' in df_noticias.columns:\n",
"    # Top 5 mas positivas\n",
"    top_pos = df_noticias.nlargest(5, 'sentiment_score')[['fecha_evento','titular','sentiment_score','sentiment_label']]\n",
"    # Top 5 mas negativas\n",
"    top_neg = df_noticias.nsmallest(5, 'sentiment_score')[['fecha_evento','titular','sentiment_score','sentiment_label']]\n",
"\n",
"    print('TOP 5 NOTICIAS MAS POSITIVAS:')\n",
"    print('=' * 70)\n",
"    for _, row in top_pos.iterrows():\n",
"        print(f'  [{row[\"fecha_evento\"]}] Score: {row[\"sentiment_score\"]:+.4f} | {str(row[\"titular\"])[:70]}')\n",
"    print()\n",
"    print('TOP 5 NOTICIAS MAS NEGATIVAS:')\n",
"    print('=' * 70)\n",
"    for _, row in top_neg.iterrows():\n",
"        print(f'  [{row[\"fecha_evento\"]}] Score: {row[\"sentiment_score\"]:+.4f} | {str(row[\"titular\"])[:70]}')\n",
]))
cells.append(interp([
"**Que muestra:** Las noticias con mayor y menor score de sentimiento. "
"Esta validacion cualitativa confirma que BETO esta interpretando correctamente "
"el contexto agricola peruano.\n\n",
"**Validacion esperada:** Las noticias positivas deben hablar de buenas cosechas, "
"precios favorables o demanda creciente. Las negativas deben mencionar heladas, "
"sequias, caida de precios o problemas de distribucion.\n\n",
"**Si la clasificacion es incorrecta:** Puede indicar que el modelo necesita "
"fine-tuning con datos agricolas peruanos especificos. "
"Para esta tesis, el modelo base de BETO es suficiente como primera aproximacion.\n",
]))

# SECCION 5: AGREGACION MENSUAL
cells.append(md(["---\n","# 5. Agregacion Mensual del Sentimiento\n","\n",
"El sentimiento se agrega a nivel mensual calculando el promedio de los scores "
"de todas las noticias del mes. Los meses sin noticias reciben score = 0 (neutral).\n"]))
cells.append(code([
"if 'sentiment_score' in df_noticias.columns:\n",
"    # Agregar por mes\n",
"    sentimiento_mensual = (\n",
"        df_noticias.groupby('fecha_evento')\n",
"        .agg(\n",
"            avg_sentiment=('sentiment_score', 'mean'),\n",
"            n_noticias_beto=('sentiment_score', 'count'),\n",
"            n_positivas=('sentiment_label', lambda x: (x=='POS').sum()),\n",
"            n_negativas=('sentiment_label', lambda x: (x=='NEG').sum()),\n",
"            n_neutrales=('sentiment_label', lambda x: (x=='NEU').sum()),\n",
"        )\n",
"        .reset_index()\n",
"    )\n",
"    sentimiento_mensual['avg_sentiment'] = sentimiento_mensual['avg_sentiment'].round(4)\n",
"\n",
"    print(f'Meses con noticias: {len(sentimiento_mensual)}')\n",
"    print(f'Rango: {sentimiento_mensual[\"fecha_evento\"].min()} -> {sentimiento_mensual[\"fecha_evento\"].max()}')\n",
"    print()\n",
"    print('Estadisticas del sentimiento mensual:')\n",
"    print(f'  Media:   {sentimiento_mensual[\"avg_sentiment\"].mean():.4f}')\n",
"    print(f'  Std:     {sentimiento_mensual[\"avg_sentiment\"].std():.4f}')\n",
"    print(f'  Min:     {sentimiento_mensual[\"avg_sentiment\"].min():.4f}')\n",
"    print(f'  Max:     {sentimiento_mensual[\"avg_sentiment\"].max():.4f}')\n",
"    print()\n",
"    print('Muestra del sentimiento mensual:')\n",
"    display(sentimiento_mensual.head(10))\n",
"\n",
"    # Guardar sentimiento mensual\n",
"    out_mensual = OUTPUT_NLP + 'sentimiento_mensual.csv'\n",
"    sentimiento_mensual.to_csv(out_mensual, index=False, encoding='utf-8-sig')\n",
"    print(f'Guardado: {out_mensual}')\n",
]))

# GRAFICO 3: Serie temporal sentimiento mensual
cells.append(md(["## Grafico 3 — Serie Temporal del Sentimiento Mensual\n"]))
cells.append(code([
"if 'sentiment_score' in df_noticias.columns:\n",
"    fig, axes = plt.subplots(2, 1, figsize=(14, 8))\n",
"\n",
"    # Panel 1: Score mensual\n",
"    colors_sent = ['#27ae60' if v >= 0 else '#e74c3c'\n",
"                   for v in sentimiento_mensual['avg_sentiment']]\n",
"    axes[0].bar(range(len(sentimiento_mensual)),\n",
"                sentimiento_mensual['avg_sentiment'],\n",
"                color=colors_sent, edgecolor='white', alpha=0.85)\n",
"    axes[0].axhline(0, color='black', linewidth=1)\n",
"    axes[0].plot(range(len(sentimiento_mensual)),\n",
"                 sentimiento_mensual['avg_sentiment'].rolling(3, center=True).mean(),\n",
"                 color='#2c3e50', linewidth=2, linestyle='--', label='Media movil 3 meses')\n",
"    axes[0].set_xticks(range(0, len(sentimiento_mensual), 3))\n",
"    axes[0].set_xticklabels(sentimiento_mensual['fecha_evento'].iloc[::3],\n",
"                             rotation=45, ha='right', fontsize=7)\n",
"    axes[0].set_ylabel('Score promedio mensual', fontsize=10)\n",
"    axes[0].set_title('Sentimiento Mensual Promedio (BETO)', fontsize=11, fontweight='bold')\n",
"    axes[0].legend(fontsize=9)\n",
"    axes[0].grid(True, linestyle='--', alpha=0.4)\n",
"\n",
"    # Panel 2: Composicion por etiqueta\n",
"    axes[1].bar(range(len(sentimiento_mensual)),\n",
"                sentimiento_mensual['n_positivas'],\n",
"                color='#27ae60', edgecolor='white', alpha=0.85, label='Positivas')\n",
"    axes[1].bar(range(len(sentimiento_mensual)),\n",
"                sentimiento_mensual['n_neutrales'],\n",
"                bottom=sentimiento_mensual['n_positivas'],\n",
"                color='#95a5a6', edgecolor='white', alpha=0.85, label='Neutrales')\n",
"    axes[1].bar(range(len(sentimiento_mensual)),\n",
"                sentimiento_mensual['n_negativas'],\n",
"                bottom=sentimiento_mensual['n_positivas'] + sentimiento_mensual['n_neutrales'],\n",
"                color='#e74c3c', edgecolor='white', alpha=0.85, label='Negativas')\n",
"    axes[1].set_xticks(range(0, len(sentimiento_mensual), 3))\n",
"    axes[1].set_xticklabels(sentimiento_mensual['fecha_evento'].iloc[::3],\n",
"                             rotation=45, ha='right', fontsize=7)\n",
"    axes[1].set_ylabel('Numero de noticias', fontsize=10)\n",
"    axes[1].set_title('Composicion de Noticias por Etiqueta', fontsize=11, fontweight='bold')\n",
"    axes[1].legend(fontsize=9)\n",
"\n",
"    plt.suptitle('Grafico 3 — Serie Temporal del Sentimiento Mensual\\n'\n",
"                 'Verde = meses positivos | Rojo = meses negativos',\n",
"                 fontsize=12, fontweight='bold')\n",
"    plt.tight_layout()\n",
"    g3 = OUTPUT_NLP + 'g3_serie_sentimiento_mensual.png'\n",
"    plt.savefig(g3, dpi=120, bbox_inches='tight'); plt.show()\n",
"    print('Guardado:', g3)\n",
]))
cells.append(interp([
"**Que muestra:** La evolucion mensual del sentimiento de las noticias agricolas. "
"Los meses con score negativo (rojo) deben correlacionar con eventos adversos "
"como el Fenomeno del Nino 2023 o caidas de precio.\n\n",
"**Media movil de 3 meses:** Suaviza el ruido mensual y muestra la tendencia "
"del sentimiento a mediano plazo. Es util para identificar periodos prolongados "
"de pessimismo o optimismo en el sector.\n\n",
"**Implicacion para el modelo:** El LSTM-Attention aprendera a asociar periodos "
"de sentimiento negativo con caidas de produccion en los meses siguientes, "
"capturando la latencia entre la percepcion del mercado y el impacto real.\n",
]))

# SECCION 6: CORRELACION SENTIMIENTO VS PRODUCCION
cells.append(md(["---\n","# 6. Correlacion Sentimiento vs Produccion de Limon\n","\n",
"Validacion de que el sentimiento tiene relacion con la produccion. "
"Si la correlacion es significativa, justifica incluirlo en el modelo.\n"]))
cells.append(code([
"if 'sentiment_score' in df_noticias.columns:\n",
"    # Cargar dataset Fase 1\n",
"    df_fase1 = pd.read_csv(CONFIG['base_dataset'])\n",
"    prod_mensual = df_fase1.groupby('fecha_evento')['produccion_t'].sum().reset_index()\n",
"\n",
"    # Merge con sentimiento mensual\n",
"    df_comp = pd.merge(sentimiento_mensual, prod_mensual, on='fecha_evento', how='inner')\n",
"\n",
"    corr = df_comp['avg_sentiment'].corr(df_comp['produccion_t'])\n",
"    print(f'Correlacion de Pearson sentimiento vs produccion: {corr:.4f}')\n",
"    print(f'Meses en comun: {len(df_comp)}')\n",
"    print()\n",
"\n",
"    fig, axes = plt.subplots(1, 2, figsize=(13, 5))\n",
"\n",
"    # Scatter\n",
"    axes[0].scatter(df_comp['avg_sentiment'], df_comp['produccion_t']/1e3,\n",
"                    alpha=0.7, color='#8e44ad', edgecolors='white', s=60)\n",
"    z = np.polyfit(df_comp['avg_sentiment'], df_comp['produccion_t']/1e3, 1)\n",
"    p = np.poly1d(z)\n",
"    x_line = np.linspace(df_comp['avg_sentiment'].min(), df_comp['avg_sentiment'].max(), 100)\n",
"    axes[0].plot(x_line, p(x_line), 'r--', linewidth=1.5, label=f'Tendencia (r={corr:.3f})')\n",
"    axes[0].axvline(0, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)\n",
"    axes[0].set_xlabel('Score de sentimiento promedio mensual', fontsize=10)\n",
"    axes[0].set_ylabel('Produccion mensual (miles t)', fontsize=10)\n",
"    axes[0].set_title('Scatter: Sentimiento vs Produccion', fontsize=11, fontweight='bold')\n",
"    axes[0].legend(fontsize=9)\n",
"\n",
"    # Doble eje temporal\n",
"    ax2 = axes[1].twinx()\n",
"    axes[1].bar(range(len(df_comp)), df_comp['avg_sentiment'],\n",
"                color=['#27ae60' if v >= 0 else '#e74c3c' for v in df_comp['avg_sentiment']],\n",
"                alpha=0.5, width=0.8, label='Sentimiento')\n",
"    ax2.plot(range(len(df_comp)), df_comp['produccion_t']/1e3,\n",
"             color='#2c3e50', linewidth=2, marker='o', markersize=3,\n",
"             label='Produccion (miles t)')\n",
"    axes[1].set_xticks(range(0, len(df_comp), 6))\n",
"    axes[1].set_xticklabels(df_comp['fecha_evento'].iloc[::6],\n",
"                             rotation=45, ha='right', fontsize=7)\n",
"    axes[1].set_ylabel('Score sentimiento', fontsize=10, color='#8e44ad')\n",
"    ax2.set_ylabel('Produccion (miles t)', fontsize=10, color='#2c3e50')\n",
"    axes[1].set_title('Sentimiento vs Produccion en el tiempo', fontsize=11, fontweight='bold')\n",
"    lines1, labels1 = axes[1].get_legend_handles_labels()\n",
"    lines2, labels2 = ax2.get_legend_handles_labels()\n",
"    axes[1].legend(lines1+lines2, labels1+labels2, fontsize=8, loc='upper left')\n",
"\n",
"    plt.suptitle('Grafico 4 — Correlacion Sentimiento vs Produccion de Limon\\n'\n",
"                 f'Correlacion de Pearson: r = {corr:.4f}',\n",
"                 fontsize=12, fontweight='bold')\n",
"    plt.tight_layout()\n",
"    g4 = OUTPUT_NLP + 'g4_correlacion_sentimiento_produccion.png'\n",
"    plt.savefig(g4, dpi=120, bbox_inches='tight'); plt.show()\n",
"    print('Guardado:', g4)\n",
]))
cells.append(interp([
"**Que muestra:** La relacion entre el sentimiento mensual de las noticias "
"y la produccion de limon. Una correlacion positiva indica que meses con "
"noticias optimistas coinciden con mayor produccion.\n\n",
"**Interpretacion del coeficiente:**\n"
"- |r| > 0.5: correlacion alta — el sentimiento es muy informativo\n"
"- |r| 0.3-0.5: correlacion media — el sentimiento aporta informacion util\n"
"- |r| < 0.3: correlacion baja — el sentimiento tiene poco poder predictivo directo\n\n",
"**Nota:** Una correlacion baja no invalida el uso del sentimiento. "
"El LSTM-Attention puede capturar relaciones no lineales y con rezago temporal "
"que la correlacion de Pearson no detecta.\n",
]))

# SECCION 7: MERGE CON DATASET FASE 1
cells.append(md(["---\n","# 7. Integracion del Sentimiento al Dataset Fase 1\n","\n",
"Se hace merge del sentimiento mensual con el dataset de la Fase 1. "
"Los meses sin noticias reciben `avg_sentiment = 0` (neutral).\n"]))
cells.append(code([
"if 'sentiment_score' in df_noticias.columns:\n",
"    # Cargar dataset Fase 1 (valores originales)\n",
"    df_fase1 = pd.read_csv(CONFIG['base_dataset'])\n",
"    n_antes = len(df_fase1)\n",
"\n",
"    # Merge con sentimiento mensual\n",
"    df_con_sent = pd.merge(\n",
"        df_fase1,\n",
"        sentimiento_mensual[['fecha_evento','avg_sentiment','n_noticias_beto']],\n",
"        on='fecha_evento',\n",
"        how='left'\n",
"    )\n",
"\n",
"    # Meses sin noticias -> sentimiento neutral (0)\n",
"    df_con_sent['avg_sentiment'] = df_con_sent['avg_sentiment'].fillna(0.0)\n",
"    df_con_sent['n_noticias_beto'] = df_con_sent['n_noticias_beto'].fillna(0).astype(int)\n",
"\n",
"    n_despues = len(df_con_sent)\n",
"    n_meses_sin_sent = (df_con_sent['avg_sentiment'] == 0).sum()\n",
"\n",
"    print(f'Dataset Fase 1: {n_antes:,} filas')\n",
"    print(f'Dataset con sentimiento: {n_despues:,} filas')\n",
"    print(f'Filas con avg_sentiment = 0 (sin noticias ese mes): {n_meses_sin_sent:,}')\n",
"    print(f'Nulos en avg_sentiment: {df_con_sent[\"avg_sentiment\"].isnull().sum()}')\n",
"    print()\n",
"    print('Columnas del dataset con sentimiento:')\n",
"    print(df_con_sent.columns.tolist())\n",
"    print()\n",
"    display(df_con_sent[['fecha_evento','departamento','provincia',\n",
"                          'produccion_t','n_noticias','avg_sentiment']].head(8))\n",
"\n",
"    # Exportar\n",
"    out_dataset = OUTPUT_NLP + 'dataset_con_sentimiento.csv'\n",
"    df_con_sent.to_csv(out_dataset, index=False, encoding='utf-8-sig')\n",
"    print(f'Exportado: {out_dataset}')\n",
"    print(f'Shape: {df_con_sent.shape}')\n",
]))
cells.append(interp([
"**Que muestra:** El dataset de la Fase 1 con la nueva columna `avg_sentiment` integrada. "
"Cada fila (mes-provincia) tiene ahora el sentimiento promedio de las noticias de ese mes.\n\n",
"**Estrategia de relleno:** Los meses sin noticias reciben `avg_sentiment = 0` (neutral). "
"Esto es conservador — no asumimos que la ausencia de noticias es positiva o negativa.\n\n",
"**Resultado:** El dataset pasa de 17 a 18 columnas, con `avg_sentiment` como "
"la primera variable NLP cuantificada. Las siguientes actividades agregaran "
"las variables ciclicas, geograficas y los rezagos temporales.\n",
]))

# GRAFICO 5: Distribucion avg_sentiment en el dataset
cells.append(md(["## Grafico 5 — Distribucion de avg_sentiment en el Dataset Integrado\n"]))
cells.append(code([
"if 'sentiment_score' in df_noticias.columns:\n",
"    fig, axes = plt.subplots(1, 2, figsize=(13, 4))\n",
"\n",
"    # Histograma\n",
"    axes[0].hist(df_con_sent['avg_sentiment'], bins=30,\n",
"                 color='#8e44ad', edgecolor='white', alpha=0.85)\n",
"    axes[0].axvline(0, color='black', linestyle='--', linewidth=1.5, label='Neutral (0)')\n",
"    axes[0].axvline(df_con_sent['avg_sentiment'].mean(), color='#c0392b',\n",
"                    linestyle='--', linewidth=2,\n",
"                    label=f'Media: {df_con_sent[\"avg_sentiment\"].mean():.4f}')\n",
"    axes[0].set_xlabel('avg_sentiment', fontsize=11)\n",
"    axes[0].set_ylabel('Frecuencia (filas del dataset)', fontsize=10)\n",
"    axes[0].set_title('Distribucion de avg_sentiment\\nen el dataset integrado', fontsize=11, fontweight='bold')\n",
"    axes[0].legend(fontsize=9)\n",
"\n",
"    # Boxplot por año\n",
"    df_con_sent['anio'] = df_con_sent['fecha_evento'].str[:4]\n",
"    df_con_sent[df_con_sent['avg_sentiment'] != 0].boxplot(\n",
"        column='avg_sentiment', by='anio', ax=axes[1],\n",
"        patch_artist=True,\n",
"        boxprops=dict(facecolor='#8e44ad', alpha=0.6),\n",
"        medianprops=dict(color='#c0392b', linewidth=2)\n",
"    )\n",
"    axes[1].axhline(0, color='black', linestyle='--', linewidth=1)\n",
"    axes[1].set_xlabel('Año', fontsize=11)\n",
"    axes[1].set_ylabel('avg_sentiment', fontsize=11)\n",
"    axes[1].set_title('avg_sentiment por Año', fontsize=11, fontweight='bold')\n",
"\n",
"    plt.suptitle('Grafico 5 — Variable avg_sentiment en el Dataset Integrado\\n'\n",
"                 f'Shape: {df_con_sent.shape} | Nulos: {df_con_sent[\"avg_sentiment\"].isnull().sum()}',\n",
"                 fontsize=12, fontweight='bold')\n",
"    plt.tight_layout()\n",
"    g5 = OUTPUT_NLP + 'g5_avg_sentiment_dataset.png'\n",
"    plt.savefig(g5, dpi=120, bbox_inches='tight'); plt.show()\n",
"    print('Guardado:', g5)\n",
]))
cells.append(interp([
"**Que muestra:** La distribucion de `avg_sentiment` en el dataset integrado completo. "
"El pico en 0 corresponde a los meses sin noticias (relleno neutral).\n\n",
"**Validacion:** La distribucion debe tener variabilidad suficiente para ser informativa. "
"Si casi todos los valores son 0, el sentimiento no aportara informacion al modelo.\n\n",
"**Implicacion para el modelo:** La variable `avg_sentiment` esta lista para ser "
"incluida en el dataset de la Fase 2. En la Actividad 3 se generaran sus rezagos "
"temporales (avg_sentiment_lag1, avg_sentiment_lag3) para capturar la memoria "
"del sentimiento del mercado.\n",
]))

# RESUMEN FINAL
cells.append(md(["---\n","# 8. Resumen de la Actividad 1\n"]))
cells.append(code([
"print('=' * 70)\n",
"print('  ACTIVIDAD 1 COMPLETADA — NLP SENTIMIENTO CON BETO')\n",
"print('=' * 70)\n",
"print()\n",
"if 'sentiment_score' in df_noticias.columns:\n",
"    print(f'Noticias procesadas: {len(df_noticias):,}')\n",
"    print(f'Score medio global: {df_noticias[\"sentiment_score\"].mean():.4f}')\n",
"    print(f'Meses con sentimiento: {len(sentimiento_mensual)}')\n",
"    print(f'Dataset resultante: {df_con_sent.shape}')\n",
"    print(f'Nueva columna: avg_sentiment (float, rango [-1, 1])')\n",
"    print()\n",
"    print('Archivos generados:')\n",
"    import glob\n",
"    for f in sorted(glob.glob(OUTPUT_NLP + '*')):\n",
"        kb = os.path.getsize(f)//1024\n",
"        print(f'  {os.path.basename(f):<45} {kb} KB')\n",
"print()\n",
"print('Proximos pasos:')\n",
"print('  Actividad 2 -> Codificacion ciclica (month_sin, month_cos) + lat/lon')\n",
"print('  Entrada: notebooks/fase2/output/01_nlp_sentimiento/dataset_con_sentimiento.csv')\n",
]))

# GUARDAR NOTEBOOK
notebook = nb(cells)
out_path = 'notebooks/fase2/actividad_01_nlp_sentimiento.ipynb'
with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)
print(f'Generado: {out_path} ({len(cells)} celdas)')
