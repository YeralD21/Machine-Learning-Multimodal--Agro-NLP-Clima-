import os, sys, json
import pandas as pd
import numpy as np

# Config
while not os.path.exists('notebooks/fase2/config/fase2_config.json'):
    os.chdir('..')

with open('notebooks/fase2/config/fase2_config.json','r',encoding='utf-8') as f:
    CONFIG = json.load(f)

OUTPUT_NLP = CONFIG['output']['nlp']
cache_path = OUTPUT_NLP + 'noticias_con_sentimiento.csv'

if not os.path.exists(cache_path):
    print(f'ERROR: No existe {cache_path}')
    sys.exit(1)

df_noticias = pd.read_csv(cache_path)

print('--- SECTION 4: TOP NOTICIAS ---')
top_pos = df_noticias.nlargest(5, 'sentiment_score')[['fecha_evento','titular','sentiment_score']]
top_neg = df_noticias.nsmallest(5, 'sentiment_score')[['fecha_evento','titular','sentiment_score']]

print('\nTOP 5 POSITIVAS:')
for _, row in top_pos.iterrows():
    print(f'  [{row["fecha_evento"]}] {row["sentiment_score"]:+.4f} | {str(row["titular"])[:80]}')

print('\nTOP 5 NEGATIVAS:')
for _, row in top_neg.iterrows():
    print(f'  [{row["fecha_evento"]}] {row["sentiment_score"]:+.4f} | {str(row["titular"])[:80]}')

print('\n--- SECTION 5: SENTIMIENTO MENSUAL ---')
sentimiento_mensual = df_noticias.groupby('fecha_evento').agg(
    avg_sentiment=('sentiment_score', 'mean'),
    n_noticias=('sentiment_score', 'count')
).reset_index()

print('\nTop 5 Meses Optimistas:')
print(sentimiento_mensual.sort_values('avg_sentiment', ascending=False).head(5).to_string(index=False))
print('\nTop 5 Meses Pesimistas:')
print(sentimiento_mensual.sort_values('avg_sentiment', ascending=True).head(5).to_string(index=False))

print('\n--- SECTION 6: CORRELACION ---')
# Intentar cargar dataset base
try:
    df_fase1 = pd.read_csv(CONFIG['base_dataset'])
    prod_mensual = df_fase1.groupby('fecha_evento')['produccion_t'].sum().reset_index()
    df_comp = pd.merge(sentimiento_mensual, prod_mensual, on='fecha_evento', how='inner')
    corr = df_comp['avg_sentiment'].corr(df_comp['produccion_t'])
    print(f'Correlacion Sentimiento vs Produccion: {corr:.4f}')
    print(f'Meses comparados: {len(df_comp)}')
except Exception as e:
    print(f'No se pudo calcular la correlacion: {e}')
