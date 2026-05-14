"""
Ejecuta BETO sobre las noticias y guarda el cache.
Ejecutar desde la raiz del proyecto.
"""
import os, sys, time, json
import pandas as pd

# Navegar a raiz
while not os.path.exists('notebooks/fase2/config/fase2_config.json'):
    os.chdir('..')

with open('notebooks/fase2/config/fase2_config.json','r',encoding='utf-8') as f:
    CONFIG = json.load(f)

OUTPUT_NLP = CONFIG['output']['nlp']
os.makedirs(OUTPUT_NLP, exist_ok=True)
cache_path = OUTPUT_NLP + 'noticias_con_sentimiento.csv'

# Cargar noticias
df = pd.read_csv(CONFIG['noticias_path'], on_bad_lines='skip', low_memory=False)
df['fecha_dt'] = pd.to_datetime(df['fecha'], errors='coerce')
df['anio'] = df['fecha_dt'].dt.year
df['mes']  = df['fecha_dt'].dt.month
df['fecha_evento'] = df['fecha_dt'].dt.strftime('%Y-%m')
df = df[(df['anio'] >= 2021) & (df['anio'] <= 2025)].copy()

def preparar_texto(row, max_palabras=200):
    titular = str(row['titular']).strip() if pd.notna(row['titular']) else ''
    cuerpo  = str(row['cuerpo_completo']).strip() if pd.notna(row['cuerpo_completo']) else ''
    palabras = cuerpo.split()[:max_palabras]
    cuerpo_t = ' '.join(palabras)
    texto = f'{titular}. {cuerpo_t}' if cuerpo_t else titular
    return texto[:512]

df['texto_beto'] = df.apply(preparar_texto, axis=1)
df['n_palabras'] = df['texto_beto'].apply(lambda x: len(x.split()))

print(f'Noticias a procesar: {len(df)}')
print('Cargando modelo BETO...')

from pysentimiento import create_analyzer
analyzer = create_analyzer(task='sentiment', lang='es')
print('Modelo cargado OK.')
print()

scores = []
total = len(df)
t0 = time.time()

for i, (idx, row) in enumerate(df.iterrows()):
    texto = str(row['texto_beto'])
    try:
        result = analyzer.predict(texto)
        p_pos = result.probas.get('POS', 0.0)
        p_neg = result.probas.get('NEG', 0.0)
        p_neu = result.probas.get('NEU', 0.0)
        score = round(p_pos - p_neg, 4)
        label = result.output
    except Exception as e:
        score = 0.0
        p_pos = p_neg = p_neu = 0.0
        label = 'NEU'

    scores.append({
        'sentiment_score': score,
        'sentiment_label': label,
        'p_pos': round(p_pos, 4),
        'p_neg': round(p_neg, 4),
        'p_neu': round(p_neu, 4),
    })

    if (i + 1) % 50 == 0 or (i + 1) == total:
        elapsed = time.time() - t0
        eta = (elapsed / (i+1)) * (total - i - 1)
        print(f'  [{i+1:>3}/{total}] | Tiempo: {elapsed:.0f}s | ETA: {eta:.0f}s')

df_scores = pd.DataFrame(scores)
df_final = pd.concat([df.reset_index(drop=True), df_scores], axis=1)
df_final.to_csv(cache_path, index=False, encoding='utf-8-sig')

print()
print(f'Cache guardado: {cache_path}')
print(f'Tiempo total: {time.time()-t0:.0f}s')
print(f'Score medio: {df_final["sentiment_score"].mean():.4f}')
print('Distribucion de etiquetas:')
print(df_final['sentiment_label'].value_counts().to_string())
