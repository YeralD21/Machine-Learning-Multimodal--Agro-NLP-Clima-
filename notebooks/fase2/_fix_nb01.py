import json

with open('notebooks/fase2/actividad_01_nlp_sentimiento.ipynb','r',encoding='utf-8') as f:
    nb = json.load(f)

# Encontrar la celda de verificacion de cache (celda 10 aprox)
for i, cell in enumerate(nb['cells']):
    if cell['cell_type'] == 'code':
        src = ''.join(cell['source'])
        if 'cache_path' in src and 'BETO_EJECUTADO' in src:
            print(f'Celda cache check: {i}')
            # Reemplazar para que cargue el cache en df_noticias directamente
            new_src = (
                "# Verificar si ya existe el resultado guardado (para no re-procesar)\n"
                "cache_path = OUTPUT_NLP + 'noticias_con_sentimiento.csv'\n"
                "\n"
                "if os.path.exists(cache_path):\n"
                "    print(f'Cache encontrado: {cache_path}')\n"
                "    print('Cargando resultado previo...')\n"
                "    df_noticias = pd.read_csv(cache_path)\n"
                "    df_noticias['fecha_dt'] = pd.to_datetime(df_noticias['fecha'], errors='coerce')\n"
                "    df_noticias['fecha_evento'] = df_noticias['fecha_dt'].dt.strftime('%Y-%m')\n"
                "    BETO_EJECUTADO = True\n"
                "    print(f'  {len(df_noticias)} noticias con sentimiento cargadas.')\n"
                "    print(f'  Columnas: {df_noticias.columns.tolist()}')\n"
                "else:\n"
                "    print('No hay cache. Se ejecutara BETO...')\n"
                "    BETO_EJECUTADO = False\n"
                "\n"
                "print()\n"
                "print('Estado:', 'BETO ya ejecutado (cache)' if BETO_EJECUTADO else 'BETO pendiente')\n"
            )
            nb['cells'][i]['source'] = new_src
            nb['cells'][i]['outputs'] = []
            nb['cells'][i]['execution_count'] = None
            print('  Fix aplicado')
            break

# Limpiar todos los outputs para re-ejecucion limpia
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        cell['outputs'] = []
        cell['execution_count'] = None

with open('notebooks/fase2/actividad_01_nlp_sentimiento.ipynb','w',encoding='utf-8') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
print('Notebook guardado OK')
