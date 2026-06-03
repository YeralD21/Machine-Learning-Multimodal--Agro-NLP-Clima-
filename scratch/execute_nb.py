import nbformat
from nbclient import NotebookClient
import os

# Ruta al notebook
nb_path = 'notebooks/fase2/actividad_01_nlp_sentimiento.ipynb'

if not os.path.exists(nb_path):
    print(f'ERROR: No existe {nb_path}')
    exit(1)

print(f'Cargando notebook: {nb_path}')
with open(nb_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

print('Ejecutando celdas...')
client = NotebookClient(nb, timeout=600, kernel_name='python3')
try:
    client.execute()
    print('Ejecucion completada.')

    print('Guardando notebook con resultados...')
    with open(nb_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)
    print('Listo. El notebook ahora tiene graficos.')
except Exception as e:
    print(f'ERROR durante la ejecucion: {e}')
