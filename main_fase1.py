"""
==========================================================================
MAIN.PY — Orquestador del Pipeline de 10 Actividades (Fase 1)
==========================================================================
Ejecuta secuencialmente las 10 actividades del pipeline de ingeniería de
datos para la tesis: Predicción de Producción de Limón con LSTM Multimodal.

Uso:
    python main.py                   # Ejecuta todas las actividades
    python main.py --desde 3         # Reanuda desde la Actividad 3
    python main.py --actividad 5     # Ejecuta solo la Actividad 5

Autores: Pipeline generado automáticamente — Fase 1
==========================================================================
"""
import sys
import os
import time
import argparse
import traceback

sys.stdout.reconfigure(encoding='utf-8')

# ─── Mapa de actividades ────────────────────────────────────────────────
ACTIVIDADES = {
    1:  ('Configuración del Proyecto',           None),           # ya ejecutada como script directo
    2:  ('Lectura de Datasets',                  'src.data_processing.actividad_02_lectura'),
    3:  ('EDA — Análisis Exploratorio',          'src.data_processing.actividad_03_eda'),
    4:  ('Calidad de los Datos',                 'src.data_processing.actividad_04_calidad'),
    5:  ('Limpieza y Estandarización',           'src.data_processing.actividad_05_limpieza'),
    6:  ('Integración + Diseño DWH',             'src.data_processing.actividad_06_07_integracion_dwh'),
    7:  ('Star Schema (incluido en Act. 6)',      None),           # integrado en actividad_06_07
    8:  ('Crear Esquemas PostgreSQL',             'src.data_processing.actividad_08_postgresql'),
    9:  ('Pipeline ETL Completo',                 'src.data_processing.actividad_09_etl'),
    10: ('Reexploración Post-ETL',               'src.data_processing.actividad_10_reexploracion'),
}

def run_activity(num: int, nombre: str, modulo: str):
    """Importa y ejecuta el módulo de una actividad."""
    print()
    print('═' * 70)
    print(f'  ▶  ACTIVIDAD {num}: {nombre}')
    print('═' * 70)
    t0 = time.time()

    if modulo is None:
        print(f'  ✅ Actividad {num} integrada en otro módulo o ejecutada previamente.')
        return True

    try:
        import importlib
        mod = importlib.import_module(modulo)
        # Re-ejecutar si el módulo ya estaba importado
        importlib.reload(mod)
        elapsed = time.time() - t0
        print(f'\n  ✅ Actividad {num} completada en {elapsed:.1f}s')
        return True
    except SystemExit:
        print(f'  ⚠️  Actividad {num} llamó a sys.exit() — continuando...')
        return False
    except Exception as e:
        print(f'\n  ❌ ERROR en Actividad {num}: {e}')
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description='Orquestador Pipeline Fase 1')
    parser.add_argument('--desde', type=int, default=2,
                        help='Número de actividad desde la que iniciar (default: 2)')
    parser.add_argument('--hasta', type=int, default=10,
                        help='Número de actividad hasta la que ejecutar (default: 10)')
    parser.add_argument('--actividad', type=int, default=None,
                        help='Ejecutar solo una actividad específica')
    args = parser.parse_args()

    print()
    print('╔' + '═' * 68 + '╗')
    print('║  PIPELINE FASE 1 — Ingeniería de Datos Multimodal para LSTM      ║')
    print('║  Tesis: Predicción de Producción de Limón                        ║')
    print('╚' + '═' * 68 + '╝')
    print()
    print('  Actividades del Pipeline:')
    for num, (nombre, modulo) in ACTIVIDADES.items():
        estado = '✅ integrada' if modulo is None else '⏳ pendiente'
        print(f'    [{num:02d}] {nombre:<45} {estado}')
    print()

    # Definir rango de ejecución
    if args.actividad:
        rango = [args.actividad]
    else:
        rango = range(args.desde, args.hasta + 1)

    t_total = time.time()
    resultados = {}

    for num in rango:
        if num not in ACTIVIDADES:
            print(f'  ⚠️  Actividad {num} no definida. Omitiendo.')
            continue
        nombre, modulo = ACTIVIDADES[num]
        ok = run_activity(num, nombre, modulo)
        resultados[num] = ok

    # ─── Resumen final ───────────────────────────────────────────────
    elapsed_total = time.time() - t_total
    print()
    print('═' * 70)
    print('  RESUMEN DE EJECUCIÓN DEL PIPELINE')
    print('═' * 70)
    for num, ok in resultados.items():
        nombre = ACTIVIDADES[num][0]
        icono = '✅' if ok else '❌'
        print(f'  {icono} Actividad {num:02d}: {nombre}')
    print()
    print(f'  Tiempo total: {elapsed_total:.1f}s')

    # Verificar archivos clave
    archivos_clave = [
        'data/02_interim/pipeline_config.json',
        'data/02_interim/midagri_limon_clean.csv',
        'data/02_interim/indeci_eventos_clean.csv',
        'data/02_interim/agraria_noticias_clean.csv',
        'data/02_interim/dataset_integrado.csv',
        'data/03_processed/master_dataset_fase1_v2.csv',
        'models/scalers/scaler_fase1_v2.pkl',
        'database/dwh_star_schema.sql',
    ]
    print('  Archivos clave generados:')
    for path in archivos_clave:
        existe = '✅' if os.path.exists(path) else '❌'
        size = f'({os.path.getsize(path)/1024:.0f} KB)' if os.path.exists(path) else ''
        print(f'    {existe} {path} {size}')
    print()


if __name__ == '__main__':
    main()
