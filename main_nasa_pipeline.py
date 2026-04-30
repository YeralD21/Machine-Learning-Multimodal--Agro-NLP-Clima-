"""
==========================================================================
MAIN — Orquestador del Pipeline NASA POWER (10 Actividades)
==========================================================================
Ejecuta secuencialmente las 10 actividades del pipeline de ingeniería
de datos climáticos para la tesis: Predicción de Producción de Limón.

Uso:
    python main_nasa_pipeline.py              # Ejecuta todas las actividades
    python main_nasa_pipeline.py --desde 3    # Reanuda desde la Actividad 3
    python main_nasa_pipeline.py --actividad 9  # Ejecuta solo la Actividad 9

Estructura de directorios:
    data/raw/nasapowercrudo/  → CSVs crudos descargados de NASA POWER
    data/raw/nasapower/       → CSVs pre-procesados con dpto/provincia
    data/02_interim_nasa/     → Datos intermedios del pipeline
    data/03_processed_nasa/   → Dataset final nasa_climatic_processed.csv
==========================================================================
"""
import sys
import os
import time
import argparse
import traceback

sys.stdout.reconfigure(encoding='utf-8')

# ─── Mapa de actividades ─────────────────────────────────────────────────
ACTIVIDADES = {
    1:  ('Configuración del Proyecto NASA',
         'src.data_processing.nasa_pipeline.actividad_01_config_nasa'),
    2:  ('Lectura de Datasets NASA POWER',
         'src.data_processing.nasa_pipeline.actividad_02_lectura_nasa'),
    3:  ('EDA Climático',
         'src.data_processing.nasa_pipeline.actividad_03_eda_nasa'),
    4:  ('Calidad de los Datos',
         'src.data_processing.nasa_pipeline.actividad_04_calidad_nasa'),
    5:  ('Limpieza y Estandarización',
         'src.data_processing.nasa_pipeline.actividad_05_limpieza_nasa'),
    6:  ('Integración y Granularidad',
         'src.data_processing.nasa_pipeline.actividad_06_granularidad_nasa'),
    7:  ('Diseño del Esquema DWH',
         'src.data_processing.nasa_pipeline.actividad_07_dwh_nasa'),
    8:  ('Esquema en PostgreSQL',
         'src.data_processing.nasa_pipeline.actividad_08_postgresql_nasa'),
    9:  ('Pipeline ETL Completo',
         'src.data_processing.nasa_pipeline.actividad_09_etl_nasa'),
    10: ('Reexploración Post-ETL',
         'src.data_processing.nasa_pipeline.actividad_10_reexploracion_nasa'),
}


def run_activity(num: int, nombre: str, modulo: str) -> bool:
    """Importa y ejecuta el módulo de una actividad."""
    print()
    print('═' * 70)
    print(f'  ▶  ACTIVIDAD {num}: {nombre}')
    print('═' * 70)
    t0 = time.time()

    try:
        import importlib
        mod = importlib.import_module(modulo)
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
    parser = argparse.ArgumentParser(description='Orquestador Pipeline NASA POWER')
    parser.add_argument('--desde', type=int, default=1,
                        help='Número de actividad desde la que iniciar (default: 1)')
    parser.add_argument('--hasta', type=int, default=10,
                        help='Número de actividad hasta la que ejecutar (default: 10)')
    parser.add_argument('--actividad', type=int, default=None,
                        help='Ejecutar solo una actividad específica')
    args = parser.parse_args()

    print()
    print('╔' + '═' * 68 + '╗')
    print('║  PIPELINE NASA POWER — Ingeniería de Datos Climáticos           ║')
    print('║  Tesis: Predicción de Producción de Limón — LSTM Multimodal     ║')
    print('╚' + '═' * 68 + '╝')
    print()
    print('  Actividades del Pipeline:')
    for num, (nombre, _) in ACTIVIDADES.items():
        print(f'    [{num:02d}] {nombre}')
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

        # Si una actividad crítica falla, detener el pipeline
        if not ok and num in [1, 2, 5, 9]:
            print(f'\n  ❌ Actividad crítica {num} falló. Deteniendo pipeline.')
            break

    # ─── Resumen final ────────────────────────────────────────────────────
    elapsed_total = time.time() - t_total
    print()
    print('═' * 70)
    print('  RESUMEN DE EJECUCIÓN — PIPELINE NASA POWER')
    print('═' * 70)
    for num, ok in resultados.items():
        nombre = ACTIVIDADES[num][0]
        icono = '✅' if ok else '❌'
        print(f'  {icono} Actividad {num:02d}: {nombre}')
    print()
    print(f'  Tiempo total: {elapsed_total:.1f}s')

    # Verificar archivos clave generados
    archivos_clave = [
        'data/02_interim_nasa/nasa_pipeline_config.json',
        'data/02_interim_nasa/nasa_long_raw.csv',
        'data/02_interim_nasa/nasa_long_clean.csv',
        'data/02_interim_nasa/nasa_mensual_integrado.csv',
        'data/03_processed_nasa/nasa_climatic_processed.csv',
        'data/03_processed_nasa/nasa_climatic_raw_values.csv',
        'models/scalers/scaler_nasa_clima.pkl',
        'database/dwh_nasa_clima_schema.sql',
    ]
    print()
    print('  Archivos clave generados:')
    for path in archivos_clave:
        existe = '✅' if os.path.exists(path) else '❌'
        size = f'({os.path.getsize(path)/1024:.0f} KB)' if os.path.exists(path) else ''
        print(f'    {existe} {path} {size}')
    print()

    # Instrucciones de integración con el pipeline principal
    print('  ─' * 35)
    print('  INTEGRACIÓN CON EL PIPELINE PRINCIPAL:')
    print('  ─' * 35)
    print('  El archivo nasa_climatic_processed.csv está listo para merge.')
    print('  En actividad_06_07_integracion_dwh.py, descomenta el bloque TODO:')
    print()
    print('    df_nasa = pd.read_csv("data/03_processed_nasa/nasa_climatic_processed.csv")')
    print('    df_int = pd.merge(df_int, df_nasa,')
    print('                      on=["fecha_evento", "DEPARTAMENTO", "PROVINCIA"],')
    print('                      how="left")')
    print()


if __name__ == '__main__':
    main()
