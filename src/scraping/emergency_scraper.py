"""
==========================================================================
Paso 2 del Pipeline (Revisado): ETL Temporal de Emergencias INDECI (SINPAD)
==========================================================================
Procesa los registros de emergencia detallados (evento por evento) extraídos
de los archivos DBF del Portal de Datos Abiertos para los años 2021-2023.

Niveles de agrupacion: fecha_evento (YYYY-MM), departamento, provincia
Variables de salida:
  - num_emergencias (conteo)
  - total_afectados (suma de personas afectadas y damnificadas)
  - hectareas_cultivo_perdidas (suma)

Peligros filtrados: Solo de origen hidrometeorologico/climatico.
"""

import os
import sys
import unicodedata
import pandas as pd
from dbfread import DBF

def strip_accents(text: str) -> str:
    """Elimina tildes manteniendo la Ñ."""
    if not isinstance(text, str): return text
    result = []
    for char in unicodedata.normalize('NFD', text):
        if unicodedata.category(char) == 'Mn':
            continue
        result.append(char)
    return ''.join(result)

def normalize_geo(text: str) -> str:
    """Normaliza nombres geograficos."""
    if not isinstance(text, str):
        return text
    text = text.strip().upper()
    text = text.replace('Ñ', '##NN##').replace('ñ', '##nn##')
    text = strip_accents(text)
    text = text.replace('##NN##', 'Ñ').replace('##nn##', 'ñ')
    return text

# Peligros climaticos/naturales a mantener
PELIGROS_VALIDOS = [
    'LLUVIAS INTENSAS', 'INUNDACION', 'HUAYCO', 'SEQUIA',
    'HELADAS', 'FRIAJE', 'GRANIZADA', 'NEVADA', 'VIENTOS FUERTES',
    'DESLIZAMIENTO', 'EROSION'
]

def load_dbf_to_df(dbf_path: str) -> pd.DataFrame:
    try:
        # Se asume encoding latin1 que es comun en dbfs antiguos de Msn
        table = DBF(dbf_path, encoding='latin1', load=True)
        records = list(table)
        return pd.DataFrame(records)
    except Exception as e:
        print(f"Error cargando {dbf_path}: {e}")
        return pd.DataFrame()

def run_etl():
    print("=" * 70)
    print("  INDECI ETL - Paso 2: Procesamiento Temporal Evento a Evento")
    print("=" * 70)

    dbf_files = [
        "data/raw/indeci/E_2021/Emergencias_2021.dbf",
        "data/raw/indeci/E_2022/Emergencias_2022.dbf",
        "data/raw/indeci/E_2023/E_2023.dbf"
    ]

    dfs = []
    for dbf_file in dbf_files:
        if os.path.exists(dbf_file):
            print(f"[+] Cargando archivo: {os.path.basename(dbf_file)}...")
            df_year = load_dbf_to_df(dbf_file)
            if not df_year.empty:
                # Estandarizar algunos nombres de columna que podrian variar
                # por longitud maxima de caracteres en DBF
                df_year.columns = [str(c).lower() for c in df_year.columns]
                dfs.append(df_year)
        else:
            print(f"[!] Archivo no encontrado: {dbf_file}")

    if not dfs:
        print("No se encontraron archivos DBF de emergencias.")
        return

    print("\nCombinando y limpiando datasets...")
    df = pd.concat(dfs, ignore_index=True)

    # Validar campos esenciales
    req_cols = ['fecha', 'departamen', 'provincia', 'fenomeno']
    for c in req_cols:
        if c not in df.columns:
            print(f"ERROR: Columna requerida '{c}' no encontrada en el DBF.")
            return

    # Convertir las columnas numericas que capturamos
    num_cols = {
        'safecta': 'personas_afectadas',
        'sdamni': 'personas_damnificadas',
        'safecta_al': 'personas_afectadas_alt',
        'sdamni_al': 'personas_damnificadas_alt',
        'sareacul_1': 'hectareas_cultivo_perdidas',
        'sareaculti': 'hectareas_cultivo_afectadas'
    }

    # Asegurarnos de que si alguna columna falta por versiones, no quiebre
    for old_col in num_cols.keys():
        if old_col not in df.columns:
            df[old_col] = 0
            
    # Sumar afectadas y damnificadas
    df['total_afectados'] = pd.to_numeric(df['safecta'], errors='coerce').fillna(0) + \
                            pd.to_numeric(df['sdamni'], errors='coerce').fillna(0)
                            
    df['hectareas_cultivo_perdidas'] = pd.to_numeric(df['sareacul_1'], errors='coerce').fillna(0)
    df['hectareas_cultivo_afectadas'] = pd.to_numeric(df['sareaculti'], errors='coerce').fillna(0)

    # 1. Filtro Temporal
    print("Normalizando fechas y aplicando filtro temporal...")
    # 'fecha' viene comunmente en DD/MM/YYYY
    df['fecha_dt'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y', errors='coerce')
    # Manejar posibles fechas en YYYY-MM-DD
    mask_nat = df['fecha_dt'].isna()
    if mask_nat.any():
        df.loc[mask_nat, 'fecha_dt'] = pd.to_datetime(df.loc[mask_nat, 'fecha'], errors='coerce')

    df = df.dropna(subset=['fecha_dt'])
    
    # Acotar a 2021-01 hasta 2025-08
    start_date = pd.to_datetime('2021-01-01')
    end_date = pd.to_datetime('2025-08-31')
    df = df[(df['fecha_dt'] >= start_date) & (df['fecha_dt'] <= end_date)]
    
    df['fecha_evento'] = df['fecha_dt'].dt.strftime('%Y-%m')

    # 2. Filtro de Geometrias y Peligros
    df['departamento'] = df['departamen'].apply(normalize_geo)
    df['provincia'] = df['provincia'].apply(normalize_geo)
    df['fenomeno'] = df['fenomeno'].astype(str).str.strip().str.upper()
    df['fenomeno'] = df['fenomeno'].apply(strip_accents)

    # Filtrar solo fenomenos identificados como climaticos/hidrometeorologicos
    def is_valid_hazard(fen):
        for pv in PELIGROS_VALIDOS:
            if pv in fen:
                return True
        return False

    df_filtered = df[df['fenomeno'].apply(is_valid_hazard)].copy()
    print(f"Eventos hidrometeorologicos post-filtro: {len(df_filtered)}")

    # 3. Agrupacion
    print("Agrupando a nivel temporal/geografico...")
    df_temporal = df_filtered.groupby(['fecha_evento', 'departamento', 'provincia']).agg(
        num_emergencias=('ide_sinpad', 'count'),
        total_afectados=('total_afectados', 'sum'),
        hectareas_cultivo_perdidas=('hectareas_cultivo_perdidas', 'sum')
    ).reset_index()

    # Redondear hectareas
    df_temporal['hectareas_cultivo_perdidas'] = df_temporal['hectareas_cultivo_perdidas'].round(2)

    # Exportar
    output_dir = os.path.join("data", "interim", "indeci")
    output_path = os.path.join(output_dir, "indeci_temporal_2021_2025.csv")
    df_temporal.to_csv(output_path, index=False, encoding='utf-8')

    print("\n" + "=" * 70)
    print("  REPORTE FIN DEL ETL TEMPORAL")
    print("=" * 70)
    print(f"  Filas resultantes (mes x prov): {len(df_temporal)}")
    print(f"  Departamentos                 : {df_temporal['departamento'].nunique()}")
    print(f"  Provincias                    : {df_temporal['provincia'].nunique()}")
    print(f"  Total num_emergencias         : {df_temporal['num_emergencias'].sum()}")
    print(f"  Total afectados hist.         : {int(df_temporal['total_afectados'].sum()):,}")
    print(f"  Total Has perdidas            : {df_temporal['hectareas_cultivo_perdidas'].sum():,.2f}")
    print(f"  Meses unicos en dataset       : {df_temporal['fecha_evento'].nunique()}")
    print(f"  Archivo guardado en           : {output_path}")
    print("=" * 70)
    
    print("\n  Ejemplo de salida:")
    print(df_temporal.head(5).to_string(index=False))

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')
    run_etl()
