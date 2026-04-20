"""
==========================================================================
Paso 2 del Pipeline: ETL de Emergencias INDECI (SINPAD) 2003-2024
==========================================================================
Lee los archivos Excel oficiales de INDECI (Tabla 1 - Emergencias y Daños,
Tabla 2 - Peligros) descargados del portal gob.pe y extrae:

  - Emergencias por provincia con daños (personas afectadas, cultivo
    afectado/perdido, viviendas).
  - Emergencias por tipo de peligro hidrometeorológico por provincia.

Nota: La data oficial de INDECI es un consolidado 2003-2024 (no tiene
desglose mensual público). Para el modelo LSTM, esto funciona como una
feature estática de "propensión a desastres" por provincia, que se
enriquecerá con las noticias scrapeadas de agraria.pe que SÍ tienen
marcas temporales.

Salida: data/raw/indeci/indeci_emergencias_2021_2025.csv
"""

import os
import sys
import unicodedata
import pandas as pd
import numpy as np


# =========================================================================
# Utilidades (mismas que midagri_etl.py para consistencia de merge)
# =========================================================================

def strip_accents(text: str) -> str:
    """Elimina tildes manteniendo la Ñ."""
    result = []
    for char in unicodedata.normalize('NFD', text):
        if unicodedata.category(char) == 'Mn':
            continue
        result.append(char)
    return ''.join(result)


def normalize_geo(text: str) -> str:
    """Normaliza nombres geográficos: MAYÚSCULAS, sin tildes, con Ñ."""
    if not isinstance(text, str):
        return text
    text = text.strip().upper()
    text = text.replace('Ñ', '##NN##').replace('ñ', '##nn##')
    text = strip_accents(text)
    text = text.replace('##NN##', 'Ñ').replace('##nn##', 'ñ')
    return text


# =========================================================================
# Peligros hidrometeorológicos de interés para el modelo agrícola
# =========================================================================
PELIGROS_HIDROMET = [
    'INUNDACION',
    'LLUVIA INTENSA',
    'SEQUIA',
    'HUAYCO',
    'VIENTOS FUERTES',
    'EROSION',
    'BAJAS TEMPERATURAS',     # Heladas → pierden cosechas
    'TORMENTA ELECTRICA',
    'MAREJADA',               # Afecta zonas costeras agrícolas
]


# =========================================================================
# Pipeline ETL
# =========================================================================

def run_etl():
    """Ejecuta el ETL completo de emergencias INDECI."""

    # Rutas
    emergencias_path = os.path.join("data", "raw", "indeci", "resumen_emergencias_2003_2024.xlsx")
    peligros_path = os.path.join("data", "raw", "indeci", "resumen_peligros_2003_2024.xlsx")
    output_dir = os.path.join("data", "raw", "indeci")
    output_path = os.path.join(output_dir, "indeci_emergencias_2021_2025.csv")
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 70)
    print("  INDECI ETL - Paso 2: Emergencias Hidrometeorologicas")
    print("=" * 70)

    # ------------------------------------------------------------------
    # PARTE A: Daños por Departamento/Provincia (Tabla 1)
    # ------------------------------------------------------------------
    print("\n[1/4] Leyendo Tabla 1: Emergencias y Danos por Dpto/Prov...")

    df_danos = pd.read_excel(
        emergencias_path,
        sheet_name='POR DPTO_PROV',
        header=None,
        engine='openpyxl'
    )

    # La estructura real del Excel:
    # Fila 5 (idx 4): headers nivel 1 (DEPARTAMENTO, EMERGENCIA, DAÑOS...)
    # Fila 6 (idx 5): headers nivel 2 (VIVIENDAS, CULTIVO, etc.)
    # Fila 7 (idx 6): headers nivel 3 (AFECT, DESTR, PERD...)
    # Fila 8 (idx 7): TOTAL
    # Fila 9+ (idx 8+): datos por departamento/provincia

    # Asignar nombres de columna manualmente basándose en la estructura verificada
    col_names = [
        'ubicacion', 'emergencias', 'pers_afect', 'pers_damnif',
        'pers_desap', 'pers_lesion', 'pers_fallec',
        'viv_afect', 'viv_destr',
        'salud_afect', 'salud_destr',
        'cultivo_ha_afect', 'cultivo_ha_perd',
        'puentes_afect', 'puentes_perd',
        'carreteras_km_afect', 'carreteras_km_perd'
    ]

    # Tomar datos desde la fila 8 (idx 7) en adelante
    df_danos = df_danos.iloc[7:].copy()
    df_danos.columns = col_names[:len(df_danos.columns)]
    df_danos = df_danos.reset_index(drop=True)

    # Convertir columnas numéricas
    num_cols = col_names[1:]
    for col in num_cols:
        if col in df_danos.columns:
            df_danos[col] = pd.to_numeric(df_danos[col], errors='coerce').fillna(0)

    # Limpiar ubicacion
    df_danos['ubicacion'] = df_danos['ubicacion'].astype(str).str.strip()
    # Eliminar filas vacías o TOTAL
    df_danos = df_danos[
        (df_danos['ubicacion'] != '') &
        (df_danos['ubicacion'] != 'nan') &
        (~df_danos['ubicacion'].str.upper().str.startswith('TOTAL'))
    ].copy()

    # Identificar departamentos vs provincias
    # Los departamentos son las filas que aparecen antes de sus provincias.
    # En la estructura INDECI, los departamentos suelen estar en mayúsculas
    # y las provincias indentadas o con formato diferente.
    # Recorremos para asignar el departamento padre.
    departamento_actual = None
    records = []

    for _, row in df_danos.iterrows():
        ubic = str(row['ubicacion']).strip()
        if not ubic or ubic == 'nan':
            continue

        # Heurística: si el siguiente registro tiene un valor de emergencias
        # mucho mayor, es un departamento (subtotal). También podemos usar
        # la presencia en una lista conocida.
        # Enfoque más robusto: si la fila es un subtotal departamental,
        # será un nombre de departamento conocido.
        ubic_norm = normalize_geo(ubic)

        # Lista de departamentos peruanos para matching
        dptos_peru = [
            'AMAZONAS', 'ANCASH', 'APURIMAC', 'AREQUIPA', 'AYACUCHO',
            'CAJAMARCA', 'CALLAO', 'CUSCO', 'HUANCAVELICA', 'HUANUCO',
            'ICA', 'JUNIN', 'LA LIBERTAD', 'LAMBAYEQUE',
            'LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'LORETO',
            'MADRE DE DIOS', 'MOQUEGUA', 'PASCO', 'PIURA', 'PUNO',
            'SAN MARTIN', 'TACNA', 'TUMBES', 'UCAYALI', 'LIMA'
        ]

        if ubic_norm in dptos_peru:
            departamento_actual = ubic_norm
            # No registrar el subtotal del departamento; solo provincias
            continue
        elif departamento_actual:
            records.append({
                'departamento': departamento_actual,
                'provincia': ubic_norm,
                'total_emergencias': row.get('emergencias', 0),
                'personas_afectadas': row.get('pers_afect', 0),
                'personas_damnificadas': row.get('pers_damnif', 0),
                'personas_fallecidas': row.get('pers_fallec', 0),
                'cultivo_ha_afectadas': row.get('cultivo_ha_afect', 0),
                'cultivo_ha_perdidas': row.get('cultivo_ha_perd', 0),
                'viviendas_afectadas': row.get('viv_afect', 0),
                'viviendas_destruidas': row.get('viv_destr', 0),
            })

    df_impact = pd.DataFrame(records)
    print(f"       Registros de impacto: {len(df_impact)}")
    print(f"       Departamentos: {df_impact['departamento'].nunique()}")
    print(f"       Provincias: {df_impact['provincia'].nunique()}")

    # ------------------------------------------------------------------
    # PARTE B: Peligros Hidrometeorológicos por Provincia (Tabla 2)
    # ------------------------------------------------------------------
    print("\n[2/4] Leyendo Tabla 2: Peligros por Dpto/Prov...")

    df_pelig = pd.read_excel(
        peligros_path,
        sheet_name='PELIGRO_DPTO_PROV',
        header=None,
        engine='openpyxl'
    )

    # Fila 6 (idx 5): headers con tipos de peligro
    headers_row = df_pelig.iloc[5].tolist()
    headers_clean = [str(h).strip() if h and str(h).strip() != 'nan' else f'col_{i}'
                     for i, h in enumerate(headers_row)]
    headers_clean[0] = 'ubicacion'
    headers_clean[1] = 'total_emergencia'

    df_pelig = df_pelig.iloc[6:].copy()
    df_pelig.columns = headers_clean[:len(df_pelig.columns)]
    df_pelig = df_pelig.reset_index(drop=True)

    # Limpiar
    df_pelig['ubicacion'] = df_pelig['ubicacion'].astype(str).str.strip()
    df_pelig = df_pelig[
        (df_pelig['ubicacion'] != '') &
        (df_pelig['ubicacion'] != 'nan') &
        (~df_pelig['ubicacion'].str.upper().str.startswith('TOTAL'))
    ].copy()

    # Convertir numéricas
    for col in df_pelig.columns[1:]:
        df_pelig[col] = pd.to_numeric(df_pelig[col], errors='coerce').fillna(0)

    # Seleccionar solo peligros hidrometeorológicos
    peligro_cols = [c for c in df_pelig.columns
                    if normalize_geo(c) in [normalize_geo(p) for p in PELIGROS_HIDROMET]]
    print(f"       Columnas hidrometerologicas encontradas: {peligro_cols}")

    # Asignar departamento padre (misma lógica)
    departamento_actual = None
    records_pelig = []
    dptos_peru = [
        'AMAZONAS', 'ANCASH', 'APURIMAC', 'AREQUIPA', 'AYACUCHO',
        'CAJAMARCA', 'CALLAO', 'CUSCO', 'HUANCAVELICA', 'HUANUCO',
        'ICA', 'JUNIN', 'LA LIBERTAD', 'LAMBAYEQUE',
        'LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'LORETO',
        'MADRE DE DIOS', 'MOQUEGUA', 'PASCO', 'PIURA', 'PUNO',
        'SAN MARTIN', 'TACNA', 'TUMBES', 'UCAYALI', 'LIMA'
    ]

    for _, row in df_pelig.iterrows():
        ubic = str(row['ubicacion']).strip()
        if not ubic or ubic == 'nan':
            continue
        ubic_norm = normalize_geo(ubic)

        if ubic_norm in dptos_peru:
            departamento_actual = ubic_norm
            continue
        elif departamento_actual:
            hidromet_total = sum(row.get(c, 0) for c in peligro_cols)
            pelig_detail = {normalize_geo(c): int(row.get(c, 0)) for c in peligro_cols}

            records_pelig.append({
                'departamento': departamento_actual,
                'provincia': ubic_norm,
                'emergencias_hidromet': int(hidromet_total),
                **{f'peligro_{k.lower().replace(" ","_")}': v
                   for k, v in pelig_detail.items()},
            })

    df_peligros = pd.DataFrame(records_pelig)
    print(f"       Registros de peligros: {len(df_peligros)}")

    # ------------------------------------------------------------------
    # PARTE C: Merge de Impacto + Peligros y creación de magnitud_impacto
    # ------------------------------------------------------------------
    print("\n[3/4] Fusionando datos de impacto y peligros...")

    df_merged = pd.merge(
        df_impact,
        df_peligros,
        on=['departamento', 'provincia'],
        how='outer'
    )

    # Rellenar NaN con 0 para las columnas numéricas
    num_cols_merged = df_merged.select_dtypes(include=[np.number]).columns
    df_merged[num_cols_merged] = df_merged[num_cols_merged].fillna(0)

    # Crear magnitud_impacto: combinación ponderada de indicadores clave
    # Fórmula: emergencias_hidromet (conteo) + personas_afectadas/1000
    #          + cultivo_ha_perdidas/100
    df_merged['magnitud_impacto'] = (
        df_merged.get('emergencias_hidromet', 0)
        + df_merged.get('personas_afectadas', 0) / 1000
        + df_merged.get('cultivo_ha_perdidas', 0) / 100
    ).round(2)

    print(f"       Filas fusionadas: {len(df_merged)}")

    # ------------------------------------------------------------------
    # PARTE D: Exportación
    # ------------------------------------------------------------------
    print("\n[4/4] Exportando CSV final...")

    df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')

    # ------------------------------------------------------------------
    # Reporte Final
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("  REPORTE DE VALIDACION - INDECI ETL")
    print("=" * 70)
    print(f"  Filas en CSV resultante     : {len(df_merged):,}")
    print(f"  Departamentos unicos        : {df_merged['departamento'].nunique()}")
    print(f"  Provincias unicas           : {df_merged['provincia'].nunique()}")
    print(f"  Columnas                    : {list(df_merged.columns)}")
    print(f"  Total emergencias hidromet  : {df_merged['emergencias_hidromet'].sum():,.0f}")
    print(f"  Total personas afectadas    : {df_merged['personas_afectadas'].sum():,.0f}")
    print(f"  Total cultivo ha perdidas   : {df_merged['cultivo_ha_perdidas'].sum():,.0f}")
    print(f"  Magnitud impacto (mean)     : {df_merged['magnitud_impacto'].mean():.2f}")
    print(f"  Archivo exportado           : {output_path}")
    print("=" * 70)

    # Top 10 provincias por magnitud de impacto
    print("\n  TOP 10 PROVINCIAS POR MAGNITUD DE IMPACTO:")
    top10 = df_merged.nlargest(10, 'magnitud_impacto')[
        ['departamento', 'provincia', 'magnitud_impacto', 'emergencias_hidromet',
         'cultivo_ha_perdidas']
    ]
    for _, r in top10.iterrows():
        print(f"    {r['departamento']:15s} | {r['provincia']:15s} | "
              f"mag={r['magnitud_impacto']:8.1f} | "
              f"emerg={int(r['emergencias_hidromet']):5d} | "
              f"ha_perd={r['cultivo_ha_perdidas']:10.1f}")

    print("\n  [OK] INDECI ETL completado.\n")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')
    run_etl()
