"""
==========================================================================
Paso 1 del Pipeline: ETL de datos MIDAGRI (SISAGRI) -> Limón 2021-2025
==========================================================================
Lee el Excel original de SISAGRI, filtra por cultivo "LIMON" y rango
temporal 2021-2025, normaliza la geografía, agrega a nivel provincia-mes
y exporta un CSV limpio listo para el EDA y posterior fusión con
variables exógenas (noticias, clima NASA, INDECI).

Salida: data/interim/midagri_limon_procesado.csv
"""


import os
import sys
import unicodedata
import pandas as pd
import numpy as np


# =========================================================================
# Utilidades
# =========================================================================

def strip_accents(text: str) -> str:
    """
    Elimina tildes de un string manteniendo la Ñ intacta.
    Ejemplo: 'ÁNCASH' -> 'ANCASH',  'PIÑAS' -> 'PIÑAS'
    """
    result = []
    for char in unicodedata.normalize('NFD', text):
        # Conservar la Ñ: su descomposición NFD es N + combining tilde (U+0303)
        # Pero solo queremos quitar tildes de vocales, no la tilde de la Ñ.
        # Estrategia: si el carácter es un combining mark Y el carácter anterior
        # no era N/n, lo descartamos.
        if unicodedata.category(char) == 'Mn':
            # Combining mark -> descartar (quita tildes de vocales)
            continue
        result.append(char)
    # Recomponer: la N sin su combining tilde ya no es Ñ,
    # así que usamos un approach diferente: preservar Ñ antes de descomponer.
    return ''.join(result)


def normalize_accents(text: str) -> str:
    """
    Quita tildes de vocales pero preserva la Ñ.
    """
    if not isinstance(text, str):
        return text
    # Paso 1: proteger Ñ/ñ temporalmente
    text = text.replace('Ñ', '##NN##').replace('ñ', '##nn##')
    # Paso 2: quitar combining marks (tildes)
    text = strip_accents(text)
    # Paso 3: restaurar Ñ/ñ
    text = text.replace('##NN##', 'Ñ').replace('##nn##', 'ñ')
    return text


# =========================================================================
# Pipeline ETL
# =========================================================================

def run_etl():
    """Ejecuta el pipeline completo de ETL para datos MIDAGRI de limón."""

    # Rutas
    input_path = os.path.join("data", "raw", "midagri", "Sisagri_2016_2025.xlsx")
    output_dir = os.path.join("data", "interim")
    output_path = os.path.join(output_dir, "midagri_limon_procesado.csv")

    os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # 0. Lectura del Excel
    # ------------------------------------------------------------------
    print("=" * 70)
    print("  MIDAGRI ETL — Paso 1: Extracción de Limón (2021-2025)")
    print("=" * 70)
    print(f"\n[1/5] Leyendo archivo fuente: {input_path}")
    print("       (esto puede tardar ~30s por el tamaño del Excel)")

    df = pd.read_excel(input_path, sheet_name='2021_2025', engine='openpyxl')
    print(f"       Hoja leida: '2021_2025'")
    print(f"       Filas cargadas: {len(df):,}")
    print(f"       Columnas: {list(df.columns)}")

    # ------------------------------------------------------------------
    # 1. Filtrado de Cultivo y Tiempo
    # ------------------------------------------------------------------
    print("\n[2/5] Filtrando por cultivo LIMON y rango 2021-2025...")

    # Normalizar nombre del cultivo para búsqueda flexible
    # NOTA: Se fuerza .astype(str) nativo de Python (object) para evitar
    # incompatibilidades con el backend Arrow de pandas 2.x
    cultivo_raw = df['dsc_Cultivo'].astype(str).str.upper().str.strip()
    # Aplicar normalize_accents convirtiendo a lista Python para evitar Arrow
    cultivo_norm = pd.Series(
        [normalize_accents(x) for x in cultivo_raw],
        index=df.index,
        dtype='object'
    )
    df['_cultivo_norm'] = cultivo_norm
    mask_cultivo = df['_cultivo_norm'].str.contains('LIMON', case=False, na=False)

    # Asegurar que 'anho' sea numérico
    df['anho'] = pd.to_numeric(df['anho'], errors='coerce')
    mask_tiempo = df['anho'].between(2021, 2025)

    df_filtered = df.loc[mask_cultivo & mask_tiempo].copy()
    print(f"       Filas post-filtro: {len(df_filtered):,}")

    if df_filtered.empty:
        print("\n[ERROR] No se encontraron registros de LIMON entre 2021 y 2025.")
        print("        Cultivos únicos disponibles (muestra):")
        print(f"        {df['_cultivo_norm'].unique()[:20]}")
        sys.exit(1)

    # Listar variantes encontradas para transparencia
    variantes = df_filtered['dsc_Cultivo'].unique()
    print(f"       Variantes de cultivo encontradas: {list(variantes)}")

    # ------------------------------------------------------------------
    # 2. Creación de fecha_evento (YYYY-MM)
    # ------------------------------------------------------------------
    print("\n[3/5] Creando columna fecha_evento (YYYY-MM)...")

    df_filtered['mes'] = pd.to_numeric(df_filtered['mes'], errors='coerce').astype('Int64')
    df_filtered['fecha_evento'] = (
        df_filtered['anho'].astype(int).astype(str)
        + '-'
        + df_filtered['mes'].astype(str).str.zfill(2)
    )
    print(f"       Rango temporal: {df_filtered['fecha_evento'].min()} -> {df_filtered['fecha_evento'].max()}")

    # ------------------------------------------------------------------
    # 3. Normalización Geográfica
    # ------------------------------------------------------------------
    print("\n[4/5] Normalizando geografía (Dpto, Prov)...")

    for col in ['Dpto', 'Prov']:
        df_filtered[col] = (
            df_filtered[col]
            .astype(str)
            .str.strip()
            .str.upper()
            .apply(normalize_accents)
        )

    dptos_unicos = df_filtered['Dpto'].nunique()
    provs_unicas = df_filtered['Prov'].nunique()
    print(f"       Departamentos únicos: {dptos_unicos}")
    print(f"       Provincias únicas:    {provs_unicas}")

    # ------------------------------------------------------------------
    # 4. Agrupación a nivel Provincia-Mes
    # ------------------------------------------------------------------
    print("\n[5/5] Agrupando a nivel (fecha_evento, Dpto, Prov)...")

    # Preparar columnas numéricas: reemplazar 0 por NaN en precio para
    # que no arruine el promedio
    df_filtered['PRODUCCION(t)'] = pd.to_numeric(
        df_filtered['PRODUCCION(t)'], errors='coerce'
    )
    # COSECHA (ha) eliminada: varianza cero en SISAGRI 2021-2025
    df_filtered['MTO_PRECCHAC (S/ x kg)'] = pd.to_numeric(
        df_filtered['MTO_PRECCHAC (S/ x kg)'], errors='coerce'
    )
    # Reemplazar ceros por NaN SOLO en precio (un precio de 0 no es real)
    df_filtered.loc[
        df_filtered['MTO_PRECCHAC (S/ x kg)'] == 0,
        'MTO_PRECCHAC (S/ x kg)'
    ] = np.nan

    df_agg = (
        df_filtered
        .groupby(['fecha_evento', 'Dpto', 'Prov'], as_index=False)
        .agg(
            produccion_t=('PRODUCCION(t)', 'sum'),
            precio_chacra_kg=('MTO_PRECCHAC (S/ x kg)', 'mean'),
        )
    )

    # Redondear precio a 2 decimales
    df_agg['precio_chacra_kg'] = df_agg['precio_chacra_kg'].round(2)

    # ------------------------------------------------------------------
    # 5. Renombrado final y Exportación
    # ------------------------------------------------------------------
    # Las columnas ya tienen nombres limpios por la agregación con alias.
    # Renombramos las geográficas para consistencia snake_case.
    df_agg = df_agg.rename(columns={
        'Dpto': 'departamento',
        'Prov': 'provincia',
    })

    df_agg.to_csv(output_path, index=False, encoding='utf-8-sig')

    # ------------------------------------------------------------------
    # Reporte Final
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("  REPORTE DE VALIDACION ETL")
    print("=" * 70)
    print(f"  Filas en CSV resultante : {len(df_agg):,}")
    print(f"  Departamentos unicos    : {df_agg['departamento'].nunique()}")
    print(f"  Provincias unicas       : {df_agg['provincia'].nunique()}")
    print(f"  Rango de fechas         : {df_agg['fecha_evento'].min()} -> {df_agg['fecha_evento'].max()}")
    print(f"  Produccion total (t)    : {df_agg['produccion_t'].sum():,.1f}")
    print(f"  Precio promedio (S//kg) : {df_agg['precio_chacra_kg'].mean():.2f}")
    print(f"  Archivo exportado       : {output_path}")
    print("=" * 70)
    print("\n  [OK] ETL completado. Listo para EDA en el siguiente paso.\n")


if __name__ == "__main__":
    # Windows cp1252 no soporta tildes/emojis en print(); forzar UTF-8
    sys.stdout.reconfigure(encoding='utf-8')
    run_etl()
