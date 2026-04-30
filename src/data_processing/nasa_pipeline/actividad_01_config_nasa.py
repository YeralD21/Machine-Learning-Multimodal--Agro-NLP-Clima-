"""
==========================================================================
NASA POWER Pipeline — Actividad 1: Configuración del Proyecto
==========================================================================
Genera el archivo de configuración nasa_pipeline_config.json con todas
las rutas, parámetros y constantes que usarán las demás actividades.

Salida: data/02_interim_nasa/nasa_pipeline_config.json
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 1: Configuración del Proyecto')
print('=' * 70)

# ─── Directorios del pipeline NASA ───────────────────────────────────────
DIRS = {
    # Referencia: CSVs crudos de NASA POWER (solo para documentación del proceso
    # de conversión — NO se usan como entrada del pipeline de análisis)
    "raw_nasa_crudo":   "data/raw/nasapowercrudo",
    # Entrada real del pipeline: CSVs ya ordenados con columnas departamento/provincia
    # Generados manualmente en Jupyter aplicando el proceso de conversión documentado
    # en la Actividad 2 (detectar header, skiprows, agregar dpto/prov, guardar)
    "raw_nasa_power":   "data/raw/nasapower",
    # Intermedios: datos limpios y pivoteados antes del ETL final
    "interim_nasa":     "data/02_interim_nasa",
    # Salida final: dataset mensualizado listo para merge con MIDAGRI
    "processed_nasa":   "data/03_processed_nasa",
    # Reportes y gráficos de validación
    "reports_nasa":     "data/03_processed_nasa/reports",
}

# ─── Parámetros climáticos que se extraen de la NASA ─────────────────────
# QV2M: Humedad específica a 2m (g/kg) — proxy de vapor de agua
PARAMETROS_NASA = [
    "ALLSKY_SFC_SW_DWN",   # Radiación solar (MJ/m²/día)
    "PRECTOTCORR",          # Precipitación corregida (mm/día)
    "QV2M",                 # Humedad específica a 2m (g/kg)
    "RH2M",                 # Humedad relativa a 2m (%)
    "T2M",                  # Temperatura media a 2m (°C)
    "T2M_MAX",              # Temperatura máxima a 2m (°C)
    "T2M_MIN",              # Temperatura mínima a 2m (°C)
    "WS2M",                 # Velocidad del viento a 2m (m/s)
]

# ─── Parámetros que se suman (en vez de promediar) al mensualizar ─────────
# La precipitación de la NASA viene en mm/día → al mensualizar se SUMA
# para obtener mm/mes total. El resto se promedia.
PARAMETROS_SUMA = ["PRECTOTCORR"]

# ─── Ventana temporal del proyecto ───────────────────────────────────────
ANHO_INICIO = 2021
ANHO_FIN    = 2025
MES_FIN     = 8   # Agosto 2025 (último mes con data completa)

# ─── Valor centinela de la NASA para datos faltantes ─────────────────────
NASA_MISSING_VALUE = -999.0

# ─── Rangos de validación por variable (para detección de outliers) ───────
RANGOS_VALIDOS = {
    "T2M":              (-10.0,  50.0),   # °C — Perú: costa/sierra/selva
    "T2M_MAX":          (-5.0,   55.0),   # °C
    "T2M_MIN":          (-20.0,  40.0),   # °C
    "PRECTOTCORR":      (0.0,    50.0),   # mm/día — máx histórico Perú ~40mm/día
    "RH2M":             (0.0,   100.0),   # %
    "QV2M":             (0.0,    30.0),   # g/kg
    "ALLSKY_SFC_SW_DWN":(0.0,    35.0),   # MJ/m²/día
    "WS2M":             (0.0,    20.0),   # m/s
}

# ─── Provincias productoras clave para gráficos de validación ─────────────
PROVINCIAS_CLAVE = [
    ("PIURA",       "PIURA"),
    ("PIURA",       "SULLANA"),
    ("LAMBAYEQUE",  "LAMBAYEQUE"),
    ("LA LIBERTAD", "VIRU"),
    ("ICA",         "ICA"),
    ("SAN MARTIN",  "SAN MARTIN"),
]

# ─── Nombre del archivo de salida final ───────────────────────────────────
OUTPUT_FILENAME = "nasa_climatic_processed.csv"

# ─── Ensamblaje del config ────────────────────────────────────────────────
CONFIG = {
    "ANHO_INICIO":          ANHO_INICIO,
    "ANHO_FIN":             ANHO_FIN,
    "MES_FIN":              MES_FIN,
    "NASA_MISSING_VALUE":   NASA_MISSING_VALUE,
    "PARAMETROS_NASA":      PARAMETROS_NASA,
    "PARAMETROS_SUMA":      PARAMETROS_SUMA,
    "RANGOS_VALIDOS":       RANGOS_VALIDOS,
    "PROVINCIAS_CLAVE":     PROVINCIAS_CLAVE,
    "OUTPUT_FILENAME":      OUTPUT_FILENAME,
    "DIRS":                 DIRS,
}

# ─── Crear directorios si no existen ─────────────────────────────────────
for nombre, ruta in DIRS.items():
    os.makedirs(ruta, exist_ok=True)
    print(f'  [OK] Directorio asegurado: {ruta}')

# ─── Guardar config ───────────────────────────────────────────────────────
config_path = os.path.join(DIRS["interim_nasa"], "nasa_pipeline_config.json")
with open(config_path, 'w', encoding='utf-8') as f:
    json.dump(CONFIG, f, indent=2, ensure_ascii=False)

print()
print(f'  [OK] Configuración guardada en: {config_path}')
print()
print('  Resumen de configuración:')
print(f'    Ventana temporal  : {ANHO_INICIO} → {ANHO_FIN} (hasta mes {MES_FIN})')
print(f'    Parámetros NASA   : {len(PARAMETROS_NASA)} variables climáticas')
print(f'    Valor faltante    : {NASA_MISSING_VALUE}')
print(f'    Provincias clave  : {len(PROVINCIAS_CLAVE)} para validación')
print(f'    Archivo de salida : {OUTPUT_FILENAME}')
print()
print('[ACTIVIDAD 1] COMPLETADA.')
print('  Descripción: Configuración del pipeline NASA POWER.')
print(f'  Config guardado en: {config_path}')
