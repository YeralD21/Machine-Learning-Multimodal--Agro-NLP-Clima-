# Pipeline NASA POWER — Documentación Técnica

**Proyecto:** Predicción de Producción de Limón en el Perú  
**Modelo:** LSTM-Attention Multimodal  
**Fuente de datos climáticos:** [NASA POWER API](https://power.larc.nasa.gov/)  
**Versión del pipeline:** 1.0  
**Orquestador:** `main_nasa_pipeline.py`

---

## Tabla de Contenidos

1. [Contexto del Proyecto](#1-contexto-del-proyecto)
2. [Estructura de Directorios](#2-estructura-de-directorios)
3. [Variables Climáticas](#3-variables-climáticas)
4. [Diagrama de Flujo del Pipeline](#4-diagrama-de-flujo-del-pipeline)
5. [Pre-paso: Conversión del Formato Crudo NASA (Jupyter)](#5-pre-paso-conversión-del-formato-crudo-nasa-jupyter)
6. [Actividad 1 — Configuración del Proyecto](#6-actividad-1--configuración-del-proyecto)
7. [Actividad 2 — Lectura de Datos](#7-actividad-2--lectura-de-datos)
8. [Actividad 3 — Análisis Exploratorio (EDA)](#8-actividad-3--análisis-exploratorio-eda)
9. [Actividad 4 — Calidad de Datos](#9-actividad-4--calidad-de-datos)
10. [Actividad 5 — Limpieza y Estandarización](#10-actividad-5--limpieza-y-estandarización)
11. [Actividad 6 — Integración y Granularidad](#11-actividad-6--integración-y-granularidad)
12. [Actividad 7 — Diseño del Esquema DWH](#12-actividad-7--diseño-del-esquema-dwh)
13. [Actividad 8 — Carga en PostgreSQL](#13-actividad-8--carga-en-postgresql)
14. [Actividad 9 — Pipeline ETL Completo](#14-actividad-9--pipeline-etl-completo)
15. [Actividad 10 — Reexploración Post-ETL](#15-actividad-10--reexploración-post-etl)
16. [Estructura del Dataset Final](#16-estructura-del-dataset-final)
17. [Cómo Ejecutar el Pipeline](#17-cómo-ejecutar-el-pipeline)
18. [Integración con el Pipeline Principal](#18-integración-con-el-pipeline-principal)

---

## 1. Contexto del Proyecto

Este pipeline forma parte de la tesis **"Predicción de Producción de Limón en el Perú mediante un modelo LSTM-Attention Multimodal"**. Su objetivo es construir el componente de datos climáticos que alimentará al modelo de aprendizaje profundo junto con datos de producción agrícola (MIDAGRI), emergencias (INDECI) y noticias (NLP/Agraria.pe).

| Atributo | Valor |
|---|---|
| Fuente de datos | NASA POWER API (resolución mensual) |
| Ventana temporal | Enero 2021 – Agosto 2025 |
| Cobertura geográfica | 23 departamentos, 101 provincias del Perú |
| Granularidad | Mensual por provincia |
| Dataset de salida | `data/03_processed_nasa/nasa_climatic_processed.csv` |
| Dimensiones finales | 5,656 filas × 13 columnas |
| Orquestador | `main_nasa_pipeline.py` |

La API NASA POWER (Prediction Of Worldwide Energy Resources) provee datos de reanálisis atmosférico derivados del modelo MERRA-2 de la NASA, con cobertura global y resolución espacial de 0.5° × 0.625°.

---

## 2. Estructura de Directorios

```
proyecto/
│
├── main_nasa_pipeline.py                    ← Orquestador del pipeline (10 actividades)
│
├── data/
│   ├── raw/
│   │   ├── nasapowercrudo/                  ← Solo referencia: CSVs crudos descargados de NASA POWER
│   │   │   └── POWER_Point_Monthly_*.csv    ← Formato original con bloque -BEGIN HEADER-
│   │   └── nasapower/                       ← ENTRADA del pipeline: 102 CSVs pre-procesados
│   │       ├── AMAZONAS-BAGUA.csv
│   │       ├── PIURA-SULLANA.csv
│   │       └── ... (102 archivos, uno por provincia)
│   │
│   ├── 02_interim_nasa/                     ← Archivos intermedios generados por el pipeline
│   │   ├── nasa_pipeline_config.json        ← Configuración centralizada (Actividad 1)
│   │   ├── nasa_long_raw.csv                ← Datos en formato LONG sin limpiar (Actividad 2)
│   │   ├── nasa_long_clean.csv              ← Datos limpios y estandarizados (Actividad 5)
│   │   ├── nasa_mensual_integrado.csv       ← Dataset mensual integrado (Actividad 6)
│   │   ├── reporte_lectura.txt              ← Reporte de lectura (Actividad 2)
│   │   ├── reporte_eda_climatico.txt        ← Reporte EDA (Actividad 3)
│   │   ├── reporte_calidad_nasa.txt         ← Reporte de calidad (Actividad 4)
│   │   └── reporte_limpieza_nasa.txt        ← Reporte de limpieza (Actividad 5)
│   │
│   └── 03_processed_nasa/                   ← SALIDA FINAL del pipeline
│       ├── nasa_climatic_processed.csv      ← Dataset escalado para el modelo LSTM
│       ├── nasa_climatic_raw_values.csv     ← Valores originales (para gráficos)
│       └── reports/
│           ├── g1_eda_distribucion.png
│           ├── g2_eda_cobertura.png
│           ├── g3_temperatura_series.png
│           ├── g4_precipitacion_series.png
│           ├── g5_correlacion_clima.png
│           └── g6_estacionalidad_temp.png
│
├── src/data_processing/nasa_pipeline/       ← Scripts de las 10 actividades
│   ├── actividad_01_config_nasa.py
│   ├── actividad_02_lectura_nasa.py
│   ├── actividad_03_eda_nasa.py
│   ├── actividad_04_calidad_nasa.py
│   ├── actividad_05_limpieza_nasa.py
│   ├── actividad_06_granularidad_nasa.py
│   ├── actividad_07_dwh_nasa.py
│   ├── actividad_08_postgresql_nasa.py
│   ├── actividad_09_etl_nasa.py
│   └── actividad_10_reexploracion_nasa.py
│
├── database/
│   └── dwh_nasa_clima_schema.sql            ← DDL PostgreSQL (Actividad 7)
│
└── models/scalers/
    └── scaler_nasa_clima.pkl                ← StandardScaler para transformación inversa
```

---

## 3. Variables Climáticas

El pipeline procesa **8 variables climáticas** de la API NASA POWER, todas con resolución mensual:

| Variable | Descripción | Unidad | Agregación | Fuente MERRA-2 |
|---|---|---|---|---|
| `ALLSKY_SFC_SW_DWN` | Radiación solar superficial (cielo despejado) | MJ/m²/día | Media | CERES SYN1deg |
| `PRECTOTCORR` | Precipitación corregida | mm/día → **mm/mes** | **SUMA** | MERRA-2 |
| `QV2M` | Humedad específica a 2 metros | g/kg | Media | MERRA-2 |
| `RH2M` | Humedad relativa a 2 metros | % | Media | MERRA-2 |
| `T2M` | Temperatura media a 2 metros | °C | Media | MERRA-2 |
| `T2M_MAX` | Temperatura máxima a 2 metros | °C | Media | MERRA-2 |
| `T2M_MIN` | Temperatura mínima a 2 metros | °C | Media | MERRA-2 |
| `WS2M` | Velocidad del viento a 2 metros | m/s | Media | MERRA-2 |

> **Nota sobre PRECTOTCORR:** Es la única variable que se agrega por **SUMA** en lugar de media. Los valores diarios de mm/día se acumulan para obtener el total mensual en mm, que es la unidad relevante para el análisis agrícola.

---

## 4. Diagrama de Flujo del Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PIPELINE NASA POWER — FLUJO COMPLETO                 │
└─────────────────────────────────────────────────────────────────────────┘

  [NASA POWER API]
       │
       │  Descarga manual por provincia
       ▼
  data/raw/nasapowercrudo/          ← CSVs con bloque -BEGIN HEADER-
  POWER_Point_Monthly_*.csv
       │
       │  PRE-PASO (Jupyter — manual, ~102 archivos)
       │  • Detectar fila de encabezado real
       │  • Saltar bloque de metadatos
       │  • Agregar columnas departamento/provincia
       ▼
  data/raw/nasapower/               ← 102 CSVs en formato WIDE limpio
  DEPARTAMENTO-PROVINCIA.csv
       │
       ├──────────────────────────────────────────────────────────────────
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 1 — Configuración                                        │
  │  Genera: nasa_pipeline_config.json                                  │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 2 — Lectura                                              │
  │  102 CSVs WIDE → nasa_long_raw.csv (6,060 filas × 12 cols)         │
  │  Transformación: WIDE (meses como columnas) → LONG (tidy)          │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 3 — EDA                                                  │
  │  Estadísticas descriptivas + cobertura geográfica                  │
  │  Genera: reporte_eda_climatico.txt, g1_*.png, g2_*.png             │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 4 — Calidad                                              │
  │  Auditoría: centinelas -999, rangos físicos, saltos temporales      │
  │  Resultado: 0 outliers, 0 saltos, 101 nulos en ALLSKY               │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 5 — Limpieza y Estandarización  ← CRÍTICA               │
  │  • Estandarización geográfica (MAYÚSCULAS SIN TILDES, protege Ñ)   │
  │  • Imputación: 101 valores (interpolación lineal)                  │
  │  Genera: nasa_long_clean.csv                                        │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 6 — Integración y Granularidad                           │
  │  • Construye clave fecha_evento (YYYY-MM)                           │
  │  • PRECTOTCORR → SUMA mensual; resto → MEDIA                       │
  │  • Filtra ventana 2021-01 a 2025-08 (56 meses × 101 provincias)    │
  │  Genera: nasa_mensual_integrado.csv (5,656 filas × 13 cols)        │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ├──────────────────────────────────────────────────────────────────
       │                                    │
       ▼                                    ▼
  ┌──────────────────────────┐    ┌──────────────────────────────────────┐
  │  ACTIVIDAD 7 — DWH       │    │  ACTIVIDAD 8 — PostgreSQL            │
  │  Diseño Star Schema      │    │  Ejecuta DDL en limon_analytics_db   │
  │  Genera: dwh_nasa_       │    │  Carga dim_clima y actualiza         │
  │  clima_schema.sql        │    │  fact_produccion_limon               │
  └──────────────────────────┘    └──────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 9 — ETL Completo  ← CRÍTICA                             │
  │  • StandardScaler sobre 8 variables climáticas                     │
  │  • Exporta CSV escalado y CSV con valores originales               │
  │  • Guarda scaler_nasa_clima.pkl                                     │
  │  Genera: nasa_climatic_processed.csv (5,656 × 13)                  │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ACTIVIDAD 10 — Reexploración Post-ETL                              │
  │  4 gráficos de validación con valores originales (sin escalar)     │
  │  Genera: g3_*.png, g4_*.png, g5_*.png, g6_*.png                   │
  └─────────────────────────────────────────────────────────────────────┘
       │
       ▼
  data/03_processed_nasa/nasa_climatic_processed.csv
  ← LISTO PARA MERGE CON PIPELINE PRINCIPAL (MIDAGRI + INDECI + NLP)
```

---
## 5. Pre-paso: Conversión del Formato Crudo NASA (Jupyter)

> **Tipo:** Paso manual ejecutado en Jupyter Notebook **antes** de correr el pipeline.  
> **Propósito:** Convertir los archivos crudos de NASA POWER a un formato que pandas pueda leer directamente.

### El problema: formato nativo de NASA POWER

Los archivos descargados directamente de la API NASA POWER contienen un bloque de metadatos con la cabecera `-BEGIN HEADER-` que pandas no puede interpretar como CSV estándar:

```
-BEGIN HEADER-
NASA/POWER Source Native Resolution Monthly and Annual
Dates (month/day/year): 01/01/2021 through 12/31/2025 in LST
Location: Latitude  -5.9   Longitude -78.23
Elevation from MERRA-2: Average for 0.5 x 0.625 degree lat/lon region = 2112.5 meters
The value for missing source data that cannot be computed or is outside of the
sources availability range: -999
Parameter(s):
ALLSKY_SFC_SW_DWN  CERES SYN1deg All Sky Surface Shortwave Downward Irradiance (MJ/m^2/day)
PRECTOTCORR        MERRA-2 Precipitation Corrected (mm/day)
RH2M               MERRA-2 Relative Humidity at 2 Meters (%)
T2M                MERRA-2 Temperature at 2 Meters (C)
T2M_MAX            MERRA-2 Temperature at 2 Meters Maximum (C)
T2M_MIN            MERRA-2 Temperature at 2 Meters Minimum (C)
WS2M               MERRA-2 Wind Speed at 2 Meters (m/s)
-END HEADER-
PARAMETER,YEAR,JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC,ANN
T2M,2021,17.06,18.22,16.64,17.56,17.78,16.73,16.55,17.92,17.9,18.58,17.84,18.14,17.57
T2M,2022,17.43,17.85,17.12,17.23,17.56,16.89,16.71,17.45,18.12,18.34,17.92,17.65,17.69
```

El encabezado real del CSV comienza en la línea que contiene `PARAMETER,YEAR,JAN,FEB,...`, pero su posición varía entre archivos según la cantidad de parámetros descargados.

### Formato de salida (WIDE con departamento/provincia)

Después de la conversión, cada archivo queda en formato **WIDE** con las columnas geográficas al inicio:

```
departamento,provincia,PARAMETER,YEAR,JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC,ANN
PIURA,SULLANA,T2M,2021,26.1,26.3,25.9,25.4,24.8,23.1,22.9,23.5,24.2,25.1,25.8,26.0,24.9
PIURA,SULLANA,PRECTOTCORR,2021,0.12,0.08,0.45,0.02,0.00,0.00,0.00,0.00,0.01,0.03,0.05,0.09,0.07
PIURA,SULLANA,RH2M,2021,68.2,70.1,72.4,65.3,61.8,58.2,55.9,57.3,60.1,63.4,65.2,67.8,63.8
```

### Script de conversión (aplicado a cada archivo)

```python
import pandas as pd

file_name = 'PIURA-SULLANA.csv'  # Reemplazar por cada provincia

# Paso 1: Encontrar la fila del encabezado real
# La línea que contiene "PARAMETER,YEAR,JAN,FEB" marca el inicio del CSV
with open(file_name, 'r') as f:
    lineas = f.readlines()

indice_header = 0
for i, linea in enumerate(lineas):
    if 'PARAMETER,YEAR,JAN,FEB' in linea.replace(" ", ""):
        indice_header = i
        break

# Paso 2: Cargar el CSV saltando el bloque de metadatos
df_clima = pd.read_csv(file_name, skiprows=indice_header)
df_clima.columns = [c.strip() for c in df_clima.columns]

# Paso 3: Agregar columnas geográficas
df_clima['departamento'] = 'PIURA'
df_clima['provincia']    = 'SULLANA'

# Paso 4: Reordenar columnas (dpto y prov al inicio)
cols = ['departamento', 'provincia'] + [
    c for c in df_clima.columns
    if c not in ['departamento', 'provincia']
]
df_clima = df_clima[cols]

# Paso 5: Guardar el archivo convertido
df_clima.to_csv('PIURA-SULLANA.csv', index=False)
```

### Escala del trabajo manual

| Detalle | Valor |
|---|---|
| Archivos procesados | ~102 (uno por provincia) |
| Archivo omitido | `APURIMAC-GRAU.csv` (error de formato) |
| Directorio de entrada | `data/raw/nasapowercrudo/` |
| Directorio de salida | `data/raw/nasapower/` |
| Convención de nombres | `DEPARTAMENTO-PROVINCIA.csv` |

> **Importante:** Este paso es un **prerrequisito** para ejecutar el pipeline. Sin los archivos convertidos en `data/raw/nasapower/`, la Actividad 2 no puede leer los datos.

---

## 6. Actividad 1 — Configuración del Proyecto

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_01_config_nasa.py` |
| **Entrada** | Ninguna (genera configuración desde cero) |
| **Salida** | `data/02_interim_nasa/nasa_pipeline_config.json` |
| **Criticidad** | Alta — falla del pipeline si no se ejecuta |

### Objetivo

Centralizar todos los parámetros del pipeline en un único archivo JSON. Esto garantiza que todas las actividades posteriores usen los mismos valores de configuración sin hardcodear constantes en cada script.

### Parámetros generados

```json
{
  "ANHO_INICIO": 2021,
  "ANHO_FIN": 2025,
  "MES_FIN": 8,
  "NASA_MISSING_VALUE": -999.0,
  "PARAMETROS_NASA": [
    "ALLSKY_SFC_SW_DWN", "PRECTOTCORR", "QV2M",
    "RH2M", "T2M", "T2M_MAX", "T2M_MIN", "WS2M"
  ],
  "PARAMETROS_SUMA": ["PRECTOTCORR"],
  "RANGOS_VALIDOS": {
    "T2M":              [-10.0, 50.0],
    "T2M_MAX":          [ -5.0, 55.0],
    "T2M_MIN":          [-20.0, 40.0],
    "PRECTOTCORR":      [  0.0, 50.0],
    "RH2M":             [  0.0, 100.0],
    "QV2M":             [  0.0, 30.0],
    "ALLSKY_SFC_SW_DWN":[  0.0, 35.0],
    "WS2M":             [  0.0, 20.0]
  },
  "PROVINCIAS_CLAVE": [
    ["PIURA",       "PIURA"],
    ["PIURA",       "SULLANA"],
    ["LAMBAYEQUE",  "LAMBAYEQUE"],
    ["LA LIBERTAD", "VIRU"],
    ["ICA",         "ICA"],
    ["SAN MARTIN",  "SAN MARTIN"]
  ],
  "OUTPUT_FILENAME": "nasa_climatic_processed.csv",
  "DIRS": {
    "raw_nasa_crudo":  "data/raw/nasapowercrudo",
    "raw_nasa_power":  "data/raw/nasapower",
    "interim_nasa":    "data/02_interim_nasa",
    "processed_nasa":  "data/03_processed_nasa",
    "reports_nasa":    "data/03_processed_nasa/reports"
  }
}
```

### Decisiones de diseño

- **`PARAMETROS_SUMA: ["PRECTOTCORR"]`** — La precipitación se acumula mensualmente (suma de mm/día), no se promedia, para obtener el total mensual real.
- **`RANGOS_VALIDOS`** — Rangos físicamente posibles para el Perú, usados en la Actividad 4 para detectar outliers y en la Actividad 5 para clipping.
- **`PROVINCIAS_CLAVE`** — Las 6 provincias más relevantes para la producción de limón, usadas para generar gráficos de validación en las Actividades 3 y 10.
- **`NASA_MISSING_VALUE: -999.0`** — Valor centinela estándar de NASA POWER para datos faltantes o fuera del rango de disponibilidad de la fuente.

---

## 7. Actividad 2 — Lectura de Datos

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_02_lectura_nasa.py` |
| **Entrada** | `data/raw/nasapower/` (102 archivos CSV) |
| **Salidas** | `data/02_interim_nasa/nasa_long_raw.csv`, `reporte_lectura.txt` |
| **Criticidad** | Alta — falla del pipeline si no se ejecuta |

### Objetivo

Leer los 102 archivos CSV pre-procesados y transformarlos del formato **WIDE** (meses como columnas) al formato **LONG** (tidy), donde cada fila representa una combinación única de provincia × año × mes × variable climática.

### Transformación WIDE → LONG

**Antes (formato WIDE):**

```
departamento | provincia | PARAMETER    | YEAR | JAN   | FEB   | ... | DEC   | ANN
PIURA        | SULLANA   | T2M          | 2021 | 26.1  | 26.3  | ... | 26.0  | 25.8
PIURA        | SULLANA   | PRECTOTCORR  | 2021 | 0.12  | 0.08  | ... | 0.09  | 0.07
PIURA        | SULLANA   | RH2M         | 2021 | 68.2  | 70.1  | ... | 67.8  | 63.8
```

**Después (formato LONG / tidy):**

```
DEPARTAMENTO | PROVINCIA | ANIO | MES | T2M  | PRECTOTCORR | RH2M | QV2M | ...
PIURA        | SULLANA   | 2021 |  1  | 26.1 | 0.12        | 68.2 | 12.4 | ...
PIURA        | SULLANA   | 2021 |  2  | 26.3 | 0.08        | 70.1 | 12.8 | ...
PIURA        | SULLANA   | 2021 |  3  | 25.9 | 0.45        | 72.4 | 13.1 | ...
```

### Función principal: `parse_nasa_wide_csv()`

El proceso de transformación sigue estos pasos para cada archivo:

1. **Carga del CSV** — Intenta UTF-8 primero; si falla, usa latin1 como fallback.
2. **Extracción geográfica** — Lee `departamento` y `provincia` de las columnas del CSV.
3. **Filtrado de parámetros** — Conserva solo las 8 variables definidas en `PARAMETROS_NASA`.
4. **Melt de meses** — Convierte las columnas `JAN`, `FEB`, ..., `DEC` en filas (descarta `ANN`).
5. **Reemplazo del centinela** — Sustituye `-999.0` por `NaN`.
6. **Pivot de PARAMETER** — Convierte los valores de la columna `PARAMETER` en columnas individuales.

### Resultados de la lectura

```
Archivos leídos      : 101 de 102
Filas totales        : 6,060
Departamentos        : 23
Provincias           : 101
Rango temporal       : 2021 → 2025
Columnas climáticas  : 8 variables
```

> **Archivo omitido:** `APURIMAC-GRAU.csv` — presentó un error de formato durante la conversión manual en el pre-paso. Las 4 provincias restantes de Apurímac (Abancay, Andahuaylas, Aymaraes, Chincheros) sí fueron procesadas correctamente.

### Cobertura geográfica resultante

| Departamento | Provincias incluidas |
|---|---|
| AMAZONAS | BAGUA, CHACHAPOYAS, LUYA, UTCUBAMBA |
| ANCASH | CASMA, HUARMEY, HUAYLAS, SANTA |
| APURIMAC | ABANCAY, ANDAHUAYLAS, AYMARAES, CHINCHEROS |
| AREQUIPA | CARAVELI, LA UNION |
| AYACUCHO | HUAMANGA, HUANTA, LA MAR, LUCANAS, PARINACOCHAS, PAUCAR DEL SARA SARA, SUCRE, VILCAS HUAMAN |
| CAJAMARCA | CAJABAMBA, CHOTA, CUTERVO, JAEN, SAN IGNACIO, SAN MARCOS, SAN MIGUEL, SANTA CRUZ |
| CUSCO | LA CONVENCION |
| HUANCAVELICA | ACOBAMBA, ANGARES, CASTROVIRREYNA, CHURCAMPA, HUAYTARA, TAYACAJA |
| HUANUCO | HUAMALIES, HUANUCO, HUAYCABAMBA, LEONCIO PRADO, MARANON, PUERTO INCA |
| ICA | CHINCHA, ICA, NAZCA, PALPA, PISCO |
| JUNIN | CHANCHAMAYO, HUANCAYO, SATIPO |
| LA LIBERTAD | BOLIVAR, CHEPEN, GRAN CHIMU, OTUZCO, PATAZ, SANCHEZ CARRION, VIRU |
| LAMBAYEQUE | FERRENAFE, LAMBAYEQUE |
| LIMA | HUAURA |
| LORETO | ALTO AMAZONAS, DATEM DEL MARANON, LORETO, MARISCAL RAMON CASTILLA, MAYNAS, PUTUMAYO, REQUENA, UCAYALI |
| MADRE DE DIOS | MANU, TAHUAMANU, TAMBOPATA |
| MOQUEGUA | GENERAL SANCHEZ CERRO, MARISCAL NIETO |
| PASCO | OXAPAMPA |
| PIURA | AYABACA, HUANCABAMBA, MORROPON, PAITA, PIURA, SECHURA, SULLANA |
| PUNO | CARABAYA, SANDIA |
| SAN MARTIN | BELLAVISTA, EL DORADO, HUALLAGA, LAMAS, MARISCAL CACERES, MOYOBAMBA, PICOTA, RIOJA, SAN MARTIN, TOCACHE |
| TUMBES | CONTRALMIRANTE VILLAR, TUMBES, ZARUMILLA |
| UCAYALI | ATALAYA, CORONEL PORTILLO, PADRE ABAD, PURUS |

---

## 8. Actividad 3 — Análisis Exploratorio (EDA)

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_03_eda_nasa.py` |
| **Entrada** | `data/02_interim_nasa/nasa_long_raw.csv` |
| **Salidas** | `reporte_eda_climatico.txt`, `g1_eda_distribucion.png`, `g2_eda_cobertura.png` |

### Objetivo

Generar estadísticas descriptivas de las 8 variables climáticas y verificar la cobertura geográfica y temporal del dataset antes de la limpieza.

### Estadísticas descriptivas

| Variable | N válidos | N nulos | Media | Mediana | Std | Mín | Máx |
|---|---|---|---|---|---|---|---|
| ALLSKY_SFC_SW_DWN | 5,959 | **101** | 17.83 | 17.30 | 3.07 | 9.22 | 30.86 |
| PRECTOTCORR | 6,060 | 0 | 1.83 | 0.89 | 2.48 | 0.00 | 23.23 |
| QV2M | 6,060 | 0 | 11.56 | 11.21 | 3.58 | 1.84 | 20.37 |
| RH2M | 6,060 | 0 | 72.14 | 73.02 | 11.79 | 22.69 | 93.92 |
| T2M | 6,060 | 0 | 18.45 | 19.15 | 6.52 | 3.42 | 33.66 |
| T2M_MAX | 6,060 | 0 | 27.03 | 27.49 | 5.98 | 13.39 | 42.40 |
| T2M_MIN | 6,060 | 0 | 11.80 | 13.23 | 7.63 | -5.93 | 25.51 |
| WS2M | 6,060 | 0 | 1.63 | 1.80 | 1.17 | 0.01 | 5.36 |

### Hallazgos clave del EDA

- **Amplitud térmica:** T2M varía de 3.42°C a 33.66°C, reflejando la diversidad geográfica del Perú (costa, sierra y selva).
- **Precipitación:** PRECTOTCORR va de 0 a 23.23 mm/día, con mediana de 0.89 mm/día, indicando alta asimetría positiva (muchas provincias costeras con precipitación casi nula).
- **Humedad relativa:** RH2M entre 22.69% y 93.92%, con media de 72.14%, coherente con la variedad climática peruana.
- **101 nulos en ALLSKY_SFC_SW_DWN:** Corresponden a los meses futuros (septiembre–diciembre 2025) para los que NASA POWER aún no tiene datos disponibles. Serán imputados en la Actividad 5.
- **Completitud temporal:** Todas las 101 provincias tienen 60 meses de datos (100% de completitud para el período 2021–2025).

### Gráficos generados

| Archivo | Contenido |
|---|---|
| `g1_eda_distribucion.png` | Histogramas de distribución para las 8 variables climáticas |
| `g2_eda_cobertura.png` | Mapa de cobertura geográfica por departamento |

---
## 9. Actividad 4 — Calidad de Datos

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_04_calidad_nasa.py` |
| **Entrada** | `data/02_interim_nasa/nasa_long_raw.csv` |
| **Salida** | `data/02_interim_nasa/reporte_calidad_nasa.txt` |

### Objetivo

Realizar una auditoría exhaustiva de la calidad de los datos antes de la limpieza, verificando cuatro tipos de problemas:

1. **Valores centinela residuales** — Detectar si quedaron valores `-999.0` sin convertir a `NaN`.
2. **Violaciones de rangos físicos** — Identificar valores fuera de los límites físicamente posibles para cada variable.
3. **Saltos temporales** — Verificar que cada provincia tenga una serie temporal continua sin meses faltantes.
4. **Nulos** — Cuantificar y localizar valores nulos por variable.

### Rangos de validación física

| Variable | Mínimo | Máximo | Justificación |
|---|---|---|---|
| T2M | -10°C | 50°C | Rango climático extremo para el Perú |
| T2M_MAX | -5°C | 55°C | Temperatura máxima diaria extrema |
| T2M_MIN | -20°C | 40°C | Temperatura mínima diaria extrema |
| PRECTOTCORR | 0 mm/día | 50 mm/día | Precipitación máxima diaria registrada en Perú |
| RH2M | 0% | 100% | Rango físico absoluto de humedad relativa |
| QV2M | 0 g/kg | 30 g/kg | Rango de humedad específica en tropósfera baja |
| ALLSKY_SFC_SW_DWN | 0 MJ/m²/día | 35 MJ/m²/día | Radiación solar máxima en latitudes tropicales |
| WS2M | 0 m/s | 20 m/s | Velocidad de viento máxima a 2m en condiciones normales |

### Resultados de la auditoría

```
Total filas analizadas : 6,060
Variables analizadas   : 8
Outliers detectados    : 0
Provincias con saltos  : 0
Valores centinela -999 : 0

NULOS POR VARIABLE:
  ALLSKY_SFC_SW_DWN    101  (1.7%)
  PRECTOTCORR            0  (0.0%)
  QV2M                   0  (0.0%)
  RH2M                   0  (0.0%)
  T2M                    0  (0.0%)
  T2M_MAX                0  (0.0%)
  T2M_MIN                0  (0.0%)
  WS2M                   0  (0.0%)
```

> **Conclusión:** Los datos de NASA POWER son de alta calidad. Los únicos valores faltantes son los 101 nulos en `ALLSKY_SFC_SW_DWN` correspondientes a meses futuros (sep–dic 2025), que serán imputados en la Actividad 5.

---

## 10. Actividad 5 — Limpieza y Estandarización

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_05_limpieza_nasa.py` |
| **Entrada** | `data/02_interim_nasa/nasa_long_raw.csv` |
| **Salidas** | `data/02_interim_nasa/nasa_long_clean.csv`, `reporte_limpieza_nasa.txt` |
| **Criticidad** | Alta — falla del pipeline si no se ejecuta |

### Objetivo

Aplicar cuatro transformaciones de limpieza para garantizar la consistencia del dataset antes de la integración con las demás fuentes de datos.

### Paso 1: Estandarización geográfica

Convierte todos los nombres de departamentos y provincias a **MAYÚSCULAS SIN TILDES**, con protección especial para la letra Ñ:

```python
# Regla de estandarización
nombre = nombre.upper()           # Todo en mayúsculas
nombre = nombre.replace('Á', 'A') # Eliminar tildes
nombre = nombre.replace('É', 'E')
nombre = nombre.replace('Í', 'I')
nombre = nombre.replace('Ó', 'O')
nombre = nombre.replace('Ú', 'U')
# La Ñ se CONSERVA (no se convierte a N)
```

**Verificaciones críticas post-estandarización:**

| Nombre original | Nombre estandarizado | Estado |
|---|---|---|
| JUNÍN | JUNIN | ✅ |
| PIURA | PIURA | ✅ |
| LA LIBERTAD | LA LIBERTAD | ✅ |
| SAN MARTÍN | SAN MARTIN | ✅ |
| MADRE DE DIOS | MADRE DE DIOS | ✅ |

> **Por qué es crítico:** La clave de merge con el pipeline principal (MIDAGRI) usa `DEPARTAMENTO + PROVINCIA + fecha_evento`. Si los nombres no coinciden exactamente, el JOIN producirá nulos en lugar de unir los datos correctamente.

### Paso 2: Reemplazo de centinelas residuales

Aunque la Actividad 4 confirmó 0 centinelas, se aplica el reemplazo como medida de seguridad:

```python
df[col] = df[col].replace(-999.0, np.nan)
```

### Paso 3: Clipping de outliers físicos

Recorta valores fuera de los rangos válidos definidos en la configuración:

```python
df[col] = df[col].clip(lower=rango_min, upper=rango_max)
```

### Paso 4: Imputación de valores nulos

Estrategia de imputación en dos niveles:

- **≤ 3 nulos consecutivos** → Interpolación lineal (preserva la tendencia temporal).
- **> 3 nulos consecutivos** → Media histórica mensual (promedio del mismo mes en otros años).

### Resultados de la limpieza

```
Filas procesadas          : 6,060
Valores centinela (-999)  : 0
Valores clipeados (rango) : 0
Valores imputados         : 101

NULOS RESIDUALES POST-LIMPIEZA:
  Todas las variables      : 0
```

Los 101 valores imputados corresponden exactamente a los nulos de `ALLSKY_SFC_SW_DWN` para los meses futuros (sep–dic 2025) de las 101 provincias. Al ser ≤ 4 meses consecutivos por provincia, se aplicó interpolación lineal.

---

## 11. Actividad 6 — Integración y Granularidad

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_06_granularidad_nasa.py` |
| **Entrada** | `data/02_interim_nasa/nasa_long_clean.csv` |
| **Salida** | `data/02_interim_nasa/nasa_mensual_integrado.csv` (5,656 filas × 13 columnas) |

### Objetivo

Verificar la granularidad mensual, construir la clave temporal `fecha_evento`, aplicar la agregación correcta por variable y filtrar el dataset a la ventana temporal del proyecto.

### Decisiones de agregación

| Variable | Agregación | Justificación |
|---|---|---|
| PRECTOTCORR | **SUMA** | Acumula mm/día → total mensual en mm |
| Todas las demás | **MEDIA** | Promedio mensual del valor diario |

### Construcción de la clave temporal

```python
# Formato YYYY-MM, compatible con el pipeline MIDAGRI
df['fecha_evento'] = df['ANIO'].astype(str) + '-' + df['MES'].astype(str).str.zfill(2)
# Ejemplo: 2021-01, 2021-02, ..., 2025-08
```

### Filtrado de la ventana temporal

```python
# Ventana del proyecto: enero 2021 a agosto 2025
df = df[(df['fecha_evento'] >= '2021-01') & (df['fecha_evento'] <= '2025-08')]
```

Esto resulta en **56 meses** por provincia (enero 2021 a agosto 2025 inclusive).

### Verificación de completitud

```
101 provincias × 56 meses = 5,656 filas esperadas
5,656 filas obtenidas → 100% de completitud
Todas las provincias tienen serie completa (56/56 meses)
```

### Clave de merge con el pipeline principal

```
DEPARTAMENTO + PROVINCIA + fecha_evento
```

Esta clave es compatible con la usada en `actividad_06_07_integracion_dwh.py` del pipeline MIDAGRI.

### Estructura del archivo de salida

```
DEPARTAMENTO | PROVINCIA | ANIO | MES | fecha_evento | ALLSKY_SFC_SW_DWN | PRECTOTCORR | QV2M | RH2M | T2M | T2M_MAX | T2M_MIN | WS2M
AMAZONAS     | BAGUA     | 2021 |  1  | 2021-01      | 14.94             | 28.83       | ...  | ...  | ... | ...     | ...     | ...
```

---

## 12. Actividad 7 — Diseño del Esquema DWH

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_07_dwh_nasa.py` |
| **Entrada** | Ninguna (genera DDL desde cero) |
| **Salida** | `database/dwh_nasa_clima_schema.sql` |

### Objetivo

Diseñar la extensión del Star Schema existente en `limon_analytics_db` para incorporar las variables climáticas de NASA POWER.

### Dos opciones de implementación

#### Opción A — Columnas directas en `fact_produccion_limon` (Recomendada para LSTM)

Agrega las 8 columnas climáticas directamente a la tabla de hechos. Ventaja: un solo JOIN para obtener todos los features del modelo.

```sql
ALTER TABLE fact_produccion_limon
    ADD COLUMN IF NOT EXISTS allsky_sfc_sw_dwn  FLOAT,  -- Radiación solar (MJ/m²/día)
    ADD COLUMN IF NOT EXISTS prectotcorr         FLOAT,  -- Precipitación corregida (mm/mes)
    ADD COLUMN IF NOT EXISTS qv2m                FLOAT,  -- Humedad específica a 2m (g/kg)
    ADD COLUMN IF NOT EXISTS rh2m                FLOAT,  -- Humedad relativa a 2m (%)
    ADD COLUMN IF NOT EXISTS t2m                 FLOAT,  -- Temperatura media a 2m (°C)
    ADD COLUMN IF NOT EXISTS t2m_max             FLOAT,  -- Temperatura máxima a 2m (°C)
    ADD COLUMN IF NOT EXISTS t2m_min             FLOAT,  -- Temperatura mínima a 2m (°C)
    ADD COLUMN IF NOT EXISTS ws2m                FLOAT;  -- Velocidad del viento a 2m (m/s)
```

#### Opción B — Tabla `dim_clima` separada (Para análisis climático independiente)

```sql
CREATE TABLE IF NOT EXISTS dim_clima (
    id_clima             SERIAL PRIMARY KEY,
    id_tiempo            INT    NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion         INT    NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
    allsky_sfc_sw_dwn    FLOAT,
    prectotcorr          FLOAT,
    qv2m                 FLOAT,
    rh2m                 FLOAT,
    t2m                  FLOAT,
    t2m_max              FLOAT,
    t2m_min              FLOAT,
    ws2m                 FLOAT,
    fuente               VARCHAR(20) DEFAULT 'NASA_POWER',
    fecha_carga          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id_tiempo, id_ubicacion)
);
```

### Star Schema extendido

```
                    ┌─────────────────┐
                    │   dim_tiempo    │
                    │  id_tiempo (PK) │
                    │  fecha_evento   │
                    │  anho, mes      │
                    │  trimestre      │
                    │  month_sin/cos  │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_ubicacion  │          │          │    dim_clima     │
│ id_ubicacion(PK)│          │          │  id_clima (PK)  │
│  departamento   │          │          │  id_tiempo (FK) │
│  provincia      │          │          │  id_ubicacion(FK│
│  lat, lon       │          │          │  allsky_sfc_...  │
└────────┬────────┘          │          │  prectotcorr    │
         │                   │          │  t2m, t2m_max   │
         │         ┌─────────┴──────────┤  t2m_min, rh2m  │
         └────────►│ fact_produccion_   │  qv2m, ws2m     │
                   │      limon         └─────────────────┘
                   │  id_tiempo (FK)
                   │  id_ubicacion (FK)
                   │  [MIDAGRI]
                   │    produccion_t
                   │    cosecha_ha
                   │    precio_chacra_kg
                   │  [INDECI]
                   │    num_emergencias
                   │    total_afectados
                   │  [NLP]
                   │    n_noticias
                   │    avg_sentimiento
                   │  [NASA — Opción A]
                   │    allsky_sfc_sw_dwn
                   │    prectotcorr
                   │    t2m, t2m_max
                   │    t2m_min, rh2m
                   │    qv2m, ws2m
                   └────────────────────
```

### Vista unificada para el modelo LSTM

```sql
CREATE OR REPLACE VIEW v_lstm_features AS
SELECT
    dt.fecha_evento, dt.anho, dt.mes, dt.trimestre,
    dt.month_sin, dt.month_cos,
    du.departamento, du.provincia, du.lat, du.lon,
    -- Métricas agrícolas (MIDAGRI)
    f.produccion_t, f.cosecha_ha, f.precio_chacra_kg,
    -- Métricas de emergencias (INDECI)
    f.num_emergencias, f.total_afectados, f.has_cultivo_perdidas,
    -- Métricas NLP (Agraria.pe)
    f.n_noticias, f.avg_sentimiento,
    -- Métricas climáticas (NASA POWER)
    f.allsky_sfc_sw_dwn, f.prectotcorr, f.qv2m,
    f.rh2m, f.t2m, f.t2m_max, f.t2m_min, f.ws2m
FROM fact_produccion_limon f
JOIN dim_tiempo    dt ON f.id_tiempo    = dt.id_tiempo
JOIN dim_ubicacion du ON f.id_ubicacion = du.id_ubicacion
ORDER BY dt.fecha_evento, du.departamento, du.provincia;
```

---

## 13. Actividad 8 — Carga en PostgreSQL

| Atributo | Valor |
|---|---|
| **Script** | `src/data_processing/nasa_pipeline/actividad_08_postgresql_nasa.py` |
| **Entradas** | `nasa_mensual_integrado.csv`, `dwh_nasa_clima_schema.sql` |
| **Base de datos** | `limon_analytics_db` |
| **Conexión** | `postgresql://postgres:postgres@localhost:5432/limon_analytics_db` |

### Objetivo

Ejecutar el DDL generado en la Actividad 7 y cargar los datos climáticos en la base de datos PostgreSQL del proyecto.

### Prerrequisitos

- PostgreSQL corriendo localmente en el puerto 5432.
- Base de datos `limon_analytics_db` existente con las tablas del pipeline principal ya creadas (`fact_produccion_limon`, `dim_tiempo`, `dim_ubicacion`).

### Pasos de ejecución

1. **Ejecutar ALTER TABLE** — Agrega las 8 columnas climáticas a `fact_produccion_limon`.
2. **Crear `dim_clima`** — Ejecuta el DDL de la tabla de dimensión climática.
3. **Cargar datos en `dim_clima`** — Inserta los registros de `nasa_mensual_integrado.csv` haciendo JOIN con `dim_tiempo` y `dim_ubicacion` para obtener los IDs correspondientes.
4. **Actualizar `fact_produccion_limon`** — Ejecuta un UPDATE con JOIN para poblar las columnas climáticas en la tabla de hechos.

```sql
-- Ejemplo del UPDATE de carga (simplificado)
UPDATE fact_produccion_limon f
SET
    allsky_sfc_sw_dwn = c.allsky_sfc_sw_dwn,
    prectotcorr       = c.prectotcorr,
    t2m               = c.t2m,
    -- ... resto de variables
FROM dim_clima c
WHERE f.id_tiempo    = c.id_tiempo
  AND f.id_ubicacion = c.id_ubicacion;
```

> **Nota:** Esta actividad es opcional si solo se necesita el CSV para el modelo LSTM. Es relevante para análisis en SQL o para conectar herramientas de BI al DWH.

---
