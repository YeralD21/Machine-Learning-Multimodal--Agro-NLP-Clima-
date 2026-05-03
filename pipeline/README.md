# Pipeline de Ingeniería de Datos — Fase 1
## Predicción de Producción de Limón en el Perú
### Modelo LSTM-Attention Multimodal (Agro + Clima + NLP)

---

## Estructura

```
pipeline/
│
├── actividad_01_configuracion.ipynb      ← Entorno, rutas, pipeline_config.json
├── actividad_02_lectura_datos.ipynb      ← Lectura de las 4 fuentes con pandas
├── actividad_03_eda.ipynb                ← Análisis Exploratorio de Datos
├── actividad_04_calidad.ipynb            ← Auditoría de calidad de datos
├── actividad_05_limpieza.ipynb           ← Limpieza y estandarización
├── actividad_06_integracion_dwh.ipynb    ← Integración multimodal al DWH
├── actividad_07_dwh_schema.ipynb         ← Diseño Star Schema + UML + DDL SQL
├── actividad_08_postgresql.ipynb         ← Creación del esquema en PostgreSQL
├── actividad_09_etl.ipynb                ← Pipeline ETL completo
├── actividad_10_reexploracion.ipynb      ← Reexploración post-ETL
│
├── config/
│   └── pipeline_config.json              ← Generado por Actividad 1
│
└── output/
    ├── 02_lectura/                        ← CSVs raw de cada fuente
    ├── 03_eda/                            ← Gráficos EDA
    ├── 04_calidad/                        ← Reportes de calidad
    ├── 05_limpieza/                       ← Datasets limpios por fuente
    ├── 06_integracion/                    ← dataset_integrado.csv
    ├── 07_dwh/                            ← UML + DDL SQL
    ├── 09_etl/                            ← Dataset maestro final + scaler
    └── 10_reexploracion/                  ← Gráficos y reporte final
```

---

## Fuentes de Datos

| Fuente | Archivo(s) | Ubicación |
|--------|-----------|-----------|
| MIDAGRI | `Sisagri_2016_2025.xlsx` | `sources/midagri/` |
| INDECI | `resumen_emergencias_2003_2024.xlsx`, `resumen_peligros_2003_2024.xlsx`, shapefiles E_2021/2022/2023 | `sources/indeci/` |
| NASA POWER | 102 CSVs por provincia (WIDE format) | `sources/nasa/nasapower/` |
| Agraria.pe | `agro_news_2021.csv` → `agro_news_2025.csv` | `sources/agraria-pe/sin-unificar/` |

---

## Orden de Ejecución

Ejecutar los notebooks **en orden secuencial**. Cada actividad depende de las salidas de la anterior.

```
01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 10
```

> **Nota:** La Actividad 8 requiere PostgreSQL corriendo localmente.  
> Las demás actividades son independientes de la base de datos.

---

## Dataset Final

El dataset maestro generado en la Actividad 9 tiene la siguiente estructura:

| Columna | Fuente | Descripción |
|---------|--------|-------------|
| `fecha_evento` | — | Llave temporal (YYYY-MM) |
| `departamento` | — | Llave geográfica |
| `provincia` | — | Llave geográfica |
| `produccion_t` | MIDAGRI | Producción de limón en toneladas |
| `cosecha_ha` | MIDAGRI | Hectáreas cosechadas |
| `precio_chacra_kg` | MIDAGRI | Precio en chacra (S/./kg) |
| `num_emergencias` | INDECI | Número de emergencias en el mes |
| `total_afectados` | INDECI | Personas afectadas |
| `has_cultivo_perdidas` | INDECI | Hectáreas de cultivo perdidas |
| `T2M` | NASA | Temperatura media (°C) |
| `T2M_MAX` | NASA | Temperatura máxima (°C) |
| `T2M_MIN` | NASA | Temperatura mínima (°C) |
| `PRECTOTCORR` | NASA | Precipitación total mensual (mm/mes) |
| `RH2M` | NASA | Humedad relativa (%) |
| `QV2M` | NASA | Humedad específica (g/kg) |
| `ALLSKY_SFC_SW_DWN` | NASA | Radiación solar (MJ/m²/día) |
| `WS2M` | NASA | Velocidad del viento (m/s) |
| `n_noticias` | Agraria.pe | Número de noticias del mes |
| `month_sin` / `month_cos` | — | Codificación cíclica de estacionalidad |
