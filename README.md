# Machine Learning Multimodal (Agro + NLP + Clima)

Este proyecto desarrolla un sistema agnóstico (Multi-crop) de predicción y análisis de la demanda agroindustrial en el Perú. Utiliza una arquitectura multimodal (Agro + Clima + Noticias) basada en modelos de series de tiempo (LSTM-Attention) con NLP (BETO) y algoritmos de explicabilidad (SHAP).

## 🚀 Instalación y Uso

1. **Requisito Previo de Python**:
Este proyecto emplea redes neuronales y NLP pesado, por lo que estricta y obligatoriamente requiere **Python 3.11**. Versiones superiores (como 3.13 o 3.14) no son nativamente compatibles con TensorFlow y librerías clave todavía.
- 🔗 **Descarga de Python 3.11.9 (Windows 64-bit)**: [Haz clic aquí para descargar el instalador oficial](https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe)
*(⚠️ IMPORTANTE: Durante la instalación, en la primera pantalla, asegúrate de marcar con check la opción **"Add python.exe to PATH"**).*

2. **Instalar Dependencias**:
Es necesario instalar las librerías base (pandas, numpy, tensorflow, transformers, etc.) contenidas en el archivo de requirements. Asegúrate de estar en la carpeta raíz y usando tu entorno de Python 3.11 ejecuta en consola:
```bash
pip install -r requirements.txt
```

3. **Ejecutar el Pipeline de Prueba**:
Para inicializar el orquestador y validar la correcta integración de los módulos (Agro, Clima y NLP):
```bash
python main.py
```
*(Nota: El script generará datos falsos de ejemplo "mock data" transitorios si los datasets originales crudos aún no están almacenados en `data/raw/`).*

---

## 📁 Estructura del Código y Alcance de Trabajo

Para evitar cambios disruptivos que afecten la modularidad cruzada, a continuación se delimita qué código base existe y qué tareas precisas corresponden a cada carpeta.

### `data/`
Directorio para el almacenamiento seguro de datos espaciales y tabulares (Excluido de git por peso).
- `/raw`: Para ubicar los datasets primarios y pesados crudos (ej. los 800k de MIDAGRI).
- `/processed`: Carpeta de salida donde se aloja el CSV procesado listo para ingestar a la red LSTM y CSV listos para BETO.
- `/external`: Espacio para archivos constantes o mapeo, como una lista general de Ubigeos.

### Módulos del Core (`src/`)

#### 1. `src/agro/` (Procesamiento Dinámico Agrario)
- **Código Base**: `processor.py` cuenta con una clase orientada a objetos (`AgroProcessor`) que posee filtros elásticos de productos agrícolas ignorando signos y mayúsculas, una remoción de valores atípicos usando el rango intercuartílico (IQR) para Precios y Producción e imprime advertencias si la longitud es menor a 24 meses.
- **Trabajo Futuro**: Solo deberá ampliarse si se introducen reglas comerciales agrarias netamente específicas o lógicas complejas de agrupaciones entre distintas variables MIDAGRI, sin alterar el formato final del output hacia el `FeatureBuilder`.

#### 2. `src/weather/` (Sincronía Meteorológica)
- **Código Base**: `processor.py` aloja `WeatherProcessor` que importa la data de SENAMHI, formatea caracteres distritales problemáticos e integra un rellenado seguro basado en comportamiento histórico o interpolación lineal según de qué volumen sea el déficit.
- **Trabajo Futuro**: Incluir métricas climáticas avanzadas temporales (ej. promedios semanales, eventos anómalos o Fenómeno del Niño) respetando el dataframe base actual.

#### 3. `src/features/` (Integración y Variables Sintéticas)
- **Código Base**: `builder.py` une asimétricamente vía Left-Join y extrae características artificiales de Estacionalidad Cíclica (Seno/Coseno de meses). Asimismo, ensambla retardos en masa o "Lags" `(t-1, t-2, t-3)` a métricas numéricas cruciales.
- **Trabajo Futuro**: Aquí se incorporaría la codificación On-Hot/Embeddings espaciales de ubicaciones o cruce con indicadores de IPC o Macroeconomía antes de alimentar al modelo general. No realizar procesamiento bruto en este archivo, solo creaciones matemáticas de nuevas variables combinadas.

#### 4. `src/scraping/` (Módulo de Procesamiento NLP NLP Ético)
- **Código Base**: `news_scraper.py` implementa interacciones con sitios web mediante cabeceras dinámicas, pausas al azar amigables al sitio (`time.sleep`) y obediencia a registros locales ocultos (`robots.txt`).
- **Trabajo Futuro**: Ajustar o clonar rutinas para abarcar múltiples orígenes (`Andina`, `Agraria.pe`, etc.) y conectar con los pipelines de inferencia para Transformers (BETO) en la asignación del Score del `Sentimiento Base` extraído.

#### 5. `src/models/` (Arquitecturas LSTM-Attention)
- **Código Base**: (Carpeta preparada para inicializar).
- **Trabajo Futuro**: Creación del entorno puramente matemático. La arquitectura de `tensorflow/keras` o librerías predictivas se implementará aquí, en aislamiento. Su entrada será estrictamente el archivo curado de `data/processed/`.

---
### Presentación y Análisis Opcional

- **`notebooks/`**: Uso exclusivo como "Sandbox" donde se pueden desarrollar EDA's informales y gráficas sin desbaratar ni comprometer los ficheros `src/` utilizados en producción.
- **`dashboard/`**: Alojará el "Front-end de Predicción". Scripts (ej. Streamlit o Dash), gráficas e integraciones de caja transparente (XAI, SHAP). No debe alojar lógicas complejas de procesado de dataframe, sino delegarlo al resto de archivos Python subyacentes.

> ⚠️ Regla clave: Todo nuevo archivo a crear en el núcleo para procesamiento debe ser escrito en forma de Clases y empaquetado en sus respectivas carpetas de `src` para favorecer mantenimientos a futuro.
