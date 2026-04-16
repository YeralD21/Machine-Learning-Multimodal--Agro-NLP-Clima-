import logging
import os
import pandas as pd
import numpy as np
from src.agro.processor import AgroProcessor
from src.weather.processor import WeatherProcessor
from src.features.builder import FeatureBuilder
from src.scraping.news_scraper import NewsScraper

def setup_logging():
    """Implementa logging grabando en data_health_report.log y en consola"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        filename='data_health_report.log',
        filemode='w',
        level=logging.INFO,
        format=log_format
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger('').addHandler(console)
    return logging.getLogger('PipelineOrchestrator')

def create_mock_data():
    """Genera datos MOCK si no existen para probar el Pipeline sin error."""
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    
    # Agro mock
    if not os.path.exists('data/raw/agro_data.csv'):
        pd.DataFrame({
            'ANHO': [2023, 2023, 2024, 2024, 2024],
            'MES': [1, 2, 1, 2, 3],
            'Dpto': ['Lima', 'Lima', 'Ica', 'Ica', 'Lima'],
            'PROV': ['Lima', 'Lima', 'Ica', 'Ica', 'Lima'],
            'DIST': ['Ate', 'Ate', 'Ica', 'Ica', 'Ate'],
            'PRODUCTO': ['Papa', 'Papa', 'Palta', 'Palta', 'Papa'],
            'PRECIO': [1.5, 999.0, 5.0, 4.8, 1.6], # 999.0 is an outlier
            'PRODUCCION': [1000, 1100, 500, 520, 1050],
            'UBIGEO': ['150103', '150103', '110101', '110101', '150103']
        }).to_csv('data/raw/agro_data.csv', index=False)
        
    # Weather mock
    if not os.path.exists('data/raw/weather_data.csv'):
        pd.DataFrame({
            'ANHO': [2023, 2023, 2024, 2024],
            'MES': [1, 2, 1, 2],
            'DISTRITO': ['ate', 'ate', 'ica', 'ica'],
            'UBIGEO': ['150103', '150103', '110101', '110101'],
            'TEMPERATURA': [22.5, np.nan, 28.0, 27.5], # np.nan imputable
            'PRECIPITACION': [0.0, 1.2, 0.0, 0.0]
        }).to_csv('data/raw/weather_data.csv', index=False)

def main():
    logger = setup_logging()
    logger.info("=========================================")
    logger.info(" Iniciando Orquestador Multimodal (Agro) ")
    logger.info("=========================================")
    
    create_mock_data() # Setup dummies si no hay data
    
    # 1. Load Data
    logger.info("\n---> STEP 1: Load Data")
    agro_proc = AgroProcessor('data/raw/agro_data.csv')
    weather_proc = WeatherProcessor('data/raw/weather_data.csv')
    
    crops = agro_proc.get_available_crops()
    logger.info(f"Cultivos en DB: {crops}")
    
    # 2. User Selection
    target_crop = 'papa' # Esto se volvería dinámico o parametrizable
    logger.info(f"\n---> STEP 2: User Selection -> Crop: {target_crop.upper()}")
    
    # 3. Clean
    logger.info("\n---> STEP 3: Clean & Standardize")
    df_agro_crop = agro_proc.filter_by_crop(target_crop)
    df_agro_clean = agro_proc.clean_data(df_agro_crop, num_cols=['PRECIO', 'PRODUCCION'])
    
    df_weather_clean = weather_proc.standardize_districts(weather_proc.df)
    df_weather_clean = weather_proc.check_integrity_and_impute(df_weather_clean)
    
    # Reportes para el logger
    if not df_agro_crop.empty:
        diff_outliers = len(df_agro_crop) - len(df_agro_clean)
        logger.info(f"Salud de Datos: Se removieron {diff_outliers} filas atípicas/erróneas.")

    # 4. Merge
    logger.info("\n---> STEP 4: Merge & Feature Engineering")
    builder = FeatureBuilder()
    df_merged = builder.merge_datasets(df_agro_clean, df_weather_clean)
    
    if not df_merged.empty:
        # Lags
        df_merged = builder.generate_lag_features(df_merged, columns=['TEMPERATURA', 'PRECIO'], lags=[1, 2, 3])
        # Seasonality
        df_merged = builder.add_seasonality(df_merged, month_col='MES')
    else:
        logger.warning("Dataset Merged is empty. Check joining keys.")

    # 5. Scraper (NLP)
    logger.info("\n---> STEP 5: Run NLP Scraper (News / Prices)")
    scraper = NewsScraper()
    df_news = scraper.scrape_noticias(target_crop)
    
    # 6. Export Processed Data
    logger.info("\n---> STEP 6: Export Data")
    
    # Export datasets lists for Dashboard
    out_agro = f'data/processed/dataset_final_{target_crop.lower()}.csv'
    out_news = f'data/processed/news_beto_{target_crop.lower()}.csv'
    
    if not df_merged.empty:
        df_merged.to_csv(out_agro, index=False)
        logger.info(f"Data tabular fusionada para modelo LSTM exportada: {out_agro} (Shape: {df_merged.shape})")
        
    if not df_news.empty:
        df_news.to_csv(out_news, index=False)
        logger.info(f"Data de noticias exportada para modelo BETO: {out_news} (Shape: {df_news.shape})")

    logger.info("\nPipeline completado satisfactoriamente.")

if __name__ == "__main__":
    main()
