import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

class MasterDatasetBuilder:
    def __init__(self, output_path: str = "data/processed/master_dataset_lstm.csv"):
        self.output_path = output_path
        self._setup_logging()

    def _setup_logging(self):
        self.logger = logging.getLogger("MasterBuilder")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def load_midagri(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        # Asegurar formato fecha_evento YYYY-MM
        df['fecha_evento'] = pd.to_datetime(df['fecha_evento']).dt.strftime('%Y-%m')
        # Normalizar geografía
        df['departamento'] = df['departamento'].str.upper().str.strip()
        df['provincia'] = df['provincia'].str.upper().str.strip()
        return df

    def load_nasa(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        # Renombrar DATE a fecha_evento y convertir a YYYY-MM
        df['fecha_evento'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m')
        # Eliminar columnas temporales viejas e innecesarias
        cols_to_drop = ['DATE', 'month_sin', 'month_cos']
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
        # Normalizar geografía
        df['departamento'] = df['departamento'].str.upper().str.strip()
        df['provincia'] = df['provincia'].str.upper().str.strip()
        return df

    def load_indeci(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        # Asegurar formato fecha_evento YYYY-MM
        df['fecha_evento'] = pd.to_datetime(df['fecha_evento']).dt.strftime('%Y-%m')
        # Normalizar geografía
        df['departamento'] = df['departamento'].str.upper().str.strip()
        df['provincia'] = df['provincia'].str.upper().str.strip()
        return df

    def load_news_sentiment(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        # Al ser un archivo de noticias unificado, agrupamos por mes
        df['fecha_evento'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m')
        # Si no tiene columna de sentimiento, asumimos un marcador dummy para este paso 
        # (El usuario mencionó score de sentimiento, si no existe lo simulamos o buscamos)
        if 'sentiment_score' not in df.columns:
            self.logger.warning("Sentiment score not found in news. Creating dummy/neutral score.")
            df['sentiment_score'] = 0.5 # Neutral
        
        # Agrupar por mes y promediar sentimiento
        df_agg = df.groupby('fecha_evento', as_index=False).agg({
            'sentiment_score': 'mean'
        })
        return df_agg

    def apply_cyclic_encoding(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula month_sin, month_cos, day_sin, day_cos basado en la fecha_evento.
        Como la data es mensual, day_sin/cos serán constantes (día 1), pero el usuario lo pidió.
        """
        # Convertimos fecha_evento a datetime real para extraer componentes
        temp_date = pd.to_datetime(df['fecha_evento'])
        
        # Mes (1-12)
        months = temp_date.dt.month
        df['month_sin'] = np.sin(2 * np.pi * months / 12)
        df['month_cos'] = np.cos(2 * np.pi * months / 12)
        
        # Día (1-31)
        days = temp_date.dt.day
        df['day_sin'] = np.sin(2 * np.pi * days / 31)
        df['day_cos'] = np.cos(2 * np.pi * days / 31)
        
        return df

    def build(self):
        self.logger.info("Starting Master Dataset Creation...")
        
        # 1. Cargar fuentes
        df_midagri = self.load_midagri("data/interim/midagri/midagri_limon_procesado.csv")
        df_nasa = self.load_nasa("data/interim/nasa/clima_dataset_final.csv")
        df_indeci = self.load_indeci("data/interim/indeci/indeci_temporal_2021_2025.csv")
        df_news = self.load_news_sentiment("data/interim/agraria/noticias_unificadas_2021_2025.csv")
        
        # 2. Unir MIDAGRI + NASA (Llave: fecha_evento, departamento, provincia)
        # Usamos inner join para asegurar que tenemos tanto agro como clima
        master = pd.merge(df_midagri, df_nasa, on=['fecha_evento', 'departamento', 'provincia'], how='inner')
        self.logger.info(f"After MIDAGRI + NASA join: {master.shape}")
        
        # 3. Unir + INDECI (Llave: fecha_evento, departamento, provincia)
        # Left join porque no todos los meses/provincias tienen emergencias
        master = pd.merge(master, df_indeci, on=['fecha_evento', 'departamento', 'provincia'], how='left')
        self.logger.info(f"After INDECI join: {master.shape}")
        
        # 4. Unir + NEWS (Llave: fecha_evento)
        # Left join porque las noticias son nacionales/mensuales
        master = pd.merge(master, df_news, on='fecha_evento', how='left')
        self.logger.info(f"After News join: {master.shape}")
        
        # 5. Limpieza post-union
        # Rellenar nulos de INDECI y NEWS con 0 o promedios
        fill_zeros = ['num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas']
        for col in fill_zeros:
            if col in master.columns:
                master[col] = master[col].fillna(0)
        
        if 'sentiment_score' in master.columns:
            master['sentiment_score'] = master['sentiment_score'].fillna(master['sentiment_score'].mean())
            
        # Eliminar duplicados si existen
        master = master.drop_duplicates()
        
        # 6. Codificación Cíclica
        master = self.apply_cyclic_encoding(master)
        
        # 7. Sorteo cronológico por ubicación
        master = master.sort_values(by=['departamento', 'provincia', 'fecha_evento'])
        
        # 8. Exportar
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        master.to_csv(self.output_path, index=False)
        self.logger.info(f"Successfully exported master dataset to {self.output_path}")
        self.logger.info(f"Final shape: {master.shape}")
        return master

if __name__ == "__main__":
    builder = MasterDatasetBuilder()
    builder.build()
