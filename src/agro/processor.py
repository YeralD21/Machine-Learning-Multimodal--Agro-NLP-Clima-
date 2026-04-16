import pandas as pd
import numpy as np
import logging
import unicodedata

class AgroProcessor:
    def __init__(self, data_path: str):
        self.data_path = data_path
        self.logger = logging.getLogger(__name__)
        try:
            self.df = pd.read_csv(data_path)
            self.logger.info(f"Loaded Agro data from {data_path} with shape {self.df.shape}")
        except FileNotFoundError:
            self.logger.warning(f"File {data_path} not found. Operating with empty DataFrame.")
            self.df = pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error loading agro data: {e}")
            self.df = pd.DataFrame()
            
    def _normalize_text(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        # Ignore tildes and convert to lowercase
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        return text.strip().lower()

    def get_available_crops(self) -> list:
        if 'PRODUCTO' in self.df.columns:
            return self.df['PRODUCTO'].dropna().unique().tolist()
        return []

    def filter_by_crop(self, crop_name: str) -> pd.DataFrame:
        if 'PRODUCTO' not in self.df.columns:
            self.logger.warning("Column 'PRODUCTO' not found in dataset. Cannot filter by crop.")
            return self.df
            
        crop_clean = self._normalize_text(crop_name)
        # Búsqueda flexible ignorando tildes y mayúsculas
        prod_norm = self.df['PRODUCTO'].apply(self._normalize_text)
        filtered_df = self.df[prod_norm == crop_clean].copy()
        
        # Validación de meses (mínimo 24 meses)
        if 'ANHO' in filtered_df.columns and 'MES' in filtered_df.columns:
            unique_months = filtered_df.groupby(['ANHO', 'MES']).ngroups
        elif 'FECHA' in filtered_df.columns:
            filtered_df['FECHA_parsed'] = pd.to_datetime(filtered_df['FECHA'], errors='coerce')
            unique_months = filtered_df['FECHA_parsed'].dt.to_period('M').nunique()
        else:
            unique_months = len(filtered_df)
            
        if unique_months < 24:
            self.logger.warning(f"Warning: Cultivo '{crop_name}' tiene menos de 24 meses de data (encontrados {unique_months} meses).")
        
        return filtered_df

    def clean_data(self, df: pd.DataFrame, num_cols: list = ['PRECIO', 'PRODUCCION']) -> pd.DataFrame:
        df_clean = df.copy()
        
        # Remoción de Outliers usando Rango Intercuartílico (IQR)
        for col in num_cols:
            if col in df_clean.columns:
                Q1 = df_clean[col].quantile(0.25)
                Q3 = df_clean[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Filtrar dentro de los límites
                df_clean = df_clean[(df_clean[col] >= lower_bound) & (df_clean[col] <= upper_bound)]
                
        # Limpieza de texto para columnas de ubicación
        loc_cols = ['DPTO', 'PROV', 'DIST']
        for col in loc_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(self._normalize_text)
                
        if len(df) > 0:
            removed = len(df) - len(df_clean)
            self.logger.info(f"Cleaned data for outliers: {removed} rows removed.")
            
        return df_clean
