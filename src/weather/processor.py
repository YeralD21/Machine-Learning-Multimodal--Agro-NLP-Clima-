import pandas as pd
import numpy as np
import logging
import unicodedata

class WeatherProcessor:
    def __init__(self, data_path: str):
        self.data_path = data_path
        self.logger = logging.getLogger(__name__)
        try:
            self.df = pd.read_csv(data_path)
            self.logger.info(f"Loaded Weather data from {data_path} with shape {self.df.shape}")
        except FileNotFoundError:
            self.logger.warning(f"File {data_path} not found. Operating with empty DataFrame.")
            self.df = pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error loading weather data: {e}")
            self.df = pd.DataFrame()

    def _normalize_text(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        # Limpieza de caracteres especiales (solo alfanuméricos y espacios)
        text = "".join([c for c in text if c.isalnum() or c.isspace()])
        return text.strip().lower()

    def standardize_districts(self, df: pd.DataFrame, col_name: str = 'DISTRITO') -> pd.DataFrame:
        """
        Estandariza los nombres de distritos para que coincidan con la data Agro.
        """
        if col_name in df.columns:
            df[col_name] = df[col_name].apply(self._normalize_text)
        return df

    def check_integrity_and_impute(self, df: pd.DataFrame, value_cols: list = ['TEMPERATURA', 'PRECIPITACION']) -> pd.DataFrame:
        """
        Chequeo de integridad y rellenado con promedio histórico si faltan más de 3 meses.
        """
        df_imputed = df.copy()
        
        if 'MES' in df_imputed.columns:
            for col in value_cols:
                if col in df_imputed.columns:
                    missing_count = df_imputed[col].isnull().sum()
                    if missing_count > 3:
                        self.logger.info(f"Integrity Check: {missing_count} missing values in {col}. Imputing using historical monthly mean.")
                        # Rellenar con promedio histórico del mes
                        df_imputed[col] = df_imputed.groupby('MES')[col].transform(lambda x: x.fillna(x.mean()))
                    elif missing_count > 0:
                        # Si faltan 3 o menos, usar interpolación básica
                        self.logger.info(f"Integrity Check: Missing < 3 in {col}. Imputing with interpolation.")
                        df_imputed[col] = df_imputed[col].interpolate()
        else:
            self.logger.warning("Column 'MES' not found, cannot impute based on monthly historical data.")

        return df_imputed
