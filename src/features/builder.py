import pandas as pd
import numpy as np
import logging

class FeatureBuilder:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def merge_datasets(self, df_agro: pd.DataFrame, df_weather: pd.DataFrame) -> pd.DataFrame:
        """
        Join dinámico por ANHO, MES y UBIGEO.
        """
        merge_keys = ['ANHO', 'MES', 'UBIGEO']
        
        # Verificar qué llaves existen en ambos datasets
        available_keys = [k for k in merge_keys if k in df_agro.columns and k in df_weather.columns]
        
        if not available_keys:
            # Fallback en caso no existan columnas de UBIGEO sino solo distrito
            fallback_keys = ['ANHO', 'MES', 'DIST']
            # Se requiere renombrar si difieren los nombres
            if 'DISTRITO' in df_weather.columns and 'DIST' not in df_weather.columns:
                 df_weather = df_weather.rename(columns={'DISTRITO': 'DIST'})
            
            available_keys = [k for k in fallback_keys if k in df_agro.columns and k in df_weather.columns]
            
        if not available_keys:
            self.logger.error("No common keys found for merging (expected ANHO, MES, UBIGEO or DIST).")
            return df_agro
            
        self.logger.info(f"Merging datasets dynamically on keys: {available_keys}")
        
        # Left Join
        df_merged = pd.merge(df_agro, df_weather, on=available_keys, how='left')
        return df_merged

    def generate_lag_features(self, df: pd.DataFrame, columns: list, lags: list = [1, 2, 3]) -> pd.DataFrame:
        """
        Genera automáticamente Lag Features (t-1, t-2, t-3) para variables numéricas dadas.
        El clima pasado afecta la oferta presente.
        """
        df_lagged = df.copy()
        
        # Idealmente el dataframe debe estar sorteado cronológicamente para aplicar .shift()
        if 'ANHO' in df_lagged.columns and 'MES' in df_lagged.columns:
            df_lagged = df_lagged.sort_values(by=['ANHO', 'MES'])
            
        # Agrupar por zona geográfica para que el lag sea consistente por ubicación
        group_col = 'UBIGEO' if 'UBIGEO' in df_lagged.columns else ('DIST' if 'DIST' in df_lagged.columns else None)
        
        for col in columns:
            if col in df_lagged.columns:
                for lag in lags:
                    feature_name = f'{col}_lag_{lag}'
                    if group_col:
                        df_lagged[feature_name] = df_lagged.groupby(group_col)[col].shift(lag)
                    else:
                        df_lagged[feature_name] = df_lagged[col].shift(lag)
                        
        self.logger.info(f"Generated {len(lags)} lag features (t-1, t-2, t-3) for columns: {columns}")
        return df_lagged

    def add_seasonality(self, df: pd.DataFrame, month_col: str = 'MES') -> pd.DataFrame:
        """
        Columna de Estacionalidad basada en el mes con codificación cíclica (Seno/Coseno).
        """
        df_season = df.copy()
        if month_col in df_season.columns:
            df_season['MES_sin'] = np.sin(2 * np.pi * df_season[month_col] / 12)
            df_season['MES_cos'] = np.cos(2 * np.pi * df_season[month_col] / 12)
            self.logger.info("Added seasonality cyclic features (MES_sin, MES_cos)")
        else:
            self.logger.warning(f"Month column '{month_col}' not found for seasonality encoding.")
            
        return df_season
