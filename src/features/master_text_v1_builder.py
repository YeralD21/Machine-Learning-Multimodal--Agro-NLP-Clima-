import pandas as pd
import numpy as np
import os
import logging

class MasterDatasetTextBuilder:
    def __init__(self, output_path: str = "data/processed/master_dataset_text_v1.csv"):
        self.output_path = output_path
        self._setup_logging()

    def _setup_logging(self):
        self.logger = logging.getLogger("MasterTextBuilder")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def load_master_base(self, path: str) -> pd.DataFrame:
        """Carga el dataset maestro actual y elimina el score de sentimiento."""
        df = pd.read_csv(path)
        if 'sentiment_score' in df.columns:
            df = df.drop(columns=['sentiment_score'])
        return df

    def load_and_group_news(self, path: str) -> pd.DataFrame:
        """Carga noticias y agrupa titular + cuerpo por mes."""
        df = pd.read_csv(path)
        
        # Crear llave mes YYYY-MM
        df['fecha_evento'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m')
        
        # Limpiar nulos en texto
        df['titular'] = df['titular'].fillna('')
        df['cuerpo_completo'] = df['cuerpo_completo'].fillna('')
        
        # Combinar titular y cuerpo
        df['full_item'] = "TITULO: " + df['titular'] + " | CUERPO: " + df['cuerpo_completo']
        
        # Agrupar por mes uniendo con saltos de línea
        df_blob = df.groupby('fecha_evento')['full_item'].apply(lambda x: " [SIGUIENTE NOTICIA] ".join(x)).reset_index()
        df_blob = df_blob.rename(columns={'full_item': 'news_text_blob'})
        
        return df_blob

    def build(self):
        self.logger.info("Iniciando creación de Dataset Maestro con Blobs de Texto...")
        
        # 1. Cargar base (Agro + Clima + INDECI)
        # Reutilizamos el master_dataset_lstm.csv que ya tiene la unión geo-temporal limpia
        master = self.load_master_base("data/processed/master_dataset_lstm.csv")
        
        # 2. Cargar y procesar noticias
        news_blob = self.load_and_group_news("data/interim/agraria/noticias_unificadas_2021_2025.csv")
        
        # 3. Merge por fecha_evento
        final_df = pd.merge(master, news_blob, on='fecha_evento', how='left')
        
        # 4. Rellenar meses sin noticias
        final_df['news_text_blob'] = final_df['news_text_blob'].fillna('Sin noticias reportadas')
        
        # 5. Exportar
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        final_df.to_csv(self.output_path, index=False)
        
        self.logger.info(f"Éxito. Archivo generado en: {self.output_path}")
        self.logger.info(f"Dimensiones finales: {final_df.shape}")
        
        return final_df

if __name__ == "__main__":
    builder = MasterDatasetTextBuilder()
    builder.build()
