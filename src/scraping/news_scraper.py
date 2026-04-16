import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from urllib import robotparser
from datetime import datetime
import pandas as pd

class NewsScraper:
    def __init__(self, user_agent: str = "AgroDataBot/1.0 (+http://tu-dominio-agro.com)"):
        self.logger = logging.getLogger(__name__)
        self.user_agent = user_agent
        self.headers = {"User-Agent": self.user_agent}
        self.rp = robotparser.RobotFileParser()

    def _respect_robots_txt(self, base_url: str):
        robots_url = f"{base_url}/robots.txt"
        self.rp.set_url(robots_url)
        try:
            self.rp.read()
        except:
            self.logger.warning(f"Could not read robots.txt from {robots_url}. Proceeding cautiously.")

    def _is_allowed(self, url: str) -> bool:
        return self.rp.can_fetch(self.user_agent, url)

    def scrape_noticias(self, crop_name: str) -> pd.DataFrame:
        """
        Scraper ético y modular. Extrae noticias para el modelo BETO.
        """
        base_url = "https://www.actualidadambiental.pe" # Dominio mock o real a reemplazar
        search_url = f"{base_url}/?s={crop_name}"
        
        # Configuración Ética
        self._respect_robots_txt(base_url)
        
        if not self._is_allowed(search_url):
            self.logger.error("Scraping not allowed by robots.txt for this URL. Stopping extraction.")
            return pd.DataFrame()

        news_data = [] # Guardará (Fecha, Titular, Sentimiento_Base)
        
        try:
            # Delay aleatorio entre 3 y 5 segundos
            delay = random.uniform(3, 5)
            self.logger.info(f"Módulo Ético: Esperando {delay:.2f} segundos antes de request...")
            time.sleep(delay)
            
            response = requests.get(search_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            # Simulando lectura con BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.find_all('article')
            
            for article in articles[:10]: # Solo limitamos para prueba
                title_elem = article.find('h2') or article.find('h3')
                date_elem = article.find('time')
                
                title = title_elem.get_text(strip=True) if title_elem else f"Reporte mercado {crop_name}"
                date_str = date_elem['datetime'] if (date_elem and date_elem.has_attr('datetime')) else datetime.now().strftime('%Y-%m-%d')
                
                # Sentimiento Base: 1 (Neutral). BETO actualizará esto después.
                news_data.append({
                    "Fecha": date_str,
                    "Titular": title,
                    "Sentimiento_Base": 1,
                    "Fuente": base_url
                })
                
        except Exception as e:
            self.logger.error(f"Error scraping data para {crop_name}: {e}")

        # Si no hay noticias (ej. por dominio mock), devolvemos dummy responsable
        if not news_data:
            self.logger.warning("No se encontraron elementos HTML esperados. Retornando dummy data para BETO.")
            news_data = [{
                "Fecha": datetime.now().strftime('%Y-%m-%d'),
                "Titular": f"El mercado de {crop_name} muestra estabilidad este mes",
                "Sentimiento_Base": 1,
                "Fuente": "Dummy"
            }]

        df = pd.DataFrame(news_data, columns=["Fecha", "Titular", "Sentimiento_Base", "Fuente"])
        self.logger.info(f"Scraped {len(df)} articles for {crop_name}. Formato listo para BETO.")
        return df
