import os
import re
import time
import json
import random
import logging
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =====================================================================
# Mapeo de meses en español → número (Agraria usa "17 abril 2026")
# =====================================================================
MESES_ES = {
    'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
    'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
    'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
}

def parse_spanish_date(raw_text: str) -> str:
    """
    Convierte texto de span.fecha '17 abril 2026 | 09:03 am' → '2026-04-17' (ISO).
    Retorna '' si no logra parsear.
    """
    try:
        # Tomar solo la parte antes del pipe
        clean = raw_text.split('|')[0].strip()
        # Quitar iconos unicode residuales
        clean = re.sub(r'[^\w\s]', '', clean).strip()
        # Limpiar espacios múltiples
        clean = re.sub(r'\s+', ' ', clean).strip()
        parts = clean.split()
        if len(parts) >= 3:
            day = parts[0].zfill(2)
            month = MESES_ES.get(parts[1].lower(), '')
            year = parts[2]
            if month and year.isdigit() and len(year) == 4:
                return f"{year}-{month}-{day}"
    except Exception:
        pass
    return ''


class HistoricalNewsScraper:
    """
    Scraper de Alta Resiliencia para extracción de noticias históricas de Agraria.pe.
    Usa Selenium Headless + Persistencia por año + Checkpoints + Manejo de Pop-ups.
    
    Selectores VERIFICADOS contra el DOM real de agraria.pe (Abril 2026):
      - Contenedor general:  div.card.noticia_single
      - Cada noticia:        div.card-body (hijos directos del contenedor)
      - Fecha:               span.fecha  →  "17 abril 2026 | 09:03 am"
      - Título + Link:       h2.header-page > a[href]
      - Resumen:             div.sumilla
      - Paginación:          ?page=N en la URL de categoría
    """
    # Categorías reales extraídas del menú de agraria.pe
    CATEGORIAS = [
        'negocios', 'produccion', 'alimentacion',
        'tecnologia', 'clima-y-medio-ambiente', 'eventos',
        'opinion', 'politica', 'proyectos', 'salud-y-sanidad',
        'agro-en-la-prensa', 'agraria-tv'
    ]

    def __init__(self, use_headless: bool = True):
        self.logger = logging.getLogger(__name__)
        
        # Keywords de sincronización con MIDAGRI/NASA
        self.keywords = [
            'limón', 'limon', 'emergencia agraria', 'bloqueo carretera', 
            'paro transporte', 'lluvias intensas', 'sequía', 'sequia',
            'estacionalidad', 'cosecha', 'siembra', 'fertilizantes', 'flete'
        ]
        self.min_year = 2021
        self.max_year = 2025
        
        # Persistencia
        self.data_dir = os.path.join("data", "raw", "data_ingestion", "agraria_pe")
        os.makedirs(self.data_dir, exist_ok=True)
        self.checkpoint_file = os.path.join(self.data_dir, "checkpoint.json")
        self.seen_urls_cache = set()
        
        # Selenium
        self._setup_driver(use_headless)

    def _setup_driver(self, use_headless: bool):
        chrome_options = Options()
        if use_headless:
            chrome_options.add_argument("--headless=new")
        
        # User-Agent real para evadir bloqueos
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(30)

    def __del__(self):
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
        except Exception:
            pass

    # =====================================================================
    # Resiliencia de UI
    # =====================================================================
    def _handle_popups(self):
        """Elimina modals, banners de cookies y overlays inyectando JS."""
        script = """
        const selectors = [
            'div[class*="modal"]', 'div[id*="modal"]',
            'div[class*="cookie"]', 'div[id*="cookie"]',
            'div[class*="overlay"]', 'div[class*="popup"]',
            'iframe[title*="recaptcha"]'
        ];
        document.querySelectorAll(selectors.join(',')).forEach(el => el.remove());
        document.body.style.overflow = "auto";
        """
        try:
            self.driver.execute_script(script)
        except Exception as e:
            self.logger.debug("Error limpiando modals: %s", e)

    def _human_delay(self):
        """Pausa aleatoria entre 3.5 y 7.5 segundos."""
        time.sleep(random.uniform(3.5, 7.5))

    # =====================================================================
    # Persistencia y Deduplicación
    # =====================================================================
    def _load_seen_urls_for_year(self, year: int):
        """Carga URLs ya escrapeados de un CSV anual al caché en memoria."""
        filepath = os.path.join(self.data_dir, f"agro_news_{year}.csv")
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                if 'url' in df.columns:
                    self.seen_urls_cache.update(df['url'].dropna().tolist())
            except Exception as e:
                self.logger.warning(f"No se pudo leer caché de {filepath}: {e}")

    def _is_duplicate(self, url: str) -> bool:
        return url in self.seen_urls_cache

    def _append_to_csv(self, article_data: dict, year: int):
        """Escribe una fila al CSV del año correspondiente (modo append)."""
        if not article_data:
            return
        filepath = os.path.join(self.data_dir, f"agro_news_{year}.csv")
        df = pd.DataFrame([article_data])
        
        if not os.path.exists(filepath):
            df.to_csv(filepath, index=False, encoding='utf-8')
        else:
            df.to_csv(filepath, mode='a', header=False, index=False, encoding='utf-8')
            
        self.seen_urls_cache.add(article_data['url'])

    # =====================================================================
    # Checkpoint (Por número de página)
    # =====================================================================
    def _save_checkpoint(self, source_key: str, page: int):
        state = {}
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
            except Exception:
                pass
        state[source_key] = {"last_page": page, "updated_at": datetime.now().isoformat()}
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4)

    def _load_checkpoint(self, source_key: str) -> int:
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    if source_key in state:
                        last = state[source_key]['last_page']
                        self.logger.info(f"Checkpoint encontrado para '{source_key}': retomando desde página {last}")
                        return last
            except Exception:
                pass
        return 1

    # =====================================================================
    # Validación
    # =====================================================================
    def _validate_year(self, date_iso: str) -> bool:
        """Valida que la fecha ISO esté en la ventana 2021-2025."""
        try:
            year = int(date_iso[:4])
            return self.min_year <= year <= self.max_year
        except (ValueError, IndexError):
            return False

    def _contains_keywords(self, text: str) -> bool:
        text_lower = text.lower()
        return any(kw in text_lower for kw in self.keywords)

    # =====================================================================
    # Extracción del Cuerpo Completo (página individual de noticia)
    # =====================================================================
    def _fetch_article_body(self, article_url: str) -> str:
        """Navega a la noticia individual y extrae todos los <p> del cuerpo."""
        try:
            self._human_delay()
            self.driver.get(article_url)
            self._handle_popups()
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Selector VERIFICADO: el cuerpo real del artículo en agraria.pe es div.cuerpo
            content = (
                soup.find('div', class_='cuerpo') or
                soup.find('div', class_='contenido_inside') or
                soup.find('div', class_='card-body')
            )
            if content:
                paragraphs = content.find_all('p')
                return " ".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
            return ""
        except Exception as e:
            self.logger.error(f"Error extrayendo cuerpo de {article_url}: {e}")
            return ""

    # =====================================================================
    # Parsing de noticias desde el listado (selectores verificados)
    # =====================================================================
    def _parse_news_cards(self, soup: BeautifulSoup) -> list:
        """
        Extrae noticias del DOM de una página de categoría de agraria.pe.
        
        Estructura verificada del DOM:
          div.card.noticia_single
            └─ div.card-body          (una por noticia)
                 ├─ div.noticia_header
                 │    └─ span.fecha   →  "17 abril 2026 | 09:03 am"
                 ├─ h2.header-page
                 │    └─ a[href]      →  link + título
                 └─ div.sumilla       →  resumen
        """
        candidates = []
        
        # Buscar el contenedor principal de noticias
        container = soup.find('div', class_='noticia_single')
        if not container:
            return candidates
        
        # Cada noticia es un div.card-body dentro del contenedor
        card_bodies = container.find_all('div', class_='card-body')
        
        for card in card_bodies:
            # --- Título y Link (h2.header-page > a) ---
            h2 = card.find('h2', class_='header-page')
            if not h2:
                continue
            link_elem = h2.find('a', href=True)
            if not link_elem:
                continue
            
            title = link_elem.get_text(strip=True)
            href = link_elem['href']
            if not title or '/noticias/' not in href:
                continue
            
            article_url = href if href.startswith("http") else f"https://agraria.pe{href}"
            
            # Control de duplicados O(1)
            if self._is_duplicate(article_url):
                continue

            # --- Fecha (span.fecha) → "17 abril 2026 | 09:03 am" ---
            fecha_span = card.find('span', class_='fecha')
            if not fecha_span:
                continue
            
            raw_date_text = fecha_span.get_text(strip=True)
            date_iso = parse_spanish_date(raw_date_text)
            
            if not date_iso:
                self.logger.debug(f"Fecha no parseable: '{raw_date_text}'. Saltando.")
                continue

            # Filtro de ventana temporal (2021-2025)
            if not self._validate_year(date_iso):
                continue

            # Filtro de keywords en titular + resumen
            sumilla = card.find('div', class_='sumilla')
            full_text = title
            if sumilla:
                full_text += " " + sumilla.get_text(strip=True)
            
            if not self._contains_keywords(full_text):
                continue
            
            candidates.append({
                "url": article_url,
                "title": title,
                "date_iso": date_iso,
                "year": int(date_iso[:4])
            })
        
        return candidates

    # =====================================================================
    # Motor Principal de Scraping
    # =====================================================================
    def scrape_category(self, categoria: str, end_page: int = 200, resume: bool = True):
        """
        Scrapea una categoría completa de agraria.pe por paginación.
        URL real: https://agraria.pe/noticias/categoria/{categoria}?page={N}
        """
        source_key = f"agraria.pe/{categoria}"
        base_url = f"https://agraria.pe/noticias/categoria/{categoria}"
        
        start_page = self._load_checkpoint(source_key) if resume else 1
        
        # Pre-cargar caché de URLs existentes
        for y in range(self.min_year, self.max_year + 1):
            self._load_seen_urls_for_year(y)
        self.logger.info(f"Caché cargado: {len(self.seen_urls_cache)} URLs conocidos.")

        consecutive_empty = 0

        for page in range(start_page, end_page + 1):
            url = f"{base_url}?page={page}"
            
            try:
                self.logger.info(f"==> [{categoria.upper()}] Página {page}/{end_page} - {url}")
                self.driver.get(url)
                self._human_delay()
                self._handle_popups()
                
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Parsear las tarjetas de noticias con selectores verificados
                candidates = self._parse_news_cards(soup)
                
                # Verificar si la página tiene contenido
                container = soup.find('div', class_='noticia_single')
                if not container:
                    consecutive_empty += 1
                    self.logger.warning(f"Página {page} sin contenedor de noticias ({consecutive_empty} vacías consecutivas).")
                    if consecutive_empty >= 3:
                        self.logger.warning("3 páginas vacías consecutivas. Fin de paginación. Deteniendo.")
                        break
                    self._save_checkpoint(source_key, page)
                    continue
                
                card_bodies = container.find_all('div', class_='card-body')
                if not card_bodies:
                    consecutive_empty += 1
                    self.logger.warning(f"Página {page} sin noticias ({consecutive_empty} vacías consecutivas).")
                    if consecutive_empty >= 3:
                        break
                    self._save_checkpoint(source_key, page)
                    continue
                
                consecutive_empty = 0  # Reset si encontramos contenido
                
                self.logger.info(f"    {len(card_bodies)} noticias en página, {len(candidates)} pasaron filtros.")

                # --- Deep Crawling: Extraer cuerpo de cada candidato ---
                for item in candidates:
                    self.logger.info(f"    -> Extrayendo: {item['title'][:60]}...")
                    body = self._fetch_article_body(item['url'])
                    
                    if body:
                        final_row = {
                            "fecha": item['date_iso'],
                            "titular": item['title'],
                            "cuerpo_completo": body,
                            "fuente": f"agraria.pe/{categoria}",
                            "url": item['url']
                        }
                        self._append_to_csv(final_row, item['year'])
                        self.logger.info(f"    [OK] Guardado en agro_news_{item['year']}.csv")

                # Checkpoint después de procesar toda la página
                self._save_checkpoint(source_key, page)
                
            except WebDriverException as e:
                self.logger.error(f"Error grave de Selenium en página {page}: {e}")
                self.logger.info("Pausa de seguridad de 15s antes de abortar ventana.")
                time.sleep(15)
                break
                
        self.logger.info(f"Ventana de extracción para '{source_key}' finalizada.")

    def scrape_all_categories(self, end_page: int = 200, resume: bool = True):
        """Ejecuta el scraping sobre todas las categorías disponibles de agraria.pe."""
        for cat in self.CATEGORIAS:
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"  INICIANDO CATEGORÍA: {cat.upper()}")
            self.logger.info(f"{'='*60}")
            self.scrape_category(cat, end_page=end_page, resume=resume)


# =========================================================================
# Punto de Entrada
# =========================================================================
if __name__ == "__main__":
    scraper = HistoricalNewsScraper(use_headless=True)
    
    # Producción: scrapea TODAS las categorías, 200 páginas cada una.
    # Las noticias 2021-2025 empiezan aprox. desde la página 30+ en adelante.
    # El sistema de checkpoint (resume=True) permite detener y reanudar sin perder progreso.
    scraper.scrape_all_categories(end_page=200, resume=True)
