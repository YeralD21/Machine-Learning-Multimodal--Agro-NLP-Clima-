"""Diagnostico: verificar selectores del cuerpo de una noticia individual."""
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(30)

# Una noticia real que sabemos existe
url = "https://agraria.pe/noticias/fuertes-lluvias-en-el-norte-amenazan-proxima-campana-en-uva-d-36312"
print(f"Cargando: {url}")
driver.get(url)
time.sleep(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')

# Probar selectores
selectors_to_try = [
    ('div.text-justify', soup.find('div', class_='text-justify')),
    ('div.col-md-8', soup.find('div', class_='col-md-8')),
    ('div.noticia_single', soup.find('div', class_='noticia_single')),
    ('div.sumilla', soup.find('div', class_='sumilla')),
    ('div.card-body', soup.find('div', class_='card-body')),
    ('div.nota-contenido', soup.find('div', class_='nota-contenido')),
]

for name, elem in selectors_to_try:
    if elem:
        paragraphs = elem.find_all('p')
        text = " ".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
        print(f"\n  {name}: FOUND ({len(paragraphs)} <p> tags)")
        if text:
            safe = text[:300].encode('ascii', errors='replace').decode('ascii')
            print(f"  Preview: {safe}")
        else:
            print("  (No text in <p> tags)")
            # Show raw text instead
            raw = elem.get_text(strip=True)[:300].encode('ascii', errors='replace').decode('ascii')
            print(f"  Raw text: {raw}")
    else:
        print(f"\n  {name}: NOT FOUND")

# Also dump the body HTML tags to see the real structure 
body = soup.find('body')
if body:
    # Find all divs with a class attr
    all_divs = body.find_all('div', class_=True, limit=50)
    print("\n--- All div classes in article page (first 50) ---")
    seen = set()
    for d in all_divs:
        classes = ' '.join(d.get('class', []))
        if classes not in seen:
            seen.add(classes)
            print(f"  div.{classes}")

driver.quit()
print("\nDone.")
