import os
from pathlib import Path

def create_project_structure():
    """
    Crea la estructura de directorios y archivos base para el proyecto de
    Machine Learning Multimodal (Agro + NLP + Clima).
    """
    # Usar el directorio de trabajo actual como base (raíz del proyecto)
    base_dir = Path.cwd()
    
    print(f"[*] Creando estructura del proyecto en: {base_dir}\n")
    
    # 1. Definir los directorios a crear
    directories = [
        "data/raw",          # datos brutos
        "data/processed",    # datos limpios/unidos
        "data/external",     # archivos de mapeo como Ubigeos
        "src/scraping",      # Scraper de noticias (Agraria, Andina)
        "src/weather",       # procesar archivos SENAMHI
        "src/agro",          # procesar data del MIDAGRI
        "src/features",      # Lógica para unir fuentes (Merge) y crear dataset
        "src/models",        # Definición Arquitectura LSTM-Attention y entrenamiento
        "notebooks",         # Experimentos rápidos
        "dashboard"          # Visualización y XAI (SHAP)
    ]
    
    # Crear todos los directorios
    for dir_path in directories:
        path = base_dir / dir_path
        path.mkdir(parents=True, exist_ok=True)
        print(f"[+] Creado directorio: {dir_path}")
        
    # 2. Crear archivos __init__.py en la carpeta src y sus subcarpetas
    src_dirs = [
        "src",
        "src/scraping",
        "src/weather",
        "src/agro",
        "src/features",
        "src/models"
    ]
    
    for src_dir in src_dirs:
        init_file = base_dir / src_dir / "__init__.py"
        init_file.touch(exist_ok=True)
        print(f"[+] Creado archivo: {src_dir}/__init__.py")
        
    # 3. Crear el archivo requirements.txt en la raíz del proyecto
    req_file_path = base_dir / "requirements.txt"
    if not req_file_path.exists():
        # Añadiendo algunas librerías básicas sugeridas para el rol (opcional, pero útil)
        with open(req_file_path, "w", encoding="utf-8") as f:
            f.write("# --- Machine Learning Multimodal (Agro + NLP + Clima) ---\n\n")
            f.write("# Manipulación de datos\n")
            f.write("pandas\nnumpy\n\n")
            f.write("# Scraping\n")
            f.write("beautifulsoup4\nrequests\nselenium\n\n")
            f.write("# NLP\n")
            f.write("nltk\nspacy\ntransformers\n\n")
            f.write("# Modelado (LSTM-Attention)\n")
            f.write("tensorflow\nscikit-learn\n\n")
            f.write("# Visualización y Dashboard\n")
            f.write("streamlit\nmatplotlib\nplotly\n\n")
            f.write("# Explainable AI (XAI)\n")
            f.write("shap\n\n")
            f.write("# Jupyter\n")
            f.write("notebook\njupyterlab\n")
        print(f"[+] Creado archivo: requirements.txt")
    else:
        print(f"[!] El archivo requirements.txt ya existe, se omitio su creacion para evitar sobrescribir.")

    print("\n[OK] La estructura de tu proyecto de Machine Learning esta lista!")

if __name__ == "__main__":
    create_project_structure()
