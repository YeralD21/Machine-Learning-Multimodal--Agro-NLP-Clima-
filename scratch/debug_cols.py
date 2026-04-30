import pandas as pd
import os

INTERIM = "data/02_interim"

def check(file):
    print(f"Checking {file}...")
    df = pd.read_csv(f"{INTERIM}/{file}")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {df.columns.tolist()}")
    if 'fecha_evento' in df.columns:
        print("  fecha_evento present.")
    else:
        print("  !!! fecha_evento MISSING !!!")

check("midagri_limon_clean.csv")
check("indeci_eventos_clean.csv")
check("agraria_noticias_clean.csv")
