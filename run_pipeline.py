"""
run_pipeline.py — Orquestador de reproducibilidad end-to-end.

Ejecuta TODOS los notebooks del proyecto en el orden correcto (Fase 1 ETL ->
Fase 2 Features/NLP -> Fase 3 Modelos clasicos -> Fase 4 Multimodal/SHAP/Sim)
usando papermill. Cada notebook se ejecuta y se guarda una copia con sufijo
`_run` en la MISMA carpeta para no sobrescribir la fuente.

Uso
----
    python run_pipeline.py                 # ejecuta todo el pipeline
    python run_pipeline.py --fase 1        # solo una fase (1,2,3,4)
    python run_pipeline.py --dry-run       # solo imprime el orden, no ejecuta
    python run_pipeline.py --list          # lista los notebooks detectados

Requisitos
----------
    pip install papermill   (incluido en requirements.txt)
    Python 3.11 con el resto de dependencias instaladas.

Si papermill NO esta instalado, el script no ejecuta nada: imprime el orden
manual exacto para que lo corras a mano (jupyter nbconvert / abrir y Run All).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Orden canonico de ejecucion. Cada entrada: (fase, ruta relativa al repo).
# Solo se incluyen los notebooks "fuente" (no los *_ejecutado.ipynb, que son
# copias con outputs ya guardados).
PIPELINE: list[tuple[int, str]] = [
    # --- FASE 1: Data Engineering / ETL (10 actividades) ---
    (1, "pipeline/actividad_01_configuracion.ipynb"),
    (1, "pipeline/actividad_02_lectura_datos.ipynb"),
    (1, "pipeline/actividad_03_eda.ipynb"),
    (1, "pipeline/actividad_04_calidad.ipynb"),
    (1, "pipeline/actividad_05_limpieza.ipynb"),
    (1, "pipeline/actividad_06_integracion_dwh.ipynb"),
    (1, "pipeline/actividad_07_dwh_schema.ipynb"),
    (1, "pipeline/actividad_08_postgresql.ipynb"),
    (1, "pipeline/actividad_09_etl.ipynb"),
    (1, "pipeline/actividad_10_reexploracion.ipynb"),
    # --- FASE 2: Feature Engineering + NLP ---
    (2, "notebooks/fase2/actividad_01_nlp_sentimiento.ipynb"),
    (2, "notebooks/fase2/actividad_02_cyclic_time_encoding.ipynb"),
    (2, "notebooks/fase2/actividad_03_rezagos_temporales.ipynb"),
    (2, "notebooks/fase2/actividad_04_normalizacion_escalado.ipynb"),
    # --- FASE 3: Modelos clasicos (baselines) ---
    (3, "notebooks/fase3/actividad_11_gc1_sarima.ipynb"),
    (3, "notebooks/fase3/actividad_12_gc1_prophet.ipynb"),
    (3, "notebooks/fase3/actividad_13_gc2_sarimax_lstm.ipynb"),
    (3, "notebooks/fase3/actividad_14_ge_lstm_attention.ipynb"),
    # --- FASE 4: Multimodal, competidores, SHAP, simulaciones ---
    (4, "notebooks/fase4/actividad_15_multimodal_nlp.ipynb"),
    (4, "notebooks/fase4/actividad_15v2_gm_nlp_mejorado.ipynb"),
    (4, "notebooks/fase4/actividad_15v3_xgboost_competidor.ipynb"),
    (4, "notebooks/fase4/actividad_15v4_tcn_competidor.ipynb"),
    (4, "notebooks/fase4/actividad_15v5_gm_nlp_v2.ipynb"),
    (4, "notebooks/fase4/actividad_15v6_gm_v4.ipynb"),
    (4, "notebooks/fase4/actividad_16_shap.ipynb"),
    (4, "notebooks/fase4/actividad_16_reentrenamiento_extendido.ipynb"),
    (4, "notebooks/fase4/actividad_17_reentrenamiento_final.ipynb"),
    (4, "notebooks/fase4/actividad_17_validacion.ipynb"),
    (4, "notebooks/fase4/escenario_nino_2026.ipynb"),
    (4, "notebooks/fase4/impacto_economico_nino_2026.ipynb"),
]


def select(fase: int | None) -> list[tuple[int, str]]:
    return [item for item in PIPELINE if fase is None or item[0] == fase]


def print_order(items: list[tuple[int, str]]) -> None:
    current = None
    for fase, nb in items:
        if fase != current:
            print(f"\n=== FASE {fase} ===")
            current = fase
        exists = "OK " if (ROOT / nb).exists() else "FALTA"
        print(f"  [{exists}] {nb}")


def manual_instructions(items: list[tuple[int, str]]) -> None:
    print("\npapermill NO esta instalado. Instalalo con:  pip install papermill")
    print("...o ejecuta los notebooks MANUALMENTE en este orden")
    print("(abrir en Jupyter y 'Run All', o `jupyter nbconvert --to notebook --execute <nb>`):")
    print_order(items)


def run(items: list[tuple[int, str]]) -> int:
    try:
        import papermill as pm
    except ImportError:
        manual_instructions(items)
        return 1

    failures: list[str] = []
    for fase, nb in items:
        src = ROOT / nb
        if not src.exists():
            print(f"[SKIP] no existe: {nb}")
            failures.append(nb)
            continue
        out = src.with_name(src.stem + "_run.ipynb")
        print(f"\n[FASE {fase}] ejecutando {nb} -> {out.name}")
        try:
            pm.execute_notebook(str(src), str(out), kernel_name="python3")
            print(f"  OK -> {out.relative_to(ROOT)}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR en {nb}: {exc}")
            failures.append(nb)

    print("\n================ RESUMEN ================")
    print(f"Total: {len(items)} | Fallidos/saltados: {len(failures)}")
    if failures:
        for f in failures:
            print(f"  - {f}")
        return 1
    print("Pipeline completo sin errores.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Orquestador reproducible del pipeline (papermill).")
    ap.add_argument("--fase", type=int, choices=[1, 2, 3, 4], help="Ejecutar solo una fase.")
    ap.add_argument("--dry-run", action="store_true", help="Imprimir el orden sin ejecutar.")
    ap.add_argument("--list", action="store_true", help="Listar notebooks detectados y salir.")
    args = ap.parse_args()

    items = select(args.fase)

    if args.list or args.dry_run:
        print(f"Notebooks en orden ({'fase ' + str(args.fase) if args.fase else 'todas las fases'}):")
        print_order(items)
        return 0

    return run(items)


if __name__ == "__main__":
    sys.exit(main())
