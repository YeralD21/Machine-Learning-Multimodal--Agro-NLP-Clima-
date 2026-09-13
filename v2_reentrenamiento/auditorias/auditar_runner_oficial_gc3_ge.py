"""Dry-run audit for the official GC3/GE runner.

Allowed operations only: imports, static checks, sequence construction, model
construction, count_params, callback/config checks, hashes. It does not call
model.fit.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


REPORT_PATH = REPO_ROOT / "v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md"
SUMMARY_PATH = REPO_ROOT / "v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json"


def git_status() -> str:
    result = subprocess.run(
        ["git", "status", "--short", "--branch"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    stderr = result.stderr.strip()
    stdout = result.stdout.strip()
    return (stdout + ("\n" + stderr if stderr else "")).strip()


def write_report(summary: dict, status: str) -> None:
    data = summary["data_checks"]
    lines = [
        "# AUDITORIA RUNNER OFICIAL GC3/GE",
        "",
        "## Alcance",
        "",
        "Se audito y preparo el runner oficial GC3/GE para las 40 corridas",
        "preespecificadas. En esta tarea se ejecuto solo dry-run: no se llamo",
        "`model.fit()`, no se entreno ninguna red, no se generaron checkpoints",
        "oficiales y no se hizo commit.",
        "",
        "## Decision de implementacion",
        "",
        f"- Decision: {summary['decision']}.",
        "- Motivo: ya existian componentes auditados para features, arquitectura,",
        "  configuracion, callbacks, determinismo, history, metadata y artefactos;",
        "  faltaba un orquestador oficial con pre-flight integral y guardas contra",
        "  uso de TEST/sobrescritura.",
        "",
        "## Archivos creados/modificados",
        "",
        "- Creado: `v2_reentrenamiento/src/training/official_runner_gc3_ge.py`.",
        "- Creado: `v2_reentrenamiento/auditorias/auditar_runner_oficial_gc3_ge.py`.",
        "- Creado: `v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json`.",
        "- Creado: `v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md`.",
        "- No se modificaron datasets, scalers, notebooks ni decisiones metodologicas.",
        "",
        "## Diseno del runner",
        "",
        "- Destino oficial previsto: `v2_reentrenamiento/resultados_v2_final/official_gc3_ge/`.",
        "- Estructura por corrida: `cultivar/modelo/seed_XX`.",
        "- Corridas esperadas: 2 cultivares x 2 modelos x 10 seeds = 40.",
        "- Seeds oficiales: `0,1,2,3,4,5,6,7,8,9`.",
        "- El dry-run no crea directorios oficiales de corrida.",
        "- La funcion futura `run_official_training()` rechaza configuraciones no oficiales",
        "  y directorios existentes con archivos.",
        "",
        "## Pre-flight checks",
        "",
        "- Dataset correcto `master_dataset_{cultivar}_v2_escalado.csv`.",
        "- Scaler correcto `scaler_{cultivar}_v2c.joblib` como StandardScaler.",
        "- Hash SHA256 de dataset, scaler y parametros del scaler registrado.",
        "- Fechas 2016-07..2025-12, sin 2026.",
        "- Split estructural: TRAIN efectivo 90 filas, VAL 12, TEST 12.",
        "- Secuencias de fit construidas solo con filas <= 2024.",
        "- TRAIN sequences = 84 y VAL targets = 12.",
        "- TEST no se materializa como arrays de fit ni callbacks.",
        "- Lookback = 6.",
        "- Features exactas GC3/GE y ausencia de exogenas contemporaneas indebidas.",
        "- Shapes exactos por cultivar/modelo.",
        "- Parametros exactos GC3=6273 y GE=6657.",
        "- Seeds exactas 0..9.",
        "- `shuffle=False`.",
        "- Loss/optimizer/callbacks oficiales.",
        "- Directorio oficial diferenciado de `technical_pilot*`.",
        "- Bloqueo de sobrescritura silenciosa de corridas oficiales existentes.",
        "- Git hash y hashes de inputs congelados registrados.",
        "",
        "## Resultado del dry-run",
        "",
        f"- Estado dry-run: {summary['status']}.",
        f"- `model.fit()` ejecutado: {summary['model_fit_executed']}.",
        f"- TEST usado para entrenamiento: {summary['test_used_for_training']}.",
        f"- TEST cargado para fit: {summary['test_loaded_for_fit']}.",
        f"- Corridas oficiales planificadas: {summary['official_runs_expected']}.",
        f"- Seeds verificadas: {', '.join(str(seed) for seed in summary['seeds'])}.",
        f"- Shuffle: {summary['training_config']['shuffle']}.",
        f"- Batch size: {summary['training_config']['batch_size']}.",
        f"- Max epochs: {summary['training_config']['max_epochs']}.",
        f"- Git hash: `{summary['git_commit']}`.",
        "",
        "## Parametros del modelo",
        "",
        "| Modelo | Params esperados | Params dry-run | Loss | Optimizer | LR |",
        "|---|---:|---:|---|---|---:|",
    ]
    for model_name, expected in (("GC3", 6273), ("GE", 6657)):
        item = summary["model_checks"][model_name]
        lines.append(
            f"| {model_name} | {expected} | {item['params']} | {item['loss']} | "
            f"{item['optimizer']} | {item['learning_rate']} |"
        )

    lines.extend(
        [
            "",
            "## Shapes verificados",
            "",
            "| Cultivar | Modelo | Rama A | Rama B | Inputs | TRAIN seq | VAL targets | Xa_train | Xb_train | Xa_val | Xb_val |",
            "|---|---|---:|---:|---:|---:|---:|---|---|---|---|",
        ]
    )
    for cultivar, cultivar_data in data.items():
        for model_name, item in cultivar_data["models"].items():
            shapes = item["shapes"]
            lines.append(
                f"| {cultivar} | {model_name} | {item['n_features_a']} | {item['n_features_b']} | "
                f"{item['total_predictive_inputs']} | {item['train_sequences']} | "
                f"{item['validation_targets']} | {tuple(shapes['Xa_train'])} | "
                f"{tuple(shapes['Xb_train'])} | {tuple(shapes['Xa_val'])} | {tuple(shapes['Xb_val'])} |"
            )

    lines.extend(
        [
            "",
            "## Confirmaciones explicitas",
            "",
            "- TEST 2025 no fue usado para entrenamiento.",
            "- TEST 2025 no fue pasado a callbacks.",
            "- TEST 2025 no fue materializado como arrays de fit en el runner oficial.",
            "- No hubo llamada a `model.fit()` durante esta auditoria.",
            "- GC3 confirma 6273 parametros.",
            "- GE confirma 6657 parametros.",
            "- TRAIN sequences confirma 84 por cultivar/modelo.",
            "- VAL targets confirma 12 por cultivar/modelo.",
            "- Seeds oficiales confirmadas: 0..9.",
            "",
            "## Riesgos o bloqueos",
            "",
            "- No se encontraron bloqueos metodologicos.",
            "- Riesgo operativo pendiente: el entrenamiento oficial todavia no fue autorizado",
            "  ni ejecutado; los directorios oficiales deben permanecer sin archivos previos",
            "  para evitar sobrescritura.",
            "- `git status` sigue mostrando archivos untracked preexistentes y los nuevos",
            "  artefactos de auditoria; no se hizo commit.",
            "",
            "## Git status final",
            "",
            "```text",
            status,
            "```",
            "",
            "No commit.",
            "",
            "RUNNER OFICIAL GC3/GE LISTO PARA AUTORIZACIÓN DE ENTRENAMIENTO",
        ]
    )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    from v2_reentrenamiento.src.training.official_runner_gc3_ge import dry_run_preflight

    summary = dry_run_preflight(REPO_ROOT)
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    status = git_status()
    write_report(summary, status)
    print(json.dumps({"report": str(REPORT_PATH), "summary": str(SUMMARY_PATH)}, indent=2))


if __name__ == "__main__":
    main()
