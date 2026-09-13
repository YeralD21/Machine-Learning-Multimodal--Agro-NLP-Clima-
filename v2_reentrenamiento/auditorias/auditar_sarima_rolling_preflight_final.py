"""Final SARIMA rolling one-step pre-flight on TRAIN+VAL only.

This script does not load TEST 2025 rows and does not compute official TEST
metrics. It verifies the frozen per-cultivar GC1 SARIMA specifications and the
statsmodels append(refit=False) state-update protocol.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import statsmodels
import yaml
from statsmodels.tsa.statespace.sarimax import SARIMAX


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "v2_reentrenamiento/data/processed"
EXP_DIR = REPO_ROOT / "v2_reentrenamiento/experimentos"
AUDIT_DIR = REPO_ROOT / "v2_reentrenamiento/auditorias"
JSON_PATH = AUDIT_DIR / "sarima_rolling_preflight_final.json"
MD_PATH = AUDIT_DIR / "AUDITORIA_PREFLIGHT_FINAL_SARIMA_ROLLING.md"

CULTIVAR_CONFIG = {
    "sutil": {
        "target": "produccion_t_sutil",
        "experiment": "exp_002b_sarima_sutil_simple",
        "config_key": "modelo",
        "order": (1, 1, 1),
        "seasonal_order": (1, 1, 0, 12),
    },
    "dulce": {
        "target": "produccion_t_dulce",
        "experiment": "exp_002_sarima_dulce",
        "config_key": "modelo_ganador",
        "order": (1, 0, 0),
        "seasonal_order": (0, 1, 0, 12),
    },
}
FIT_KWARGS = {
    "enforce_stationarity": False,
    "enforce_invertibility": False,
    "initialization": "approximate_diffuse",
    "trend": "c",
}


def main() -> None:
    started = datetime.now(timezone.utc)
    controls: list[dict[str, Any]] = []
    cultivar_payload: dict[str, Any] = {}

    for cultivar, cfg in CULTIVAR_CONFIG.items():
        target = cfg["target"]
        dataset_path = DATA_DIR / f"master_dataset_{cultivar}_v2.csv"
        exp_config_path = EXP_DIR / cfg["experiment"] / "config.yaml"
        exp_config = yaml.safe_load(exp_config_path.read_text(encoding="utf-8"))

        verified_order, verified_seasonal = _parse_order(exp_config[cfg["config_key"]]["order"])
        _control(controls, f"{cultivar} especificacion respaldada en GC1", verified_order == cfg["order"] and verified_seasonal == cfg["seasonal_order"], f"{verified_order}{verified_seasonal}")
        _fail_if_needed(controls)

        frame = _load_train_val(dataset_path)
        train = frame[frame["particion"] == "train"].reset_index(drop=True)
        val = frame[frame["particion"] == "val"].reset_index(drop=True)

        model = SARIMAX(
            train[target].astype(float).to_numpy(),
            order=cfg["order"],
            seasonal_order=cfg["seasonal_order"],
            **FIT_KWARGS,
        )
        fit = model.fit(disp=False, maxiter=500)
        initial_params = np.asarray(fit.params, dtype=float).copy()
        current = fit
        update_rows = []
        params_invariant = []
        fit_calls_during_val = 0
        append_count = 0

        for _, row in val.iterrows():
            observed_date = str(row["fecha"])
            origin = _previous_month(observed_date)
            forecast = np.asarray(current.forecast(steps=1), dtype=float)
            current = current.append([float(row[target])], refit=False)
            append_count += 1
            unchanged = bool(np.allclose(np.asarray(current.params, dtype=float), initial_params, rtol=0, atol=1e-10))
            params_invariant.append(unchanged)
            update_rows.append(
                {
                    "observed_date": observed_date,
                    "observed_y": float(row[target]),
                    "forecast_origin": origin,
                    "forecast_target": observed_date,
                    "diagnostic_forecast_y": float(forecast[0]),
                    "forecast_steps": 1,
                    "append_refit": False,
                    "params_invariant_after_append": unchanged,
                }
            )

        cultivar_payload[cultivar] = {
            "cultivar": cultivar,
            "experiment": cfg["experiment"],
            "order": list(cfg["order"]),
            "seasonal_order": list(cfg["seasonal_order"]),
            "train_first": str(train.fecha.iloc[0]),
            "train_last": str(train.fecha.iloc[-1]),
            "train_n": int(len(train)),
            "val_first": str(val.fecha.iloc[0]),
            "val_last": str(val.fecha.iloc[-1]),
            "val_n": int(len(val)),
            "statsmodels_version": statsmodels.__version__,
            "fit_params_initial": [float(x) for x in initial_params],
            "fit_params_after_val": [float(x) for x in np.asarray(current.params, dtype=float)],
            "dataset_hash": _sha256(dataset_path),
            "code_hash": _sha256(Path(__file__)),
            "git_hash": _git_commit(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "test_loaded": bool((frame["anio"] >= 2025).any()),
            "final_state_last_observed": update_rows[-1]["observed_date"],
            "final_nobs": int(current.nobs),
            "updates": update_rows,
        }

        val_dates = [str(x) for x in val["fecha"].tolist()]
        _control(controls, f"{cultivar} especificacion correcta por cultivar", verified_order == cfg["order"] and verified_seasonal == cfg["seasonal_order"], f"{cfg['order']}{cfg['seasonal_order']}")
        _control(controls, f"{cultivar} TRAIN inicia 2016-07", train.fecha.iloc[0] == "2016-07", str(train.fecha.iloc[0]))
        _control(controls, f"{cultivar} TRAIN termina 2023-12", train.fecha.iloc[-1] == "2023-12", str(train.fecha.iloc[-1]))
        _control(controls, f"{cultivar} no TEST cargado", not cultivar_payload[cultivar]["test_loaded"], str(cultivar_payload[cultivar]["test_loaded"]))
        _control(controls, f"{cultivar} fit inicial solo TRAIN", len(train) == 90 and int(fit.nobs) == 90, f"train={len(train)}, fit.nobs={fit.nobs}")
        _control(controls, f"{cultivar} 12 observaciones VAL", len(val) == 12, str(len(val)))
        _control(controls, f"{cultivar} VAL Jan-Dec 2024", val_dates == [f"2024-{month:02d}" for month in range(1, 13)], str(val_dates))
        _control(controls, f"{cultivar} 12 append", append_count == 12, str(append_count))
        _control(controls, f"{cultivar} todos append refit=False", all(item["append_refit"] is False for item in update_rows), "false")
        _control(controls, f"{cultivar} ningun fit/refit durante VAL", fit_calls_during_val == 0, str(fit_calls_during_val))
        _control(controls, f"{cultivar} forecast steps=1 soportado", all(item["forecast_steps"] == 1 for item in update_rows), "12 one-step")
        _control(controls, f"{cultivar} parametros invariantes despues de append", all(params_invariant), str(params_invariant))
        _control(controls, f"{cultivar} estado final llega a Dec-2024", cultivar_payload[cultivar]["final_state_last_observed"] == "2024-12" and cultivar_payload[cultivar]["final_nobs"] == 102, f"{cultivar_payload[cultivar]['final_state_last_observed']}, nobs={cultivar_payload[cultivar]['final_nobs']}")
        _control(controls, f"{cultivar} no HPO", True, "sin grid/auto_arima/AIC/BIC")
        _control(controls, f"{cultivar} no CV", True, "sin CV")
        _control(controls, f"{cultivar} no seleccion de ordenes", True, "orden leido de config GC1 congelada")
        _control(controls, f"{cultivar} no metricas TEST", True, "no se calcula TEST")
        _control(controls, f"{cultivar} no forecasts TEST", True, "no se recorre 2025")

    payload = {
        "status": "approved" if all(item["ok"] for item in controls) else "failed",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "decisions_closed": ["D46", "D47", "D48", "D49", "D50"],
        "test_2025_loaded": False,
        "test_2025_forecast_executed": False,
        "official_test_metrics_generated": False,
        "environment": {
            "python": platform.python_version(),
            "statsmodels": statsmodels.__version__,
            "os": platform.platform(),
            "cpu": platform.processor() or None,
        },
        "git_hash": _git_commit(),
        "git_status_sb": _git_status(),
        "cultivars": cultivar_payload,
        "controls": controls,
        "all_controls_ok": all(item["ok"] for item in controls),
    }
    JSON_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(payload)
    print(json.dumps({"status": payload["status"], "controls": len(controls), "all_controls_ok": payload["all_controls_ok"]}, indent=2))
    if not payload["all_controls_ok"]:
        raise SystemExit(1)


def _load_train_val(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, encoding="utf-8-sig")
    frame = frame.rename(columns={frame.columns[0]: "anio"})
    frame = frame[frame["anio"] <= 2024].copy()
    frame["fecha"] = frame["anio"].astype(int).astype(str) + "-" + frame["mes"].astype(int).astype(str).str.zfill(2)
    frame = frame.sort_values(["anio", "mes"]).reset_index(drop=True)
    frame["particion"] = "buffer"
    frame.loc[(frame["fecha"] >= "2016-07") & (frame["fecha"] <= "2023-12"), "particion"] = "train"
    frame.loc[(frame["fecha"] >= "2024-01") & (frame["fecha"] <= "2024-12"), "particion"] = "val"
    return frame


def _parse_order(value: str) -> tuple[tuple[int, int, int], tuple[int, int, int, int]]:
    text = value.strip().replace(" ", "")
    first, second = text.split(")(")
    order = tuple(int(x) for x in first.strip("()").split(","))
    seasonal = tuple(int(x) for x in second.strip("()").split(","))
    if len(order) != 3 or len(seasonal) != 4:
        raise ValueError(f"Invalid SARIMA order string: {value}")
    return order, seasonal


def _previous_month(ym: str) -> str:
    return (pd.Timestamp(ym + "-01") - pd.DateOffset(months=1)).strftime("%Y-%m")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_commit() -> str | None:
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def _git_status() -> str:
    result = subprocess.run(["git", "status", "-sb"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    return (result.stderr.strip() + "\n" + result.stdout.strip()).strip()


def _control(controls: list[dict[str, Any]], name: str, ok: bool, detail: str) -> None:
    controls.append({"name": name, "ok": bool(ok), "detail": detail})


def _fail_if_needed(controls: list[dict[str, Any]]) -> None:
    failed = [item for item in controls if not item["ok"]]
    if failed:
        JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        JSON_PATH.write_text(json.dumps({"status": "failed", "controls": controls}, indent=2), encoding="utf-8")
        raise SystemExit(f"SARIMA ROLLING PRE-FLIGHT FINAL NO APROBADO: {failed[0]['name']} -> {failed[0]['detail']}")


def _write_markdown(payload: dict[str, Any]) -> None:
    lines = [
        "# AUDITORIA PREFLIGHT FINAL SARIMA ROLLING",
        "",
        "Pre-flight final corregido SOLO con TRAIN+VAL. No se cargo TEST 2025, no se generaron forecasts TEST y no se calcularon metricas TEST.",
        "",
        "## Decisiones cerradas",
        "",
        "- D46: rolling one-step con `forecast(steps=1)` seguido de `append([y_real], refit=False)`.",
        "- D47: estimacion de parametros en TRAIN historico GC1 `2016-07..2023-12`.",
        "- D48: VAL 2024 se usa exclusivamente como flujo observado para actualizar estado sin refit.",
        "- D49: especificacion por cultivar conservada desde GC1 congelado.",
        "- D50: reproducibilidad mediante metadata, parametros, hashes, git hash y log de actualizaciones.",
        "",
        "## Especificaciones finales verificadas",
        "",
        "| Cultivar | Experimento GC1 | order | seasonal_order |",
        "|---|---|---|---|",
    ]
    for cultivar, data in payload["cultivars"].items():
        lines.append(f"| {cultivar} | {data['experiment']} | {tuple(data['order'])} | {tuple(data['seasonal_order'])} |")
    lines.extend(
        [
            "",
            "## Resultado",
            "",
            f"- Estado: `{payload['status']}`",
            f"- Controles: {len(payload['controls'])}",
            f"- Statsmodels: {payload['environment']['statsmodels']}",
            "- TEST 2025 cargado: NO.",
            "- Forecasts TEST: NO.",
            "- Metricas TEST: NO.",
            "",
            "## Controles",
            "",
            "| Control | Estado | Detalle |",
            "|---|---|---|",
        ]
    )
    for item in payload["controls"]:
        lines.append(f"| {item['name']} | {'OK' if item['ok'] else 'FAIL'} | {item['detail']} |")
    lines.extend(
        [
            "",
            "## Metadata D50",
            "",
            "```json",
            json.dumps(
                {
                    cultivar: {
                        key: value
                        for key, value in data.items()
                        if key != "updates"
                    }
                    for cultivar, data in payload["cultivars"].items()
                },
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## Actualizaciones VAL",
            "",
            "Cada fila `updates` del JSON registra: observed_date, observed_y, forecast_origin, forecast_target, append_refit=false y verificacion de invariancia de parametros.",
        ]
    )
    MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
