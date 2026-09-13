"""Pre-flight dry-run for SARIMA rolling one-step on VAL only.

No TEST 2025 rows are loaded here.
"""

from __future__ import annotations

import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import statsmodels
from statsmodels.tsa.statespace.sarimax import SARIMAX


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "v2_reentrenamiento/data/processed"
AUDIT_DIR = REPO_ROOT / "v2_reentrenamiento/auditorias"
JSON_PATH = AUDIT_DIR / "sarima_rolling_preflight_dry_run.json"
MD_PATH = AUDIT_DIR / "AUDITORIA_PREFLIGHT_SARIMA_ROLLING.md"

CULTIVARS = ("sutil", "dulce")
TARGETS = {"sutil": "produccion_t_sutil", "dulce": "produccion_t_dulce"}
ORDER = (1, 1, 1)
SEASONAL_ORDER = (1, 1, 0, 12)
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

    for cultivar in CULTIVARS:
        target = TARGETS[cultivar]
        frame = _load_train_val(cultivar)
        train = frame[frame["particion"] == "train"].reset_index(drop=True)
        val = frame[frame["particion"] == "val"].reset_index(drop=True)

        model = SARIMAX(
            train[target].astype(float).to_numpy(),
            order=ORDER,
            seasonal_order=SEASONAL_ORDER,
            **FIT_KWARGS,
        )
        fit = model.fit(disp=False, maxiter=500)
        base_params = np.asarray(fit.params, dtype=float).copy()
        current = fit
        rows = []
        unchanged_flags = []
        append_methods = []

        for _, row in val.iterrows():
            forecast = np.asarray(current.forecast(steps=1), dtype=float)
            if forecast.shape != (1,):
                raise ValueError(f"Expected one-step forecast for {cultivar}, got {forecast.shape}.")
            rows.append(
                {
                    "origin_date": _previous_month(row["fecha"]),
                    "target_date": row["fecha"],
                    "forecast_steps": 1,
                    "y_true_observed": float(row[target]),
                    "y_pred_dry_run": float(forecast[0]),
                }
            )
            current = current.append([float(row[target])], refit=False)
            append_methods.append("append(refit=False)")
            unchanged_flags.append(bool(np.allclose(np.asarray(current.params, dtype=float), base_params, rtol=0, atol=0)))

        val_dates = [item["target_date"] for item in rows]
        cultivar_payload[cultivar] = {
            "dataset": str(DATA_DIR / f"master_dataset_{cultivar}_v2.csv"),
            "target": target,
            "order": ORDER,
            "seasonal_order": SEASONAL_ORDER,
            "fit_kwargs": FIT_KWARGS,
            "statsmodels_version": statsmodels.__version__,
            "train_range": f"{train.fecha.iloc[0]}..{train.fecha.iloc[-1]}",
            "train_n": int(len(train)),
            "val_range": f"{val.fecha.iloc[0]}..{val.fecha.iloc[-1]}",
            "val_n": int(len(val)),
            "test_loaded": bool((frame['anio'] >= 2025).any()),
            "params_before": [float(x) for x in base_params],
            "params_after": [float(x) for x in np.asarray(current.params, dtype=float)],
            "params_unchanged_all_steps": all(unchanged_flags),
            "append_methods": sorted(set(append_methods)),
            "dry_run_rows": rows,
        }
        _control(controls, f"{cultivar} TRAIN n=90", len(train) == 90, str(len(train)))
        _control(controls, f"{cultivar} TRAIN 2016-07..2023-12", train.fecha.iloc[0] == "2016-07" and train.fecha.iloc[-1] == "2023-12", f"{train.fecha.iloc[0]}..{train.fecha.iloc[-1]}")
        _control(controls, f"{cultivar} VAL n=12", len(val) == 12, str(len(val)))
        _control(controls, f"{cultivar} VAL 2024-01..2024-12", val.fecha.iloc[0] == "2024-01" and val.fecha.iloc[-1] == "2024-12", f"{val.fecha.iloc[0]}..{val.fecha.iloc[-1]}")
        _control(controls, f"{cultivar} no TEST loaded", not cultivar_payload[cultivar]["test_loaded"], str(cultivar_payload[cultivar]["test_loaded"]))
        _control(controls, f"{cultivar} forecast steps=1 all VAL", all(item["forecast_steps"] == 1 for item in rows), "12 one-step forecasts")
        _control(controls, f"{cultivar} append refit=False all VAL", set(append_methods) == {"append(refit=False)"}, str(sorted(set(append_methods))))
        _control(controls, f"{cultivar} coefficients unchanged", all(unchanged_flags), str(unchanged_flags))
        _control(controls, f"{cultivar} VAL dates exact", val_dates == [f"2024-{month:02d}" for month in range(1, 13)], str(val_dates))

    payload = {
        "status": "approved" if all(item["ok"] for item in controls) else "failed",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "git_status_sb": _git_status(),
        "environment": {
            "python": platform.python_version(),
            "statsmodels": statsmodels.__version__,
            "os": platform.platform(),
            "cpu": platform.processor() or None,
        },
        "protocol": {
            "fit_period": "TRAIN only: 2016-07..2023-12",
            "state_update_period": "VAL only dry-run: 2024-01..2024-12",
            "test_forecast_executed": False,
            "official_metrics_generated": False,
            "order": ORDER,
            "seasonal_order": SEASONAL_ORDER,
            "fit_kwargs": FIT_KWARGS,
            "api": "SARIMAXResults.append(endog, refit=False) after forecast(steps=1)",
        },
        "cultivars": cultivar_payload,
        "controls": controls,
        "all_controls_ok": all(item["ok"] for item in controls),
    }
    JSON_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_md(payload)
    print(json.dumps({"status": payload["status"], "controls": len(controls), "all_controls_ok": payload["all_controls_ok"]}, indent=2))
    if not payload["all_controls_ok"]:
        raise SystemExit(1)


def _load_train_val(cultivar: str) -> pd.DataFrame:
    path = DATA_DIR / f"master_dataset_{cultivar}_v2.csv"
    frame = pd.read_csv(path, encoding="utf-8-sig")
    year_col = frame.columns[0]
    frame = frame.rename(columns={year_col: "anio"})
    rows = frame[frame["anio"] <= 2024].copy()
    rows["fecha"] = rows["anio"].astype(int).astype(str) + "-" + rows["mes"].astype(int).astype(str).str.zfill(2)
    rows = rows.sort_values(["anio", "mes"]).reset_index(drop=True)
    rows["particion"] = "buffer"
    rows.loc[(rows["fecha"] >= "2016-07") & (rows["fecha"] <= "2023-12"), "particion"] = "train"
    rows.loc[(rows["fecha"] >= "2024-01") & (rows["fecha"] <= "2024-12"), "particion"] = "val"
    return rows


def _previous_month(ym: str) -> str:
    ts = pd.Timestamp(ym + "-01") - pd.DateOffset(months=1)
    return ts.strftime("%Y-%m")


def _control(controls: list[dict[str, Any]], name: str, ok: bool, detail: str) -> None:
    controls.append({"name": name, "ok": bool(ok), "detail": detail})


def _git_commit() -> str | None:
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def _git_status() -> str:
    result = subprocess.run(["git", "status", "-sb"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    return (result.stderr.strip() + "\n" + result.stdout.strip()).strip()


def _write_md(payload: dict[str, Any]) -> None:
    lines = [
        "# AUDITORIA PREFLIGHT SARIMA ROLLING",
        "",
        "Dry-run tecnico sobre VAL 2024 solamente. No se cargaron filas TEST 2025, no se generaron metricas oficiales nuevas y no se ejecuto busqueda de hiperparametros.",
        "",
        "## Protocolo probado",
        "",
        "- Fit unico en TRAIN 2016-07..2023-12.",
        "- order=(1,1,1), seasonal_order=(1,1,0,12).",
        "- trend='c', initialization='approximate_diffuse', enforce_stationarity=False, enforce_invertibility=False.",
        "- Para cada mes de VAL: `forecast(steps=1)` y luego `append([y_real], refit=False)`.",
        "- Verificacion: parametros identicos antes/despues de cada append.",
        "",
        "## Resultado",
        "",
        f"- Estado: `{payload['status']}`",
        f"- Controles: {len(payload['controls'])}",
        f"- Statsmodels: {payload['environment']['statsmodels']}",
        "",
        "## Controles",
        "",
        "| Control | Estado | Detalle |",
        "|---|---|---|",
    ]
    for item in payload["controls"]:
        lines.append(f"| {item['name']} | {'OK' if item['ok'] else 'FAIL'} | {item['detail']} |")
    lines.extend(
        [
            "",
            "## Confirmaciones",
            "",
            "- TEST 2025 forecast ejecutado: NO.",
            "- Metricas oficiales nuevas: NO.",
            "- Refit durante VAL: NO.",
            "- Coeficientes congelados: SI.",
        ]
    )
    MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
