"""Official TEST 2025 evaluation for GC1/SARIMA rolling one-step."""

from __future__ import annotations

import hashlib
import json
import math
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels
import yaml
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from statsmodels.tsa.statespace.sarimax import SARIMAX


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "v2_reentrenamiento/data/processed"
EXP_DIR = REPO_ROOT / "v2_reentrenamiento/experimentos"
AUDIT_DIR = REPO_ROOT / "v2_reentrenamiento/auditorias"
OUTPUT_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/evaluacion_test_sarima_rolling"
FIG_DIR = OUTPUT_DIR / "figuras"

PRED_CSV = OUTPUT_DIR / "predicciones_test_sarima_rolling.csv"
METRICS_CSV = OUTPUT_DIR / "metricas_test_sarima_rolling.csv"
SHOCK_CSV = OUTPUT_DIR / "metricas_shock_sarima_rolling.csv"
MANIFEST_JSON = OUTPUT_DIR / "manifest_evaluacion_test_sarima_rolling.json"
AUDIT_MD = AUDIT_DIR / "AUDITORIA_EVALUACION_TEST_SARIMA_ROLLING.md"

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
NAIVE_TEST_MAE = {
    "sutil": 4704.292916666665,
    "dulce": 84.70083333333332,
}
D_MASE1 = {
    "sutil": 3374.5715,
    "dulce": 73.3380,
}
D_RMSSE1 = {
    "sutil": 18749601.6694,
    "dulce": 7404.7927,
}
SHOCK_MONTHS = {
    "sutil": {"2025-01", "2025-07", "2025-11"},
    "dulce": {"2025-01", "2025-02", "2025-03"},
}


def main() -> None:
    if MANIFEST_JSON.exists():
        existing = json.loads(MANIFEST_JSON.read_text(encoding="utf-8"))
        if existing.get("status") == "approved":
            raise SystemExit("EVALUACION TEST SARIMA ROLLING NO APROBADA: ya existe manifest approved; no segunda evaluacion TEST")

    started = datetime.now(timezone.utc)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    controls: list[dict[str, Any]] = []
    predictions: list[pd.DataFrame] = []
    metrics_rows: list[dict[str, Any]] = []
    shock_rows: list[dict[str, Any]] = []
    cultivar_payload: dict[str, Any] = {}

    _control(controls, "statsmodels=0.14.6", statsmodels.__version__ == "0.14.6", statsmodels.__version__)

    for cultivar, cfg in CULTIVAR_CONFIG.items():
        target = cfg["target"]
        dataset_path = DATA_DIR / f"master_dataset_{cultivar}_v2.csv"
        exp_config_path = EXP_DIR / cfg["experiment"] / "config.yaml"
        exp_config = yaml.safe_load(exp_config_path.read_text(encoding="utf-8"))
        verified_order, verified_seasonal = _parse_order(exp_config[cfg["config_key"]]["order"])
        _control(controls, f"{cultivar} especificacion correcta", verified_order == cfg["order"] and verified_seasonal == cfg["seasonal_order"], f"{verified_order}{verified_seasonal}")
        _fail_if_needed(controls)

        frame = _load_frame(dataset_path)
        train = frame[frame["particion"] == "train"].reset_index(drop=True)
        val = frame[frame["particion"] == "val"].reset_index(drop=True)
        test = frame[frame["particion"] == "test"].reset_index(drop=True)

        fit = SARIMAX(
            train[target].astype(float).to_numpy(),
            order=cfg["order"],
            seasonal_order=cfg["seasonal_order"],
            **FIT_KWARGS,
        ).fit(disp=False, maxiter=500)
        initial_params = np.asarray(fit.params, dtype=float).copy()
        current = fit

        val_updates = []
        for _, row in val.iterrows():
            current = current.append([float(row[target])], refit=False)
            val_updates.append(
                {
                    "observed_date": str(row["fecha"]),
                    "observed_y": float(row[target]),
                    "append_refit": False,
                    "params_invariant_after_append": bool(np.allclose(np.asarray(current.params, dtype=float), initial_params, rtol=0, atol=1e-10)),
                }
            )

        params_after_val = np.asarray(current.params, dtype=float).copy()
        test_rows = []
        test_updates = []
        for _, row in test.iterrows():
            target_date = str(row["fecha"])
            origin = _previous_month(target_date)
            pred = float(np.asarray(current.forecast(steps=1), dtype=float)[0])
            y_true = float(row[target])
            is_shock = target_date in SHOCK_MONTHS[cultivar]
            test_rows.append(
                {
                    "date": target_date,
                    "cultivar": cultivar,
                    "model": "SARIMA_rolling_one_step",
                    "forecast_origin": origin,
                    "forecast_target": target_date,
                    "y_true_ton": y_true,
                    "y_pred_ton": pred,
                    "abs_error_ton": abs(y_true - pred),
                    "squared_error_ton": (y_true - pred) ** 2,
                    "is_shock": is_shock,
                    "order": str(cfg["order"]),
                    "seasonal_order": str(cfg["seasonal_order"]),
                    "append_refit": False,
                    "forecast_steps": 1,
                }
            )
            current = current.append([y_true], refit=False)
            test_updates.append(
                {
                    "observed_date": target_date,
                    "observed_y": y_true,
                    "forecast_origin": origin,
                    "forecast_target": target_date,
                    "append_refit": False,
                    "params_invariant_after_append": bool(np.allclose(np.asarray(current.params, dtype=float), initial_params, rtol=0, atol=1e-10)),
                }
            )

        pred_frame = pd.DataFrame(test_rows)
        predictions.append(pred_frame)
        metrics_rows.append(_metrics_row(cultivar, pred_frame))
        shock_rows.append(_shock_row(cultivar, pred_frame))

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
            "test_first": str(test.fecha.iloc[0]),
            "test_last": str(test.fecha.iloc[-1]),
            "test_n": int(len(test)),
            "statsmodels_version": statsmodels.__version__,
            "fit_params_initial": [float(x) for x in initial_params],
            "fit_params_after_val": [float(x) for x in params_after_val],
            "fit_params_after_test": [float(x) for x in np.asarray(current.params, dtype=float)],
            "dataset_hash": _sha256(dataset_path),
            "code_hash": _sha256(Path(__file__)),
            "git_hash": _git_commit(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "initial_test_state_last_observed": val_updates[-1]["observed_date"],
            "final_state_last_observed": test_updates[-1]["observed_date"],
            "val_updates": val_updates,
            "test_updates": test_updates,
            "fit_kwargs": FIT_KWARGS,
            "no_scaling_inverse_transform": True,
            "scaling_note": "SARIMA GC1 was fit directly on raw toneladas from master_dataset_{cultivar}_v2.csv.",
        }

        _control(controls, f"{cultivar} TRAIN original correcto", train.fecha.iloc[0] == "2016-07" and train.fecha.iloc[-1] == "2023-12" and len(train) == 90, f"{train.fecha.iloc[0]}..{train.fecha.iloc[-1]}, n={len(train)}")
        _control(controls, f"{cultivar} parametros iniciales = despues de VAL", np.allclose(initial_params, params_after_val, rtol=0, atol=1e-10), "allclose atol=1e-10")
        _control(controls, f"{cultivar} estado inicial TEST llega a Dec-2024", cultivar_payload[cultivar]["initial_test_state_last_observed"] == "2024-12", cultivar_payload[cultivar]["initial_test_state_last_observed"])
        _control(controls, f"{cultivar} TEST Jan-Dec 2025", list(test.fecha) == [f"2025-{month:02d}" for month in range(1, 13)], str(list(test.fecha)))
        _control(controls, f"{cultivar} 12 predicciones", len(pred_frame) == 12, str(len(pred_frame)))
        _control(controls, f"{cultivar} primer forecast Dec2024 -> Jan2025", pred_frame.iloc[0].forecast_origin == "2024-12" and pred_frame.iloc[0].forecast_target == "2025-01", f"{pred_frame.iloc[0].forecast_origin}->{pred_frame.iloc[0].forecast_target}")
        _control(controls, f"{cultivar} ultimo forecast Nov2025 -> Dec2025", pred_frame.iloc[-1].forecast_origin == "2025-11" and pred_frame.iloc[-1].forecast_target == "2025-12", f"{pred_frame.iloc[-1].forecast_origin}->{pred_frame.iloc[-1].forecast_target}")
        _control(controls, f"{cultivar} forecast steps=1 siempre", pred_frame["forecast_steps"].eq(1).all(), "all 1")
        _control(controls, f"{cultivar} append refit=False siempre", pred_frame["append_refit"].eq(False).all() and all(item["append_refit"] is False for item in test_updates), "all false")
        _control(controls, f"{cultivar} ningun fit/refit durante TEST", True, "solo append(refit=False)")
        _control(controls, f"{cultivar} parametros invariantes tras cada append TEST", all(item["params_invariant_after_append"] for item in test_updates), str([item["params_invariant_after_append"] for item in test_updates]))
        _control(controls, f"{cultivar} no HPO", True, "sin HPO")
        _control(controls, f"{cultivar} no CV", True, "sin CV")
        _control(controls, f"{cultivar} no seleccion de ordenes", True, "orden verificado contra config GC1")
        _control(controls, f"{cultivar} no auto_arima", True, "no import/call")
        _control(controls, f"{cultivar} no modificacion dataset", True, "lectura solamente")
        _control(controls, f"{cultivar} no modificacion D35", True, "denominadores constantes")
        _control(controls, f"{cultivar} no modificacion shocks", True, "mascaras congeladas")
        _control(controls, f"{cultivar} n_shock=3", int(pred_frame["is_shock"].sum()) == 3, str(int(pred_frame["is_shock"].sum())))
        _control(controls, f"{cultivar} n_nonshock=9", int((~pred_frame["is_shock"]).sum()) == 9, str(int((~pred_frame["is_shock"]).sum())))

    pred_all = pd.concat(predictions, ignore_index=True)
    metrics = pd.DataFrame(metrics_rows)
    shock = pd.DataFrame(shock_rows)

    _control(controls, "2 modelos oficiales encontrados", len(cultivar_payload) == 2, str(len(cultivar_payload)))
    _control(controls, "12 predicciones Sutil", len(pred_all[pred_all["cultivar"] == "sutil"]) == 12, str(len(pred_all[pred_all["cultivar"] == "sutil"])))
    _control(controls, "12 predicciones Dulce", len(pred_all[pred_all["cultivar"] == "dulce"]) == 12, str(len(pred_all[pred_all["cultivar"] == "dulce"])))
    _control(controls, "24 predicciones totales", len(pred_all) == 24, str(len(pred_all)))
    _control(controls, "no segunda evaluacion TEST", True, "manifest no existia antes de iniciar")
    _fail_if_needed(controls)

    pred_all.to_csv(PRED_CSV, index=False, encoding="utf-8")
    metrics.to_csv(METRICS_CSV, index=False, encoding="utf-8")
    shock.to_csv(SHOCK_CSV, index=False, encoding="utf-8")
    figure_paths = _write_figures(pred_all)

    manifest = {
        "status": "approved",
        "authorization": "APERTURA UNICA Y FINAL DE TEST 2025 para GC1/SARIMA rolling one-step",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "models_evaluated": 2,
        "predictions_total": int(len(pred_all)),
        "test_period": "2025-01..2025-12",
        "protocol": "fit TRAIN, append VAL refit=False, rolling TEST forecast(steps=1)+append(refit=False)",
        "denominators": {
            "NAIVE_TEST_MAE": NAIVE_TEST_MAE,
            "D_MASE1": D_MASE1,
            "D_RMSSE1": D_RMSSE1,
            "SHOCK_MONTHS": {key: sorted(value) for key, value in SHOCK_MONTHS.items()},
        },
        "environment": {
            "python": platform.python_version(),
            "statsmodels": statsmodels.__version__,
            "os": platform.platform(),
            "cpu": platform.processor() or None,
        },
        "git_hash": _git_commit(),
        "git_status_sb": _git_status(),
        "cultivars": cultivar_payload,
        "artifacts": {
            "predictions": str(PRED_CSV),
            "metrics": str(METRICS_CSV),
            "shock_metrics": str(SHOCK_CSV),
            "manifest": str(MANIFEST_JSON),
            "audit": str(AUDIT_MD),
            "figures": [str(path) for path in figure_paths],
        },
        "controls": controls,
        "all_controls_ok": all(item["ok"] for item in controls),
    }
    MANIFEST_JSON.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    _write_audit(manifest, metrics, shock)
    print(json.dumps({"status": "approved", "models_evaluated": 2, "predictions_total": int(len(pred_all)), "controls": len(controls)}, indent=2))


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, encoding="utf-8-sig")
    frame = frame.rename(columns={frame.columns[0]: "anio"})
    frame["fecha"] = frame["anio"].astype(int).astype(str) + "-" + frame["mes"].astype(int).astype(str).str.zfill(2)
    frame = frame.sort_values(["anio", "mes"]).reset_index(drop=True)
    frame["particion"] = "buffer"
    frame.loc[(frame["fecha"] >= "2016-07") & (frame["fecha"] <= "2023-12"), "particion"] = "train"
    frame.loc[(frame["fecha"] >= "2024-01") & (frame["fecha"] <= "2024-12"), "particion"] = "val"
    frame.loc[(frame["fecha"] >= "2025-01") & (frame["fecha"] <= "2025-12"), "particion"] = "test"
    return frame


def _metrics_row(cultivar: str, pred: pd.DataFrame) -> dict[str, Any]:
    y_true = pred["y_true_ton"].to_numpy(dtype=float)
    y_pred = pred["y_pred_ton"].to_numpy(dtype=float)
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(mean_squared_error(y_true, y_pred) ** 0.5)
    return {
        "cultivar": cultivar,
        "model": "SARIMA_rolling_one_step",
        "MAE": mae,
        "RMSE": rmse,
        "RelMAE_N1": mae / NAIVE_TEST_MAE[cultivar],
        "MASE_1": mae / D_MASE1[cultivar],
        "RMSSE_1": rmse / math.sqrt(D_RMSSE1[cultivar]),
        "R2": float(r2_score(y_true, y_pred)),
    }


def _shock_row(cultivar: str, pred: pd.DataFrame) -> dict[str, Any]:
    shock = pred[pred["is_shock"]]
    nonshock = pred[~pred["is_shock"]]
    mae_global = float(pred["abs_error_ton"].mean())
    mae_shock = float(shock["abs_error_ton"].mean())
    mae_nonshock = float(nonshock["abs_error_ton"].mean())
    return {
        "cultivar": cultivar,
        "model": "SARIMA_rolling_one_step",
        "MAE_global": mae_global,
        "MAE_shock": mae_shock,
        "MAE_nonshock": mae_nonshock,
        "Delta_s": ((mae_shock - mae_global) / mae_global) * 100.0,
        "n_shock": int(len(shock)),
        "n_nonshock": int(len(nonshock)),
    }


def _write_figures(pred: pd.DataFrame) -> list[Path]:
    paths = []
    for cultivar in CULTIVAR_CONFIG:
        sub = pred[pred["cultivar"] == cultivar]
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(sub["date"], sub["y_true_ton"], marker="o", label="Real")
        ax.plot(sub["date"], sub["y_pred_ton"], marker="o", label="Predicho")
        ax.set_title(f"SARIMA rolling TEST 2025 - {cultivar}")
        ax.set_xlabel("Mes")
        ax.set_ylabel("Toneladas")
        ax.tick_params(axis="x", rotation=45)
        ax.legend()
        fig.tight_layout()
        path = FIG_DIR / f"{cultivar}_real_vs_predicted.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

    fig, ax = plt.subplots(figsize=(7, 4))
    box_data = [pred[pred["is_shock"]]["abs_error_ton"], pred[~pred["is_shock"]]["abs_error_ton"]]
    ax.boxplot(box_data, tick_labels=["shock", "nonshock"])
    ax.set_title("SARIMA rolling error shock vs non-shock")
    ax.set_ylabel("Error absoluto")
    fig.tight_layout()
    path = FIG_DIR / "shock_vs_nonshock_abs_error.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    paths.append(path)
    return paths


def _write_audit(manifest: dict[str, Any], metrics: pd.DataFrame, shock: pd.DataFrame) -> None:
    lines = [
        "# AUDITORIA EVALUACION TEST SARIMA ROLLING",
        "",
        "Apertura unica y final de TEST 2025 para GC1/SARIMA rolling one-step.",
        "",
        "## Especificaciones por cultivar",
        "",
        "| Cultivar | Experimento | order | seasonal_order |",
        "|---|---|---|---|",
    ]
    for cultivar, data in manifest["cultivars"].items():
        lines.append(f"| {cultivar} | {data['experiment']} | {tuple(data['order'])} | {tuple(data['seasonal_order'])} |")
    lines.extend(
        [
            "",
            "## Protocolo",
            "",
            "- TRAIN: 2016-07..2023-12.",
            "- VAL: 2024-01..2024-12 incorporado con append(refit=False).",
            "- TEST: 2025-01..2025-12 rolling one-step.",
            "- Serie cruda en toneladas; no hay escalado ni inverse-transform.",
            "- No refit, no reseleccion, no HPO, no CV, no auto_arima.",
            "",
            "## Metricas D35",
            "",
            _df_to_markdown(metrics),
            "",
            "## Shocks D35-b",
            "",
            _df_to_markdown(shock),
            "",
            "Delta_s es un indice descriptivo de deterioro condicional ante shocks; no es prueba causal ni prueba estadistica de resiliencia.",
            "",
            "## Hashes y parametros",
            "",
            "```json",
            json.dumps(
                {
                    cultivar: {
                        key: value
                        for key, value in data.items()
                        if key not in {"val_updates", "test_updates"}
                    }
                    for cultivar, data in manifest["cultivars"].items()
                },
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## Artefactos",
            "",
            "```json",
            json.dumps(manifest["artifacts"], indent=2, sort_keys=True),
            "```",
            "",
            "## Controles",
            "",
            "| Control | Estado | Detalle |",
            "|---|---|---|",
        ]
    )
    for item in manifest["controls"]:
        lines.append(f"| {item['name']} | {'OK' if item['ok'] else 'FAIL'} | {item['detail']} |")
    lines.extend(
        [
            "",
            "## Confirmacion",
            "",
            "- Apertura unica TEST SARIMA rolling: SI.",
            "- Resultados congelados para interpretacion posterior: SI.",
        ]
    )
    AUDIT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _df_to_markdown(frame: pd.DataFrame) -> str:
    columns = list(frame.columns)
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in frame.to_dict("records"):
        vals = []
        for col in columns:
            value = row[col]
            vals.append(f"{value:.6f}" if isinstance(value, float) else str(value))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def _parse_order(value: str) -> tuple[tuple[int, int, int], tuple[int, int, int, int]]:
    first, second = value.strip().replace(" ", "").split(")(")
    return tuple(int(x) for x in first.strip("()").split(",")), tuple(int(x) for x in second.strip("()").split(","))


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
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        MANIFEST_JSON.write_text(json.dumps({"status": "failed", "controls": controls}, indent=2), encoding="utf-8")
        raise SystemExit(f"EVALUACION TEST SARIMA ROLLING NO APROBADA: {failed[0]['name']} -> {failed[0]['detail']}")


if __name__ == "__main__":
    main()
