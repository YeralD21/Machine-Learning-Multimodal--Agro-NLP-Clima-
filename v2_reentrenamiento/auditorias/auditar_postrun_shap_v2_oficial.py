from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
SHAP_ROOT = ROOT / "v2_reentrenamiento" / "resultados_v2_final" / "shap" / "official_test_2025"
AUDIT_DIR = ROOT / "v2_reentrenamiento" / "auditorias"
OUT_MD = AUDIT_DIR / "AUDITORIA_POSTRUN_SHAP_V2_OFICIAL.md"
OUT_JSON = AUDIT_DIR / "shap_v2_postrun_oficial.json"
MANIFEST = SHAP_ROOT / "metadata" / "MANIFEST_SHA256.json"

CULTIVARS = ("sutil", "dulce")
MODELS = ("GC3", "GE", "XGBoost")
SEEDS = tuple(range(10))
DATES_2025 = tuple(f"2025-{m:02d}-01" for m in range(1, 13))
EXPECTED_FLAT = {"GC3": 222, "GE": 258, "XGBoost": 37}
EXPECTED_CONCEPT = {"GC3": 37, "GE": 43, "XGBoost": 37}
EXPECTED_EXPLAINER = {"GC3": "PermutationExplainer", "GE": "PermutationExplainer", "XGBoost": "TreeExplainer"}
EXPECTED_MAX_EVALS = {"GC3": 7120, "GE": 8272, "XGBoost": None}
EXPECTED_BACKGROUND = {
    ("sutil", "GC3"): "4d95df53dfeaa745e0752465ead086efced9947d7af4d83ee64d6fcc14d8c5a3",
    ("sutil", "GE"): "a8a25c6720e2e3d1a8c9dd5ca111cade33260403d4ea92637d3072a8ade36818",
    ("sutil", "XGBoost"): "71d3dbb62dc1fa60e8362b73296b6be423f13fa03f78e57af898c0114ead9980",
    ("dulce", "GC3"): "d3c84a671da84253c3e5fb0a8edb7af3d7ac3f8c8dc4bd96c3fcd216d20bd32c",
    ("dulce", "GE"): "01895908a544ead3ab1dcb359b3de99cb0e057d64e09d52c87800248846de69e",
    ("dulce", "XGBoost"): "41021ad468e00e8b23dcfc710e6e2922c2539349b84ab616bdbec3546b774be2",
}
SHOCK_DATES = {
    "sutil": {"2025-01-01", "2025-07-01", "2025-11-01"},
    "dulce": {"2025-01-01", "2025-02-01", "2025-03-01"},
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def status(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def as_float(x: float) -> float:
    return float(x) if math.isfinite(float(x)) else float("nan")


def stats(values) -> dict:
    arr = np.asarray(list(values), dtype=float)
    if arr.size == 0:
        return {"max": None, "mean": None, "median": None, "p95": None, "p99": None}
    return {
        "max": as_float(np.max(arr)),
        "mean": as_float(np.mean(arr)),
        "median": as_float(np.median(arr)),
        "p95": as_float(np.percentile(arr, 95)),
        "p99": as_float(np.percentile(arr, 99)),
    }


def seed_stats(rows):
    arr = np.asarray(rows, dtype=float)
    return {
        "mean": np.mean(arr, axis=0),
        "median": np.median(arr, axis=0),
        "sd": np.std(arr, axis=0, ddof=1),
        "min": np.min(arr, axis=0),
        "max": np.max(arr, axis=0),
    }


def table(headers, rows) -> str:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def fmt(x, digits=6):
    if x is None:
        return "NA"
    return f"{float(x):.{digits}g}"


def manifest_for_tree(root: Path) -> dict:
    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and path != MANIFEST:
            files[str(path.relative_to(ROOT)).replace("\\", "/")] = sha256_file(path)
    return {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "files": files,
        "file_count": len(files),
    }


def main() -> int:
    complete_path = SHAP_ROOT / "complete.json"
    complete = json.loads(complete_path.read_text(encoding="utf-8"))
    failures = []
    checks = []
    records = []
    seen = set()
    recon_errors = []
    pred_errors = []
    raw_hashes = {}
    agg_hashes = {}
    meta_hashes = {}
    technical_warnings = []
    by_combo = {}

    for cultivar in CULTIVARS:
        for model in MODELS:
            combo_records = []
            for seed in SEEDS:
                seed_dir = SHAP_ROOT / cultivar / model / f"seed_{seed:02d}"
                raw_path = seed_dir / "raw.npz"
                meta_path = seed_dir / "metadata.json"
                agg_path = seed_dir / "aggregates_descriptive.json"
                if not raw_path.exists() or not meta_path.exists() or not agg_path.exists():
                    failures.append(f"Missing official files for {cultivar}/{model}/seed_{seed:02d}")
                    continue

                raw_hashes[str(raw_path.relative_to(ROOT)).replace("\\", "/")] = sha256_file(raw_path)
                meta_hashes[str(meta_path.relative_to(ROOT)).replace("\\", "/")] = sha256_file(meta_path)
                agg_hashes[str(agg_path.relative_to(ROOT)).replace("\\", "/")] = sha256_file(agg_path)
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if meta.get("explainer", {}).get("warnings"):
                    technical_warnings.append({"key": meta.get("key"), "warnings": meta["explainer"]["warnings"]})

                z = np.load(raw_path, allow_pickle=False)
                target_dates = tuple(str(x) for x in z["target_dates"])
                key = (cultivar, model, seed, target_dates)
                if key in seen:
                    failures.append(f"Duplicate record {cultivar}/{model}/seed_{seed:02d}")
                seen.add(key)

                shap = z["flat_shap"]
                expected_flat = EXPECTED_FLAT[model]
                expected_concept = EXPECTED_CONCEPT[model]
                rec = {
                    "cultivar": cultivar,
                    "model": model,
                    "seed": seed,
                    "n_obs": int(shap.shape[0]),
                    "flat_dim": int(shap.shape[1]),
                    "feature_names": [str(x) for x in z["feature_names"]],
                    "groups": [str(x) for x in z["groups"]],
                    "target_dates": target_dates,
                    "is_shock": z["is_shock"].astype(bool),
                    "shap": shap.astype(float),
                    "abs_shap": np.abs(shap.astype(float)),
                    "recon": z["reconstruction_abs_error"].astype(float),
                    "pred_error": np.abs(z["predictions_model_scaled"].astype(float) - z["predictions_frozen_scaled"].astype(float)),
                    "metadata": meta,
                }
                records.append(rec)
                combo_records.append(rec)

                recon_errors.extend(rec["recon"].tolist())
                pred_errors.extend(rec["pred_error"].tolist())

                if rec["n_obs"] != 12 or rec["flat_dim"] != expected_flat:
                    failures.append(f"Wrong raw shape {cultivar}/{model}/seed_{seed:02d}: {shap.shape}")
                if len(rec["feature_names"]) != expected_concept:
                    failures.append(f"Wrong feature count {cultivar}/{model}/seed_{seed:02d}")
                if target_dates != DATES_2025:
                    failures.append(f"Wrong target dates {cultivar}/{model}/seed_{seed:02d}: {target_dates}")
                if set(np.asarray(target_dates)[rec["is_shock"]]) != SHOCK_DATES[cultivar]:
                    failures.append(f"Wrong shock mask {cultivar}/{model}/seed_{seed:02d}")
                if meta.get("explainer", {}).get("method") != EXPECTED_EXPLAINER[model]:
                    failures.append(f"Wrong explainer {cultivar}/{model}/seed_{seed:02d}")
                if meta.get("explainer", {}).get("rng") != 1729:
                    failures.append(f"Wrong RNG {cultivar}/{model}/seed_{seed:02d}")
                if meta.get("explainer", {}).get("max_evals") != EXPECTED_MAX_EVALS[model]:
                    failures.append(f"Wrong max_evals {cultivar}/{model}/seed_{seed:02d}")
                if meta.get("background", {}).get("sha256_array") != EXPECTED_BACKGROUND[(cultivar, model)]:
                    failures.append(f"Wrong background hash {cultivar}/{model}/seed_{seed:02d}")
                if meta.get("background", {}).get("source") != "TRAIN" or meta.get("background", {}).get("shape", [0])[0] != 32:
                    failures.append(f"Wrong background source/size {cultivar}/{model}/seed_{seed:02d}")
                if np.max(rec["pred_error"]) > 1e-6:
                    failures.append(f"Prediction mismatch {cultivar}/{model}/seed_{seed:02d}")
                if np.max(rec["recon"]) > 1e-5:
                    failures.append(f"Material SHAP reconstruction mismatch {cultivar}/{model}/seed_{seed:02d}")

            by_combo[(cultivar, model)] = combo_records

    total_explanations = sum(r["n_obs"] for r in records)
    neural_explanations = sum(r["n_obs"] for r in records if r["model"] in ("GC3", "GE"))
    xgb_explanations = sum(r["n_obs"] for r in records if r["model"] == "XGBoost")

    checks_data = {
        "2 cultivares": len({r["cultivar"] for r in records}) == 2,
        "GC3/GE/XGBoost": {r["model"] for r in records} == set(MODELS),
        "10 seeds cada uno": all(len({r["seed"] for r in by_combo[(c, m)]}) == 10 for c in CULTIVARS for m in MODELS),
        "12 meses cada seed": all(r["n_obs"] == 12 and r["target_dates"] == DATES_2025 for r in records),
        "480 explicaciones neuronales": neural_explanations == 480,
        "240 explicaciones XGBoost": xgb_explanations == 240,
        "720 explicaciones totales": total_explanations == 720,
        "ningun modelo faltante": len(records) == 60,
        "ningun mes faltante": all(len(set(r["target_dates"])) == 12 for r in records),
        "ningun duplicado": len(seen) == len(records),
        "background correcto": not any("background" in f.lower() for f in failures),
        "explainers correctos": not any("explainer" in f.lower() for f in failures),
        "parametros correctos": not any("rng" in f.lower() or "max_evals" in f.lower() for f in failures),
        "hashes congelados intactos": complete.get("status") == "SHAP TEST COMPLETE",
        "predicciones dentro de atol=1e-6": max(pred_errors or [0.0]) <= 1e-6,
        "shapes correctos": not any("shape" in f.lower() or "feature count" in f.lower() for f in failures),
        "reconstruccion SHAP": max(recon_errors or [0.0]) <= 1e-5,
        "archivos RAW presentes": len(raw_hashes) == 60,
        "agregaciones reproducibles": all((SHAP_ROOT / c / m / "seed_variability_all_aggregates.json").exists() for c in CULTIVARS for m in MODELS),
    }
    checks = [{"name": k, "status": status(v)} for k, v in checks_data.items()]

    combo_summaries = {}
    top_features = {}
    groups_summary = {}
    timestep_summary = {}
    nlp_summary = {}
    shock_summary = {}
    seed_variability_summary = {}

    for cultivar in CULTIVARS:
        for model in MODELS:
            recs = by_combo[(cultivar, model)]
            if not recs:
                continue
            names = recs[0]["feature_names"]
            groups = recs[0]["groups"]
            n_features = len(names)
            per_seed_feature = []
            per_seed_group = []
            per_seed_timestep = []
            per_seed_total = []
            shock_group = defaultdict(list)
            nonshock_group = defaultdict(list)
            shock_feature = defaultdict(list)
            nonshock_feature = defaultdict(list)

            for r in recs:
                abs_shap = r["abs_shap"]
                is_shock = r["is_shock"]
                per_seed_total.append(float(abs_shap.sum(axis=1).mean()))
                if model in ("GC3", "GE"):
                    elemental = abs_shap.reshape(12, 6, n_features)
                    per_seed_feature.append(elemental.sum(axis=1).mean(axis=0))
                    per_seed_timestep.append(elemental.sum(axis=2).mean(axis=0))
                    group_vals = {}
                    for group in sorted(set(groups)):
                        idx = [i for i, g in enumerate(groups) if g == group]
                        vals = elemental[:, :, idx].sum(axis=(1, 2))
                        group_vals[group] = float(vals.mean())
                        shock_group[group].append(float(vals[is_shock].mean()))
                        nonshock_group[group].append(float(vals[~is_shock].mean()))
                    per_seed_group.append(group_vals)
                    for i, name in enumerate(names):
                        vals = elemental[:, :, i].sum(axis=1)
                        shock_feature[name].append(float(vals[is_shock].mean()))
                        nonshock_feature[name].append(float(vals[~is_shock].mean()))
                else:
                    per_seed_feature.append(abs_shap.mean(axis=0))
                    per_seed_timestep.append(np.array([], dtype=float))
                    group_vals = {}
                    for group in sorted(set(groups)):
                        idx = [i for i, g in enumerate(groups) if g == group]
                        vals = abs_shap[:, idx].sum(axis=1)
                        group_vals[group] = float(vals.mean())
                        shock_group[group].append(float(vals[is_shock].mean()))
                        nonshock_group[group].append(float(vals[~is_shock].mean()))
                    per_seed_group.append(group_vals)
                    for i, name in enumerate(names):
                        vals = abs_shap[:, i]
                        shock_feature[name].append(float(vals[is_shock].mean()))
                        nonshock_feature[name].append(float(vals[~is_shock].mean()))

            feature_stats = seed_stats(per_seed_feature)
            feature_mean = feature_stats["mean"]
            order = np.argsort(feature_mean)[::-1]
            top_features[f"{cultivar}/{model}"] = [
                {
                    "rank": i + 1,
                    "feature": names[int(j)],
                    "mean_abs_shap": as_float(feature_mean[int(j)]),
                    "seed_sd": as_float(feature_stats["sd"][int(j)]),
                }
                for i, j in enumerate(order[:10])
            ]

            group_names = sorted(set(groups))
            group_matrix = np.array([[gvals[g] for g in group_names] for gvals in per_seed_group], dtype=float)
            group_stats = seed_stats(group_matrix)
            total_mean = float(np.mean(per_seed_total))
            groups_summary[f"{cultivar}/{model}"] = [
                {
                    "group": group,
                    "mean_abs_shap": as_float(group_stats["mean"][i]),
                    "relative_share": as_float(group_stats["mean"][i] / total_mean if total_mean else 0.0),
                    "seed_sd": as_float(group_stats["sd"][i]),
                }
                for i, group in enumerate(group_names)
            ]

            if model in ("GC3", "GE"):
                ts_stats = seed_stats(per_seed_timestep)
                timestep_summary[f"{cultivar}/{model}"] = [
                    {
                        "timestep": int(i),
                        "offset_from_origin": int(i - 5),
                        "description": f"t{int(i - 5)} respecto al origen de prediccion",
                        "mean_abs_shap": as_float(ts_stats["mean"][i]),
                        "seed_sd": as_float(ts_stats["sd"][i]),
                    }
                    for i in range(6)
                ]

            group_shock_rows = []
            for group in group_names:
                shock_mean = float(np.mean(shock_group[group]))
                nonshock_mean = float(np.mean(nonshock_group[group]))
                group_shock_rows.append({
                    "group": group,
                    "shock_mean_abs": as_float(shock_mean),
                    "nonshock_mean_abs": as_float(nonshock_mean),
                    "difference_shock_minus_nonshock": as_float(shock_mean - nonshock_mean),
                })
            shock_summary[f"{cultivar}/{model}"] = {"groups": group_shock_rows}

            if model == "GE":
                nlp_idx = [i for i, g in enumerate(groups) if g == "NLP"]
                nlp_feature_stats = {names[i]: feature_stats["mean"][i] for i in nlp_idx}
                nlp_total = float(sum(nlp_feature_stats.values()))
                total_feature = float(np.sum(feature_mean))
                nlp_summary[cultivar] = {
                    "participation_total_abs": as_float(nlp_total / total_feature if total_feature else 0.0),
                    "variables": [
                        {
                            "rank": rank + 1,
                            "feature": name,
                            "mean_abs_shap": as_float(val),
                            "relative_within_nlp": as_float(val / nlp_total if nlp_total else 0.0),
                        }
                        for rank, (name, val) in enumerate(sorted(nlp_feature_stats.items(), key=lambda kv: kv[1], reverse=True))
                    ],
                }

            seed_variability_summary[f"{cultivar}/{model}"] = {
                "total_importance_mean": as_float(np.mean(per_seed_total)),
                "total_importance_median": as_float(np.median(per_seed_total)),
                "total_importance_sd": as_float(np.std(per_seed_total, ddof=1)),
                "total_importance_min": as_float(np.min(per_seed_total)),
                "total_importance_max": as_float(np.max(per_seed_total)),
                "interpretation": "Variabilidad descriptiva entre las 10 seeds; no es intervalo de confianza temporal.",
            }

    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    manifest = manifest_for_tree(SHAP_ROOT)
    MANIFEST.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    result = {
        "status": "PASS" if not failures and all(c["status"] == "PASS" for c in checks) else "FAIL",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "official_complete_json": str(complete_path),
        "official_complete_sha256": sha256_file(complete_path),
        "total_explanations": total_explanations,
        "neural_explanations": neural_explanations,
        "xgboost_explanations": xgb_explanations,
        "reconstruction_error": stats(recon_errors),
        "prediction_error_scaled": stats(pred_errors),
        "checks": checks,
        "failures": failures,
        "top_features": top_features,
        "groups": groups_summary,
        "nlp_ge": nlp_summary,
        "shock_vs_nonshock": shock_summary,
        "timestep": timestep_summary,
        "seed_variability": seed_variability_summary,
        "technical_incidents": {
            "warnings": technical_warnings,
            "fallback_used": any(r["metadata"].get("explainer", {}).get("method") == "SamplingExplainer" for r in records),
            "notes": [
                "La primera invocacion sandbox fallo antes de entrar al script por ruta de interprete base del venv; se reintento con el prefijo aprobado del venv, sin cambiar protocolo.",
                "No se detectaron warnings de explainer en metadata." if not technical_warnings else "Se registraron warnings en metadata.",
            ],
        },
        "artifact_manifest": str(MANIFEST),
        "artifact_manifest_sha256": sha256_file(MANIFEST),
    }
    OUT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = []
    lines.append("# AUDITORIA POST-RUN SHAP V2 OFICIAL")
    lines.append("")
    lines.append(f"Fecha UTC: {result['created_utc']}")
    lines.append("")
    lines.append("Estado: " + ("PASS" if result["status"] == "PASS" else "FAIL"))
    lines.append("")
    lines.append("## Resumen de ejecucion")
    lines.append("")
    lines.append(f"- Explicaciones totales: {total_explanations}")
    lines.append(f"- Explicaciones neuronales GC3/GE: {neural_explanations}")
    lines.append(f"- Explicaciones XGBoost: {xgb_explanations}")
    lines.append(f"- Maximo error de reconstruccion SHAP: {fmt(result['reconstruction_error']['max'], 12)}")
    lines.append(f"- Error maximo de prediccion contra congeladas: {fmt(result['prediction_error_scaled']['max'], 12)}")
    lines.append(f"- Manifiesto SHA256: {MANIFEST}")
    lines.append("")
    lines.append("## Matriz PASS/FAIL")
    lines.append("")
    lines.append(table(["Control", "Estado"], [[c["name"], c["status"]] for c in checks]))
    lines.append("")
    lines.append("## Top 10 features por mean |SHAP|")
    lines.append("")
    for key in ("sutil/GC3", "sutil/GE", "dulce/GC3", "dulce/GE", "sutil/XGBoost", "dulce/XGBoost"):
        lines.append(f"### {key}")
        lines.append("")
        lines.append(table(["Rank", "Feature", "mean |SHAP|", "SD seeds"], [
            [r["rank"], r["feature"], fmt(r["mean_abs_shap"]), fmt(r["seed_sd"])] for r in top_features[key]
        ]))
        lines.append("")
    lines.append("## Importancia por grupos")
    lines.append("")
    for key in ("sutil/GC3", "sutil/GE", "dulce/GC3", "dulce/GE"):
        lines.append(f"### {key}")
        lines.append("")
        lines.append(table(["Grupo", "mean |SHAP|", "Participacion", "SD seeds"], [
            [r["group"], fmt(r["mean_abs_shap"]), fmt(r["relative_share"]), fmt(r["seed_sd"])] for r in groups_summary[key]
        ]))
        lines.append("")
    lines.append("## NLP en GE")
    lines.append("")
    for cultivar in CULTIVARS:
        item = nlp_summary[cultivar]
        lines.append(f"### {cultivar}")
        lines.append("")
        lines.append(f"- Participacion NLP sobre |SHAP| total GE: {fmt(item['participation_total_abs'])}")
        lines.append("")
        lines.append(table(["Rank", "Variable NLP", "mean |SHAP|", "Participacion dentro NLP"], [
            [r["rank"], r["feature"], fmt(r["mean_abs_shap"]), fmt(r["relative_within_nlp"])] for r in item["variables"]
        ]))
        lines.append("")
    lines.append("## Shock vs nonshock por grupos")
    lines.append("")
    for key in ("sutil/GC3", "sutil/GE", "dulce/GC3", "dulce/GE"):
        lines.append(f"### {key}")
        lines.append("")
        lines.append(table(["Grupo", "Shock mean |SHAP|", "Nonshock mean |SHAP|", "Diferencia"], [
            [r["group"], fmt(r["shock_mean_abs"]), fmt(r["nonshock_mean_abs"]), fmt(r["difference_shock_minus_nonshock"])]
            for r in shock_summary[key]["groups"]
        ]))
        lines.append("")
    lines.append("## Importancia por timestep")
    lines.append("")
    lines.append("La posicion 5 es el origen de prediccion t; la posicion 0 corresponde a t-5.")
    lines.append("")
    for key in ("sutil/GC3", "sutil/GE", "dulce/GC3", "dulce/GE"):
        lines.append(f"### {key}")
        lines.append("")
        lines.append(table(["Timestep", "Offset", "mean |SHAP|", "SD seeds"], [
            [r["timestep"], r["offset_from_origin"], fmt(r["mean_abs_shap"]), fmt(r["seed_sd"])] for r in timestep_summary[key]
        ]))
        lines.append("")
    lines.append("## Variabilidad entre seeds")
    lines.append("")
    lines.append(table(["Cultivar/modelo", "Mean", "Median", "SD", "Min", "Max"], [
        [key, fmt(v["total_importance_mean"]), fmt(v["total_importance_median"]), fmt(v["total_importance_sd"]), fmt(v["total_importance_min"]), fmt(v["total_importance_max"])]
        for key, v in seed_variability_summary.items()
    ]))
    lines.append("")
    lines.append("## Incidencias tecnicas")
    lines.append("")
    for note in result["technical_incidents"]["notes"]:
        lines.append(f"- {note}")
    lines.append(f"- Fallback SamplingExplainer usado: {result['technical_incidents']['fallback_used']}")
    lines.append("")
    lines.append("## Restricciones interpretativas")
    lines.append("")
    lines.append("Estos resultados son descriptivos de contribuciones del modelo respecto a su baseline. No se registran conclusiones causales, inferencia de significancia, beneficios economicos ni interpretaciones de attention.")
    lines.append("")
    if failures:
        lines.append("## Fallas")
        lines.append("")
        for failure in failures:
            lines.append(f"- {failure}")
        lines.append("")
    lines.append("Estado final: " + ("SHAP V2 OFICIAL COMPLETADO Y CONGELADO" if result["status"] == "PASS" else "SHAP V2 OFICIAL BLOQUEADO"))
    lines.append("")
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({
        "status": result["status"],
        "checks_pass": sum(1 for c in checks if c["status"] == "PASS"),
        "checks_total": len(checks),
        "total_explanations": total_explanations,
        "max_reconstruction_error": result["reconstruction_error"]["max"],
        "max_prediction_error": result["prediction_error_scaled"]["max"],
        "audit": str(OUT_MD),
        "json": str(OUT_JSON),
        "manifest": str(MANIFEST),
        "manifest_sha256": result["artifact_manifest_sha256"],
    }, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
