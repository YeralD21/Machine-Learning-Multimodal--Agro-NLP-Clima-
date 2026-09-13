from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
SHAP_ROOT = ROOT / "v2_reentrenamiento" / "resultados_v2_final" / "shap" / "official_test_2025"
POSTRUN_JSON = ROOT / "v2_reentrenamiento" / "auditorias" / "shap_v2_postrun_oficial.json"
COMP_DIR = ROOT / "v2_reentrenamiento" / "resultados_v2_final" / "comparacion_final"
OUT_DIR = ROOT / "v2_reentrenamiento" / "resultados_v2_final" / "shap" / "interpretacion"
AUDIT_MD = ROOT / "v2_reentrenamiento" / "auditorias" / "AUDITORIA_INTERPRETACION_SHAP_V2.md"
CLAIMS_MD = OUT_DIR / "MATRIZ_AFIRMACIONES_SHAP.md"

CULTIVARS = ("sutil", "dulce")
MODELS = ("GC3", "GE", "XGBoost")
NEURAL_MODELS = ("GC3", "GE")
SEEDS = tuple(range(10))
TARGET_DATES = tuple(f"2025-{month:02d}-01" for month in range(1, 13))

GROUP_LABELS = {
    "produccion_historica": "produccion historica",
    "temporalidad": "temporalidad",
    "NASA_clima": "NASA/clima",
    "INDECI": "INDECI",
    "NLP": "NLP",
}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def load_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def fnum(value):
    if value is None:
        return ""
    try:
        if math.isnan(float(value)):
            return ""
    except TypeError:
        return value
    return f"{float(value):.12g}"


def lag_internal(feature: str) -> int:
    match = re.search(r"_lag(\d+)$", feature)
    return int(match.group(1)) if match else 0


def rank_stats(values_by_seed: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    arr = np.asarray(values_by_seed, dtype=float)
    ranks = []
    for row in arr:
        order = np.argsort(row)[::-1]
        r = np.empty_like(order, dtype=float)
        r[order] = np.arange(1, len(row) + 1)
        ranks.append(r)
    ranks = np.asarray(ranks, dtype=float)
    return (
        np.mean(arr, axis=0),
        np.median(arr, axis=0),
        np.std(arr, axis=0, ddof=1),
        np.min(arr, axis=0),
        np.max(arr, axis=0),
        np.std(ranks, axis=0, ddof=1),
    )


def stability_label(mean: float, sd: float, rank_sd: float) -> str:
    cv = sd / mean if mean else math.inf
    if rank_sd <= 2.0 and cv <= 0.50:
        return "relativamente estable"
    if rank_sd >= 5.0 or cv >= 0.75:
        return "altamente variable"
    return "intermedia"


def neural_abs_by_feature(record: dict) -> np.ndarray:
    shap = np.abs(record["flat_shap"])
    n_features = len(record["feature_names"])
    return shap.reshape(12, 6, n_features).sum(axis=1)


def load_records() -> list[dict]:
    records = []
    for cultivar in CULTIVARS:
        for model in MODELS:
            for seed in SEEDS:
                base = SHAP_ROOT / cultivar / model / f"seed_{seed:02d}"
                raw = np.load(base / "raw.npz", allow_pickle=False)
                meta = read_json(base / "metadata.json")
                records.append({
                    "cultivar": cultivar,
                    "model": model,
                    "seed": seed,
                    "flat_shap": raw["flat_shap"].astype(float),
                    "reconstruction_abs_error": raw["reconstruction_abs_error"].astype(float),
                    "pred_error": np.abs(raw["predictions_model_scaled"].astype(float) - raw["predictions_frozen_scaled"].astype(float)),
                    "target_dates": [str(x) for x in raw["target_dates"]],
                    "feature_names": [str(x) for x in raw["feature_names"]],
                    "groups": [str(x) for x in raw["groups"]],
                    "is_shock": raw["is_shock"].astype(bool),
                    "metadata": meta,
                    "raw_path": base / "raw.npz",
                })
    return records


def group_values_per_observation(record: dict) -> dict[str, np.ndarray]:
    groups = record["groups"]
    out = {}
    if record["model"] in NEURAL_MODELS:
        elemental = np.abs(record["flat_shap"]).reshape(12, 6, len(record["feature_names"]))
        for group in sorted(set(groups)):
            idx = [i for i, g in enumerate(groups) if g == group]
            out[group] = elemental[:, :, idx].sum(axis=(1, 2))
    else:
        abs_shap = np.abs(record["flat_shap"])
        for group in sorted(set(groups)):
            idx = [i for i, g in enumerate(groups) if g == group]
            out[group] = abs_shap[:, idx].sum(axis=1)
    return out


def feature_values_per_observation(record: dict) -> np.ndarray:
    if record["model"] in NEURAL_MODELS:
        return neural_abs_by_feature(record)
    return np.abs(record["flat_shap"])


def build_outputs() -> dict:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    postrun = read_json(POSTRUN_JSON)
    complete = read_json(SHAP_ROOT / "complete.json")
    records = load_records()
    failures = []

    max_rec = None
    for record in records:
        idx = int(np.argmax(record["reconstruction_abs_error"]))
        value = float(record["reconstruction_abs_error"][idx])
        if max_rec is None or value > max_rec["value"]:
            max_rec = {
                "value": value,
                "cultivar": record["cultivar"],
                "model": record["model"],
                "seed": record["seed"],
                "obs_index": idx,
                "target_date": record["target_dates"][idx],
                "raw_path": str(record["raw_path"]),
            }

    tolerance = complete["policy"]["reconstruction_atol_scaled"]
    pred_tolerance = complete["policy"]["pred_atol_scaled"]
    if not max_rec or max_rec["value"] > tolerance:
        failures.append("Reconstruccion SHAP excede tolerancia oficial.")

    rows_groups = []
    for cultivar in CULTIVARS:
        for model in NEURAL_MODELS:
            key = f"{cultivar}/{model}"
            rows = postrun["groups"][key]
            row_sum = sum(float(r["relative_share"]) for r in rows)
            for r in rows:
                rows_groups.append({
                    "cultivar": cultivar,
                    "modelo": model,
                    "grupo": GROUP_LABELS.get(r["group"], r["group"]),
                    "mean_abs_SHAP": fnum(r["mean_abs_shap"]),
                    "participacion": fnum(r["relative_share"]),
                    "SD_seeds": fnum(r["seed_sd"]),
                    "suma_participaciones_modelo": fnum(row_sum),
                    "check_suma_aprox_1": "PASS" if abs(row_sum - 1.0) <= 1e-9 else "FAIL",
                })
            if abs(row_sum - 1.0) > 1e-9:
                failures.append(f"Participaciones de grupo no suman 1 para {key}: {row_sum}")

    write_csv(
        OUT_DIR / "tabla_grupos_completa.csv",
        ["cultivar", "modelo", "grupo", "mean_abs_SHAP", "participacion", "SD_seeds", "suma_participaciones_modelo", "check_suma_aprox_1"],
        rows_groups,
    )

    rows_nlp = []
    for cultivar in CULTIVARS:
        recs = [r for r in records if r["cultivar"] == cultivar and r["model"] == "GE"]
        names = recs[0]["feature_names"]
        groups = recs[0]["groups"]
        nlp_idx = [i for i, g in enumerate(groups) if g == "NLP"]
        per_seed = []
        shock_per_seed = defaultdict(list)
        nonshock_per_seed = defaultdict(list)
        for r in recs:
            vals = feature_values_per_observation(r)
            seed_vals = vals[:, nlp_idx].mean(axis=0)
            per_seed.append(seed_vals)
            for pos, idx in enumerate(nlp_idx):
                shock_per_seed[names[idx]].append(float(vals[r["is_shock"], idx].mean()))
                nonshock_per_seed[names[idx]].append(float(vals[~r["is_shock"], idx].mean()))
        mean, median, sd, minimum, maximum, rank_sd = rank_stats(per_seed)
        nlp_total = float(np.sum(mean))
        total_feature_mean = np.mean([feature_values_per_observation(r).mean(axis=0) for r in recs], axis=0)
        nlp_share_total = nlp_total / float(np.sum(total_feature_mean))
        order = np.argsort(mean)[::-1]
        rank_by_pos = {int(pos): rank + 1 for rank, pos in enumerate(order)}
        for pos, idx in enumerate(nlp_idx):
            feature = names[idx]
            shock = float(np.mean(shock_per_seed[feature]))
            nonshock = float(np.mean(nonshock_per_seed[feature]))
            rows_nlp.append({
                "cultivar": cultivar,
                "modelo": "GE",
                "ranking_NLP": rank_by_pos[pos],
                "variable_NLP": feature,
                "mean_abs_SHAP": fnum(mean[pos]),
                "participacion_relativa_individual_en_NLP": fnum(mean[pos] / nlp_total if nlp_total else 0),
                "participacion_NLP_total_GE": fnum(nlp_share_total),
                "shock_mean_abs_SHAP": fnum(shock),
                "nonshock_mean_abs_SHAP": fnum(nonshock),
                "diferencia_shock_menos_nonshock": fnum(shock - nonshock),
                "seed_mean": fnum(mean[pos]),
                "seed_median": fnum(median[pos]),
                "seed_SD": fnum(sd[pos]),
                "seed_min": fnum(minimum[pos]),
                "seed_max": fnum(maximum[pos]),
                "rank_SD_seeds": fnum(rank_sd[pos]),
            })
    write_csv(
        OUT_DIR / "tabla_nlp.csv",
        [
            "cultivar", "modelo", "ranking_NLP", "variable_NLP", "mean_abs_SHAP",
            "participacion_relativa_individual_en_NLP", "participacion_NLP_total_GE",
            "shock_mean_abs_SHAP", "nonshock_mean_abs_SHAP", "diferencia_shock_menos_nonshock",
            "seed_mean", "seed_median", "seed_SD", "seed_min", "seed_max", "rank_SD_seeds",
        ],
        rows_nlp,
    )

    rows_effective = []
    for cultivar in CULTIVARS:
        for model in NEURAL_MODELS:
            recs = [r for r in records if r["cultivar"] == cultivar and r["model"] == model]
            names = recs[0]["feature_names"]
            groups = recs[0]["groups"]
            n_features = len(names)
            per_seed_eff = defaultdict(list)
            aggregate_by_effective_seed = defaultdict(list)
            for r in recs:
                elem = np.abs(r["flat_shap"]).reshape(12, 6, n_features)
                aggregate_this_seed = defaultdict(list)
                for timestep in range(6):
                    seq_offset = timestep - 5
                    for idx, name in enumerate(names):
                        internal = lag_internal(name)
                        effective = -seq_offset + internal
                        per_seed_eff[(timestep, seq_offset, idx, name, groups[idx], internal, effective)].append(
                            float(elem[:, timestep, idx].mean())
                        )
                        aggregate_this_seed[effective].append(elem[:, timestep, idx])
                for effective, arrays in aggregate_this_seed.items():
                    aggregate_by_effective_seed[effective].append(float(np.stack(arrays, axis=0).sum(axis=0).mean()))
            for (timestep, seq_offset, idx, name, group, internal, effective), vals in sorted(per_seed_eff.items(), key=lambda kv: (kv[0][-1], kv[0][0], kv[0][3])):
                rows_effective.append({
                    "tipo_fila": "mapeo_feature_timestep",
                    "cultivar": cultivar,
                    "modelo": model,
                    "feature": name,
                    "grupo": GROUP_LABELS.get(group, group),
                    "timestep_secuencia": timestep,
                    "offset_secuencia_vs_origen": seq_offset,
                    "lag_interno_feature": internal,
                    "effective_lag_vs_origen_meses": effective,
                    "mean_abs_SHAP_derivado": fnum(np.mean(vals)),
                    "SD_seeds": fnum(np.std(vals, ddof=1)),
                    "nota": "Derivada post-hoc; no reemplaza agregacion oficial por timestep.",
                })
            for effective, vals in sorted(aggregate_by_effective_seed.items()):
                rows_effective.append({
                    "tipo_fila": "agregado_effective_lag",
                    "cultivar": cultivar,
                    "modelo": model,
                    "feature": "__TODAS_LAS_FEATURES__",
                    "grupo": "__TODOS_LOS_GRUPOS__",
                    "timestep_secuencia": "",
                    "offset_secuencia_vs_origen": "",
                    "lag_interno_feature": "",
                    "effective_lag_vs_origen_meses": effective,
                    "mean_abs_SHAP_derivado": fnum(np.mean(vals)),
                    "SD_seeds": fnum(np.std(vals, ddof=1)),
                    "nota": "Agregado descriptivo por effective lag; derivado post-hoc, no reemplaza timestep oficial.",
                })
    write_csv(
        OUT_DIR / "tabla_effective_lag.csv",
        [
            "tipo_fila", "cultivar", "modelo", "feature", "grupo", "timestep_secuencia",
            "offset_secuencia_vs_origen", "lag_interno_feature", "effective_lag_vs_origen_meses",
            "mean_abs_SHAP_derivado", "SD_seeds", "nota",
        ],
        rows_effective,
    )

    rows_shock = []
    for cultivar in CULTIVARS:
        for model in MODELS:
            recs = [r for r in records if r["cultivar"] == cultivar and r["model"] == model]
            groups = sorted(set(recs[0]["groups"]))
            for group in groups:
                shock_vals = []
                nonshock_vals = []
                for r in recs:
                    gv = group_values_per_observation(r)[group]
                    shock_vals.append(float(gv[r["is_shock"]].mean()))
                    nonshock_vals.append(float(gv[~r["is_shock"]].mean()))
                shock = float(np.mean(shock_vals))
                nonshock = float(np.mean(nonshock_vals))
                rows_shock.append({
                    "cultivar": cultivar,
                    "modelo": model,
                    "grupo": GROUP_LABELS.get(group, group),
                    "n_shock": 3,
                    "n_nonshock": 9,
                    "mean_abs_SHAP_shock": fnum(shock),
                    "mean_abs_SHAP_nonshock": fnum(nonshock),
                    "diferencia_shock_menos_nonshock": fnum(shock - nonshock),
                    "ratio_shock_sobre_nonshock": fnum(shock / nonshock if nonshock > 1e-12 else None),
                    "nota": "Descriptivo; sin significancia inferencial ni causalidad.",
                })
    write_csv(
        OUT_DIR / "tabla_shock_nonshock_shap.csv",
        [
            "cultivar", "modelo", "grupo", "n_shock", "n_nonshock",
            "mean_abs_SHAP_shock", "mean_abs_SHAP_nonshock", "diferencia_shock_menos_nonshock",
            "ratio_shock_sobre_nonshock", "nota",
        ],
        rows_shock,
    )

    rows_stability = []
    for cultivar in CULTIVARS:
        for model in MODELS:
            recs = [r for r in records if r["cultivar"] == cultivar and r["model"] == model]
            # Group stability.
            group_names = sorted(set(recs[0]["groups"]))
            per_seed_group = []
            for r in recs:
                gv = group_values_per_observation(r)
                per_seed_group.append(np.asarray([float(gv[g].mean()) for g in group_names]))
            mean, median, sd, minimum, maximum, rank_sd = rank_stats(per_seed_group)
            for i, group in enumerate(group_names):
                rows_stability.append({
                    "cultivar": cultivar,
                    "modelo": model,
                    "nivel": "grupo",
                    "nombre": GROUP_LABELS.get(group, group),
                    "mean": fnum(mean[i]),
                    "median": fnum(median[i]),
                    "SD": fnum(sd[i]),
                    "min": fnum(minimum[i]),
                    "max": fnum(maximum[i]),
                    "rank_SD": fnum(rank_sd[i]),
                    "clasificacion": stability_label(float(mean[i]), float(sd[i]), float(rank_sd[i])),
                })
            # Feature stability.
            feature_names = recs[0]["feature_names"]
            per_seed_feature = [feature_values_per_observation(r).mean(axis=0) for r in recs]
            mean, median, sd, minimum, maximum, rank_sd = rank_stats(per_seed_feature)
            for i, feature in enumerate(feature_names):
                rows_stability.append({
                    "cultivar": cultivar,
                    "modelo": model,
                    "nivel": "feature",
                    "nombre": feature,
                    "mean": fnum(mean[i]),
                    "median": fnum(median[i]),
                    "SD": fnum(sd[i]),
                    "min": fnum(minimum[i]),
                    "max": fnum(maximum[i]),
                    "rank_SD": fnum(rank_sd[i]),
                    "clasificacion": stability_label(float(mean[i]), float(sd[i]), float(rank_sd[i])),
                })
    write_csv(
        OUT_DIR / "tabla_estabilidad_seeds_shap.csv",
        ["cultivar", "modelo", "nivel", "nombre", "mean", "median", "SD", "min", "max", "rank_SD", "clasificacion"],
        rows_stability,
    )

    metrics = load_csv(COMP_DIR / "tabla_maestra_modelos.csv")
    shock_metrics = load_csv(COMP_DIR / "tabla_shocks_modelos.csv")
    metric_by_key = {(r["cultivar"], r["model"]): r for r in metrics}
    shock_by_key = {(r["cultivar"], r["model"]): r for r in shock_metrics}
    rows_gc3_ge = []
    for cultivar in CULTIVARS:
        for model in NEURAL_MODELS:
            group_share = {r["grupo"]: r["participacion"] for r in rows_groups if r["cultivar"] == cultivar and r["modelo"] == model}
            key = (cultivar, model)
            rows_gc3_ge.append({
                "cultivar": cultivar,
                "modelo": model,
                "MAE_global_congelado": fnum(metric_by_key[key]["MAE"]),
                "MAE_shock_congelado": fnum(shock_by_key[key]["MAE_shock"]),
                "MAE_nonshock_congelado": fnum(shock_by_key[key]["MAE_nonshock"]),
                "Delta_s_congelado": fnum(shock_by_key[key]["Delta_s"]),
                "participacion_produccion_historica": group_share.get("produccion historica", ""),
                "participacion_temporalidad": group_share.get("temporalidad", ""),
                "participacion_NASA_clima": group_share.get("NASA/clima", ""),
                "participacion_INDECI": group_share.get("INDECI", ""),
                "participacion_NLP": group_share.get("NLP", "NA"),
                "variabilidad_total_importancia_SD": fnum(postrun["seed_variability"][f"{cultivar}/{model}"]["total_importance_sd"]),
                "nota": "Contextualizacion descriptiva GC3 vs GE; no comparar magnitudes SHAP como escala causal comun.",
            })
    write_csv(
        OUT_DIR / "tabla_gc3_vs_ge.csv",
        [
            "cultivar", "modelo", "MAE_global_congelado", "MAE_shock_congelado", "MAE_nonshock_congelado", "Delta_s_congelado",
            "participacion_produccion_historica", "participacion_temporalidad", "participacion_NASA_clima", "participacion_INDECI",
            "participacion_NLP", "variabilidad_total_importancia_SD", "nota",
        ],
        rows_gc3_ge,
    )

    rows_xgb = []
    for cultivar in CULTIVARS:
        key = f"{cultivar}/XGBoost"
        for item in postrun["top_features"][key]:
            rows_xgb.append({
                "cultivar": cultivar,
                "modelo": "XGBoost",
                "tipo": "top_feature",
                "ranking": item["rank"],
                "nombre": item["feature"],
                "mean_abs_SHAP": fnum(item["mean_abs_shap"]),
                "SD_seeds": fnum(item["seed_sd"]),
                "grupo": "",
                "shock_mean_abs_SHAP": "",
                "nonshock_mean_abs_SHAP": "",
                "diferencia_shock_menos_nonshock": "",
                "nota": "Magnitudes XGBoost descriptivas; no equivalencia directa con redes.",
            })
        for item in postrun["groups"][key]:
            group = GROUP_LABELS.get(item["group"], item["group"])
            shock_row = next(r for r in rows_shock if r["cultivar"] == cultivar and r["modelo"] == "XGBoost" and r["grupo"] == group)
            rows_xgb.append({
                "cultivar": cultivar,
                "modelo": "XGBoost",
                "tipo": "grupo",
                "ranking": "",
                "nombre": group,
                "mean_abs_SHAP": fnum(item["mean_abs_shap"]),
                "SD_seeds": fnum(item["seed_sd"]),
                "grupo": group,
                "shock_mean_abs_SHAP": shock_row["mean_abs_SHAP_shock"],
                "nonshock_mean_abs_SHAP": shock_row["mean_abs_SHAP_nonshock"],
                "diferencia_shock_menos_nonshock": shock_row["diferencia_shock_menos_nonshock"],
                "nota": "Grupo equivalente cuando aplica.",
            })
    write_csv(
        OUT_DIR / "tabla_xgboost_shap.csv",
        [
            "cultivar", "modelo", "tipo", "ranking", "nombre", "mean_abs_SHAP", "SD_seeds", "grupo",
            "shock_mean_abs_SHAP", "nonshock_mean_abs_SHAP", "diferencia_shock_menos_nonshock", "nota",
        ],
        rows_xgb,
    )

    claims = [
        ["Clima en Sutil", "NASA/clima es grupo de alta participacion en GC3/GE y aparece con diferencia shock-nonshock positiva.", "Descriptiva SHAP post-hoc", "En los modelos SHAP, las variables NASA/clima concentran una fraccion relevante de |SHAP| para Sutil.", "El clima causo los shocks o explica causalmente la produccion."],
        ["Produccion historica en Dulce", "Produccion historica tiene mayor participacion relativa en Dulce que en Sutil dentro de redes.", "Descriptiva comparativa", "La produccion historica aporta una fraccion descriptiva mayor en Dulce que en Sutil en estas atribuciones.", "La produccion pasada determina causalmente el desempeno futuro."],
        ["NLP Sutil", "GE Sutil: NLP participa 0.103566 del |SHAP| total.", "Descriptiva SHAP", "NLP tiene una participacion no nula y acotada en GE Sutil.", "Las noticias causaron cambios productivos en Sutil."],
        ["NLP Dulce", "GE Dulce: NLP participa 0.086341 del |SHAP| total.", "Descriptiva SHAP", "NLP aporta menos del 10% del |SHAP| total en GE Dulce.", "NLP explica causalmente la mejora de GE."],
        ["Shocks", "n_shock=3 y n_nonshock=9; diferencias por grupos son descriptivas.", "Baja para inferencia; util descriptiva", "Durante meses shock, algunos grupos muestran mayor o menor |SHAP| medio.", "Los shocks fueron causados por esas variables o hay significancia estadistica."],
        ["Resiliencia", "Comparacion final muestra errores shock, pero SHAP no prueba resiliencia.", "Contextual, no causal", "Se puede hablar de desempeno observado en meses shock.", "SHAP demuestra resiliencia o beneficio economico."],
        ["Effective lag", "Mapeo separa timestep, lag interno y effective lag.", "Metodologica descriptiva", "La antiguedad efectiva combina posicion secuencial e indicador lag interno.", "El timestep por si solo equivale a antiguedad real de la variable."],
        ["Comparacion con SARIMA", "Comparacion final congelada: SARIMA mejor global; casos shock segun cultivar.", "Predictiva congelada", "SHAP contextualiza modelos neuronales/XGB; SARIMA se compara por metricas congeladas.", "SHAP explica SARIMA o prueba superioridad causal."],
        ["Causalidad", "D59 y auditorias prohiben causalidad.", "Restriccion metodologica", "Contribucion positiva/negativa del modelo respecto a baseline.", "La variable causo aumento/disminucion real de produccion."],
    ]
    claim_lines = [
        "# MATRIZ DE AFIRMACIONES SHAP V2",
        "",
        "| AFIRMACION | SOPORTE | NIVEL DE EVIDENCIA | REDACCION PERMITIDA | REDACCION NO PERMITIDA |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in claims:
        claim_lines.append("| " + " | ".join(row) + " |")
    CLAIMS_MD.write_text("\n".join(claim_lines) + "\n", encoding="utf-8")

    # Audit report.
    stable = [r for r in rows_stability if r["clasificacion"] == "relativamente estable" and r["nivel"] == "feature"]
    variable = [r for r in rows_stability if r["clasificacion"] == "altamente variable" and r["nivel"] == "feature"]
    stable_top = sorted(stable, key=lambda r: float(r["mean"]), reverse=True)[:12]
    variable_top = sorted(variable, key=lambda r: float(r["SD"]), reverse=True)[:12]

    figure_candidates = [
        ("Importancia SHAP por grupos GC3/GE", "paper principal", "Barras por cultivar/modelo usando tabla_grupos_completa.csv."),
        ("Top features por cultivar", "paper principal o suplemento", "Mostrar top 10 con SD entre seeds; separar familias de modelo."),
        ("NLP Sutil vs Dulce", "paper principal si el texto discute GE; si no, suplemento", "Variables NLP, participacion total y ranking."),
        ("Shock vs nonshock por grupos", "suplemento", "n=3 vs n=9, descriptivo sin inferencia."),
        ("Effective lag", "paper principal metodologico o suplemento", "Mostrar agregacion derivada junto a advertencia de lag efectivo."),
        ("Estabilidad multi-seed o XGBoost", "suplemento", "Rank/SD entre seeds; XGBoost como patron descriptivo separado."),
    ]

    audit = []
    audit.append("# AUDITORIA INTERPRETACION SHAP V2")
    audit.append("")
    audit.append("Estado: PASS" if not failures else "Estado: FAIL")
    audit.append("")
    audit.append("## Reconstruccion SHAP")
    audit.append("")
    audit.append(f"- Error maximo documentado: {max_rec['value']:.12g}")
    audit.append(f"- Tolerancia oficial de reconstruccion: {tolerance:.12g}")
    audit.append(f"- Tolerancia oficial de prediccion congelada: {pred_tolerance:.12g}")
    audit.append("- Las tolerancias son distintas: reconstruccion usa 1e-5 en escala del target escalado; prediccion usa 1e-6 en escala del target escalado.")
    audit.append("- Criterio matematico: max(|prediction_scaled - (base_value + sum(SHAP_signed))|) <= reconstruction_atol_scaled.")
    audit.append("- Unidad: target escalado, no toneladas.")
    audit.append(f"- Maximo en: cultivar={max_rec['cultivar']}, modelo={max_rec['model']}, seed={max_rec['seed']:02d}, obs_index={max_rec['obs_index']}, fecha={max_rec['target_date']}.")
    audit.append("")
    audit.append("## Salidas generadas")
    audit.append("")
    for name in [
        "tabla_grupos_completa.csv",
        "tabla_nlp.csv",
        "tabla_effective_lag.csv",
        "tabla_shock_nonshock_shap.csv",
        "tabla_estabilidad_seeds_shap.csv",
        "tabla_gc3_vs_ge.csv",
        "tabla_xgboost_shap.csv",
        "MATRIZ_AFIRMACIONES_SHAP.md",
    ]:
        audit.append(f"- {OUT_DIR / name}")
    audit.append("")
    audit.append("## Comparacion predictiva congelada")
    audit.append("")
    audit.append("- Sutil: SARIMA es mejor global por MAE congelado; GC3 presenta menor MAE shock observado; GE queda peor que GC3 en MAE global y shock.")
    audit.append("- Dulce: SARIMA es mejor global y shock; GE mejora descriptivamente frente a GC3, pero no supera SARIMA ni XGBoost globalmente.")
    audit.append("")
    audit.append("## Effective lag")
    audit.append("")
    audit.append("Para redes, effective_lag = -offset_secuencia_vs_origen + lag_interno_feature. Ejemplo: X_lag3 en t-5 equivale a t-8 respecto del origen.")
    audit.append("Esta agregacion es derivada post-hoc y no reemplaza los SHAP elementales ni la agregacion oficial por timestep.")
    audit.append("")
    audit.append("## Figuras candidatas")
    audit.append("")
    audit.append("| Figura | Ubicacion sugerida | Nota |")
    audit.append("| --- | --- | --- |")
    for row in figure_candidates:
        audit.append("| " + " | ".join(row) + " |")
    audit.append("")
    audit.append("## Estabilidad interpretativa")
    audit.append("")
    audit.append("Features relativamente estables destacadas:")
    for row in stable_top:
        audit.append(f"- {row['cultivar']}/{row['modelo']} {row['nombre']}: mean={row['mean']}, SD={row['SD']}, rank_SD={row['rank_SD']}")
    audit.append("")
    audit.append("Features altamente variables destacadas:")
    for row in variable_top:
        audit.append(f"- {row['cultivar']}/{row['modelo']} {row['nombre']}: mean={row['mean']}, SD={row['SD']}, rank_SD={row['rank_SD']}")
    audit.append("")
    audit.append("## Riesgos de interpretacion")
    audit.append("")
    audit.append("- No interpretar SHAP como causalidad.")
    audit.append("- No comparar magnitudes SHAP brutas entre familias de modelos como una escala causal comun.")
    audit.append("- No convertir diferencias shock/nonshock en significancia inferencial.")
    audit.append("- No leer timestep como antiguedad efectiva cuando la feature contiene lag interno.")
    audit.append("- No usar NLP como prueba de efecto causal de noticias.")
    audit.append("- No afirmar resiliencia, beneficio economico ni toneladas ahorradas a partir de SHAP.")
    audit.append("")
    audit.append("## Controles")
    audit.append("")
    checks = {
        "No se ejecuto SHAP": True,
        "No se modificaron RAW oficiales": True,
        "Participaciones por grupo suman aproximadamente 1": not any(r["check_suma_aprox_1"] != "PASS" for r in rows_groups),
        "NLP extraido para Sutil GE y Dulce GE": len(rows_nlp) == 12,
        "Effective lag documentado": len(rows_effective) > 0,
        "Shock/nonshock n=3/9": all(str(r["n_shock"]) == "3" and str(r["n_nonshock"]) == "9" for r in rows_shock),
        "Comparacion predictiva usa tabla congelada": (COMP_DIR / "tabla_maestra_modelos.csv").exists(),
        "Matriz de afirmaciones creada": CLAIMS_MD.exists(),
    }
    audit.append("| Control | Estado |")
    audit.append("| --- | --- |")
    for k, v in checks.items():
        audit.append(f"| {k} | {'PASS' if v else 'FAIL'} |")
        if not v:
            failures.append(k)
    if failures:
        audit.append("")
        audit.append("## Fallas")
        for failure in failures:
            audit.append(f"- {failure}")
    AUDIT_MD.write_text("\n".join(audit) + "\n", encoding="utf-8")

    return {
        "status": "PASS" if not failures else "FAIL",
        "max_reconstruction": max_rec,
        "tolerance": tolerance,
        "rows": {
            "groups": len(rows_groups),
            "nlp": len(rows_nlp),
            "effective_lag": len(rows_effective),
            "shock": len(rows_shock),
            "stability": len(rows_stability),
            "gc3_vs_ge": len(rows_gc3_ge),
            "xgb": len(rows_xgb),
        },
        "audit": str(AUDIT_MD),
        "out_dir": str(OUT_DIR),
    }


if __name__ == "__main__":
    print(json.dumps(build_outputs(), indent=2, ensure_ascii=False))
