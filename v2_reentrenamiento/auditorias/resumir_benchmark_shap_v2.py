"""Validate saved benchmark evidence and report it without loading any model/data CSV."""

import hashlib
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
AUDIT = ROOT / "v2_reentrenamiento/auditorias"
JSON_PATH = AUDIT / "shap_v2_explainer_benchmark.json"
MD_PATH = AUDIT / "AUDITORIA_BENCHMARK_EXPLAINER_SHAP_V2.md"


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    runs = data["runs"]
    assert data["status"] == "completed_technical_benchmark_pending_D52_D55"
    assert len(runs) == 156 and len(data["tree_runs"]) == 12
    assert data["test_loaded"] is False and not data["training_performed"]
    assert all(r["ok"] and r["reconstruction_max"] < 1e-5 for r in runs)
    assert all(c["max_abs_signed_shap_difference"] == 0 for c in data["comparisons"] if c["kind"] == "same_seed")
    assert all(file_hash(p) == h for p, h in data["frozen_hashes_before"].items())
    assert all(file_hash(p) == h for p, h in data["code_hashes"].items())
    for row in runs + data["tree_runs"]:
        path = ROOT / row["array_path"]
        assert file_hash(path) == row["array_sha256"]
        with np.load(path) as saved:
            if "flat_shap" in saved:
                assert list(saved["flat_shap"].shape) == row["shape_flat"]
                elemental = saved["elemental_shap"]
                assert list(elemental.shape) == row["shape_elemental"]
                residual = np.abs(saved["prediction"] - saved["base_values"] - elemental.sum((1, 2)))
                np.testing.assert_allclose(residual, row["reconstruction_abs_error"], atol=1e-12)
            elif row["repeat"] == "same_seed":
                other = next(r for r in data["tree_runs"] if r["cultivar"] == row["cultivar"]
                             and r["background_n"] == row["background_n"] and r["repeat"] == "reference")
                with np.load(ROOT / other["array_path"]) as reference:
                    np.testing.assert_array_equal(saved["shap_values"], reference["shap_values"])
    for info in data["models"]:
        for bg in info["backgrounds"]:
            assert file_hash(ROOT / bg["path"]) == bg["file_sha256"]
            assert all(date < "2024-01-01" for date in bg["target_dates"])
    for cultivar in ("sutil", "dulce"):
        for size in (8, 16, 32, 84):
            with np.load(AUDIT / f"shap_v2_benchmark_arrays/{cultivar}_GC3_background_{size}.npz") as gc3, \
                 np.load(AUDIT / f"shap_v2_benchmark_arrays/{cultivar}_GE_background_{size}.npz") as ge:
                for key in ("background", "validation"):
                    np.testing.assert_array_equal(gc3[key][:, :24], ge[key][:, :24])
                    np.testing.assert_array_equal(gc3[key][:, 24:].reshape(-1, 6, 33),
                                                  ge[key][:, 24:].reshape(-1, 6, 39)[:, :, :33])
    groups = defaultdict(list)
    for row in runs:
        groups[(row["cultivar"], row["model"], row["method"], row["background_n"], row["budget_level"])].append(row)
    summaries = []
    for (cultivar, model, method, n, level), rows in groups.items():
        reference = next(r for r in rows if r["repeat"] == "reference")
        same = next(c for c in data["comparisons"] if c["left"] == reference["id"] and c["kind"] == "same_seed")
        changed = next(c for c in data["comparisons"] if c["left"] == reference["id"] and c["kind"] == "different_seed")
        summaries.append({"cultivar": cultivar, "model": model, "method": method, "background_n": n,
            "budget_level": level, "params": reference["params"], "runs": len(rows),
            "seconds_per_sample_mean": float(np.mean([r["seconds_per_sample"] for r in rows])),
            "setup_seconds_mean": float(np.mean([r["setup_seconds"] for r in rows])),
            "total_seconds_sum": sum(r["total_seconds"] for r in rows),
            "memory_peak_mib": max(r["memory"]["rss_peak_mib"] for r in rows),
            "reconstruction_max": max(r["reconstruction_max"] for r in rows),
            "warnings_count": sum(sum(w["count"] for w in r["warnings"]) for r in rows),
            "same_seed_max_difference": same["max_abs_signed_shap_difference"],
            "different_seed_elemental_rho": changed["elemental_spearman"],
            "different_seed_feature_rho": changed["feature_spearman"],
            "different_seed_relative_l1": changed["importance_relative_l1"]})
    estimates = []
    for method in data["budgets"]:
        selected = [s for s in summaries if s["method"] == method and s["background_n"] == 32 and s["budget_level"] == "high"]
        components = []
        for s in selected:
            info = next(i for i in data["models"] if i["cultivar"] == s["cultivar"] and i["model"] == s["model"])
            components.append({"cultivar": s["cultivar"], "model": s["model"], "explanations": 120,
                "seconds": 120 * s["seconds_per_sample_mean"] + 10 * (s["setup_seconds_mean"] + info["load_and_warmup_seconds"])})
        estimates.append({"method": method, "background_n": 32, "budget_level": "high", "explanations": 480,
            "seconds_including_measured_load_warmup": sum(c["seconds"] for c in components), "components": components})
    tree = [r for r in data["tree_runs"] if r["background_n"] == 32]
    tree_compute = 120 * sum(np.mean([r["seconds_per_sample"] for r in tree if r["cultivar"] == c]) for c in ("sutil", "dulce"))
    tree_setup = 10 * sum(np.mean([r["setup_seconds"] for r in tree if r["cultivar"] == c]) for c in ("sutil", "dulce"))
    data["technical_summary"] = summaries
    data["cost_estimates"] = {"neural": estimates, "tree_explanations": 240,
        "tree_compute_seconds": tree_compute, "tree_setup_seconds": tree_setup,
        "limitations": "CPU estimate from two VAL samples/model and seed 0; extrapolation, not runtime guarantee. IO, all-seed loading, cold imports and reporting extra; no TEST execution."}
    data["verification"] = {"neural_runs": 156, "tree_runs": 12, "unique_val_samples_per_model": 2,
        "repeated_neural_sample_explanations": 312, "official_test_explanations": 0,
        "all_artifact_hashes_valid": True, "all_frozen_files_unchanged": True,
        "same_seed_arrays_identical": True, "shared_GC3_GE_inputs_identical": True,
        "shapes_and_reconstruction_verified_from_npz": True}
    data["recommendation"] = {
        "D52": {"status": "PENDIENTE DE APROBACION FINAL", "method": "PermutationExplainer",
            "wrapper": "branch A flatten followed by branch B flatten; exact unflatten; inference training=False",
            "masker": "Independent", "max_samples": 32, "link": "identity", "rng_seed": 1729,
            "max_evals_formula": "16 * (2 * flattened_feature_count + 1)",
            "max_evals_GC3": 7120, "max_evals_GE": 8272, "batch_size": 1024,
            "fallback": {"method": "SamplingExplainer", "nsamples": 65536, "min_samples_per_feature": 100,
                         "rng_seed": 1729, "background_n": 32, "link": "identity"},
            "fallback_trigger": "technical exception, nonfinite/invalid shapes, or reconstruction abs error > 1e-5 scaled; pause and document before changing official method; never inspect attribution narrative"},
        "D55": {"status": "PENDIENTE DE APROBACION FINAL", "source": "TRAIN only, per cultivar",
            "n": 32, "rule": data["background_rule"], "train_neural_n": 84, "train_xgboost_n": 89,
            "same_neural_indices_for_GC3_GE_and_all_seeds": True, "random_selection_seed": None,
            "indices_neural": data["models"][0]["backgrounds"][2]["indices"],
            "indices_xgboost": tree[0]["background_indices"]},
    }
    data["report_code_sha256"] = file_hash(__file__)
    JSON_PATH.write_text(json.dumps(data, indent=2, allow_nan=False), encoding="utf-8")
    lines = ["# AUDITORIA BENCHMARK EXPLAINER SHAP V2", "",
        "Estado: COMPLETADO. D52 Y D55 PENDIENTES DE APROBACION FINAL.", "",
        "## Alcance y controles", "",
        f"Ejecucion UTC: {data['timestamp_utc']} a {data['completed_utc']}; {data['elapsed_seconds']:.2f} s de benchmark.",
        "Cuatro checkpoints oficiales: GC3/GE, Sutil/Dulce, seed 0 como centinela tecnico. Inventario verificado de 40 checkpoints neuronales y 20 XGBoost. No seleccion por desempeno.",
        "Mismas dos muestras por metodo: objetivos VAL 2024-01 y 2024-07 (indices 0 y 6). Contextos respectivos: 2023-07..12 y 2024-01..06. Regla por calendario, sin examinar resultados.",
        "156 corridas neuronales = 52 configuraciones x 3 repeticiones = 312 explicaciones tecnicas de muestras VAL repetidas. TreeExplainer: 12 corridas, 24 explicaciones tecnicas VAL. Cero explicaciones TEST.",
        "Lectura acotada con csv.DictReader + islice(..., 102): se parsean solo 2016-07..2024-12, calendario exacto validado. El hashing binario del CSV completo solo verifica integridad; no materializa ni explica TEST.",
        "No entrenamiento/reentrenamiento, HPO, CV, cambio de features/scalers/pesos ni seleccion de seeds. Hashes antes/despues iguales y pesos en memoria invariantes. No commit ni push.", "",
        "## Correccion del antecedente", "",
        "El pre-flight anterior usaba pd.read_csv del CSV completo y filtraba despues. Por ello su test_loaded=False no demostraba ausencia de carga de TEST. Este benchmark no llama a ese lector; reutiliza solo su constructor puro de secuencias TRAIN/VAL. No se atribuyen explicaciones oficiales TEST al antecedente. El hallazgo queda registrado sin reescribir sus evidencias.", "",
        "## Configuracion comun", "",
        f"Entorno: Python {data['environment']['python']}; SHAP {data['environment']['shap']}; TensorFlow {data['environment']['tensorflow']}; Keras {data['environment']['keras']}; NumPy {data['environment']['numpy']}; XGBoost {data['environment']['xgboost']}.",
        f"Equipo: {data['environment']['processor']}; {data['environment']['logical_cpus']} CPUs logicas; RAM {data['environment']['ram_gib']:.2f} GiB. Inferencia CPU, TensorFlow intra/inter=1, oneDNN=0, ops deterministas=1.",
        "Wrapper comun: concat(flatten(A), flatten(B)); tf.function llama al mismo modelo con training=False y lotes de hasta 1024. Prediccion verificada contra llamada directa; sin recompilar/entrenar ni guardar modelos.",
        "Shapes SHAP: GC3 flat=(2,222), elemental=(2,6,37); GE flat=(2,258), elemental=(2,6,43); Tree=(2,37). Valores firmados, base y predicciones en unidades del target escalado. No se aplica inverse_transform al benchmark.",
        "Semillas del explainer: 1729 referencia, 1729 repeticion exacta, 1730 sensibilidad Monte Carlo. Son distintas de las seeds de entrenamiento. Se reinicializa el explainer en cada corrida y se mantiene el mismo orden de muestras.",
        "Kernel: nsamples=2048; ampliacion=4096 en N=32; l1_reg=0.0, sin seleccion de features. Sampling: nsamples=16384; ampliacion=65536 en N=32; min_samples_per_feature=100. Permutation: max_evals=4*(2*P+1); ampliacion=16*(2*P+1) en N=32; Independent(max_samples=N), batch_size=1024. P es 222/258. Presupuestos no equivalen al mismo numero de filas de inferencia; el JSON registra las filas realmente evaluadas.",
        "Sampling ademas probo TRAIN completo N=84 con nsamples=16384. No se repitieron Deep/Gradient: permanecen descartados por el StagingError registrado previamente.", "",
        "## Background", "",
        "84 ventanas neuronales TRAIN, objetivos 2017-01..2023-12; cada ventana conserva sus seis filas cronologicas. Indices cero-based = rint(linspace(0,83,N)). Seleccion por cultivar; GC3/GE comparten fechas y las 37 variables comunes, verificadas iguales. No primeros N, clustering ni busqueda de muestras.", "",
        "| N | Indices exactos (o rango completo) | Cobertura de meses objetivo |", "|---:|---|---|",
    ]
    for bg in data["models"][0]["backgrounds"]:
        indices = "0..83, todos" if bg["n"] == 84 else str(bg["indices"])
        months = len({d[5:7] for d in bg["target_dates"]})
        lines.append(f"| {bg['n']} | {indices} | {months}/12 |")
    lines += ["", "N=8 concentra objetivos en enero/diciembre y sus contextos no cubren todos los meses al mismo lag. N=16 y N=32 cubren los doce meses objetivo; no son muestras estacionalmente balanceadas. N=32 amplia la cobertura de ventanas, manteniendo coste viable. Cambiar background cambia la referencia matematica: no se exige identidad de atribuciones.", "",
        "## Resultados completos por configuracion", "",
        "Tiempo/muestra: media de tres corridas, dos muestras cada una. rho E/F: Spearman del ranking mean absolute SHAP elemental/por feature entre RNG 1729 y 1730. No son p-values ni comparaciones de importancia cientifica. Alto = presupuesto ampliado; base = presupuesto comun. Memoria es pico RSS del proceso, no memoria aislada del explainer.", "",
        "| Cultivar | Modelo | Metodo | N | Presupuesto | s/muestra | s total 3 corridas | RSS MiB | Error reconstruccion max | rho E | rho F | Warnings |",
        "|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"]
    for s in summaries:
        lines.append(f"| {s['cultivar']} | {s['model']} | {s['method'].replace('Explainer','')} | {s['background_n']} | {s['budget_level']} | {s['seconds_per_sample_mean']:.4f} | {s['total_seconds_sum']:.3f} | {s['memory_peak_mib']:.1f} | {s['reconstruction_max']:.3e} | {s['different_seed_elemental_rho']:.4f} | {s['different_seed_feature_rho']:.4f} | {s['warnings_count']} |")
    lines += ["", "Todas las 156 corridas tuvieron shapes correctos, salidas finitas y reconstruccion <1e-5 escalado. Mismo RNG/config: diferencia maxima firmada SHAP=0 en las 52 parejas; Tree tambien identico. No warnings de explainers ni excepciones. La reconstruccion es un control de implementacion, no prueba de convergencia del ranking.", "",
        "El primer Permutation incluye compilacion JIT de SHAP/Numba; sus repeticiones calientes se registran separadas en JSON. Warnings de carga Keras sobre build de BahdanauAttention y mensajes de TensorFlow sobre GPU nativa/placeholder se conservan como incidencias de entorno; no se modifico la capa. Memoria RSS muestreada cada 20 ms incluye TensorFlow y retencion del allocator; puede omitir picos mas breves.", "",
        "## Sensibilidad al background", "",
        "Comparaciones al mismo presupuesto base y RNG. Diferencia L1 = sum(abs(I_N1-I_N2))/sum(abs(I_N1)), sobre importancia elemental. Incluye sensibilidad Monte Carlo y cambio de referencia; no estima un error contra SHAP exacto.", "",
        "| Cultivar | Modelo | Metodo | N1 -> N2 | rho E | rho F | L1 relativa | Cambio base max |",
        "|---|---|---|---|---:|---:|---:|---:|"]
    rows_by_id = {r["id"]: r for r in runs}
    for c in data["comparisons"]:
        if c["kind"] == "background_sensitivity":
            left, right = rows_by_id[c["left"]], rows_by_id[c["right"]]
            if (left["background_n"], right["background_n"]) not in ((8, 16), (16, 32), (32, 84)):
                continue
            lines.append(f"| {c['cultivar']} | {c['model']} | {c['method'].replace('Explainer','')} | {left['background_n']} -> {right['background_n']} | {c['elemental_spearman']:.4f} | {c['feature_spearman']:.4f} | {c['importance_relative_l1']:.4f} | {c['max_base_difference']:.5f} |")
    lines += ["", "Las comparaciones de presupuesto base->alto a N=32, todas las parejas de backgrounds, top-10 overlap y diferencias firmadas completas estan en comparisons del JSON. Solo se comparan indices de rankings, sin decidir por los nombres de las variables.", "",
        "## D52 propuesta", "",
        "Recomendar UN metodo oficial: PermutationExplainer, Independent(background, max_samples=32), link identity, RNG=1729, max_evals=16*(2*P+1): GC3=7120; GE=8272; batch_size=1024. Wrapper probado, sin alterar pesos ni inputs del modelo. Mantener el presupuesto para todos los meses/cultivares/seeds, sin ajustes por narrativas SHAP.",
        "Motivo: compatible en los cuatro centinelas, reproducibilidad exacta, reconstruccion finita y mayor estabilidad elemental entre RNG que Kernel/Sampling con los presupuestos altos ensayados. D56 requiere conservar precisamente esa dimension elemental. Kernel es mas rapido pero menos estable en el ranking elemental; no se lo considera incompatible ni singular en este ensayo. Sampling es viable y competitivo, incluyendo TRAIN completo, pero el presupuesto alto consume mas tiempo que Permutation en esta prueba.",
        "Fallback propuesto: SamplingExplainer, mismo background N=32 y wrapper, nsamples=65536, min_samples_per_feature=100, link identity, RNG=1729. Activacion solo por excepcion tecnica, valores no finitos/shapes incorrectos o reconstruccion >1e-5 escalado. Detener y documentar antes de cambiar el metodo oficial; no mezclar silenciosamente explainers entre seeds ni usar preferencias interpretativas.", "",
        "## D55 propuesta", "",
        "Background oficial propuesto: N=32 TRAIN-only por cultivar, indices equiespaciados con la regla exacta anterior; mismas ventanas para GC3/GE y las diez seeds. XGBoost: N=32 filas TRAIN de su bundle oficial de 89 pares, regla rint(linspace(0,88,32)), reutilizada en todas sus seeds; no son ventanas LSTM. No VAL/TEST para background, sin semilla de muestreo.",
        "Justificacion: N=16->32 mantiene rankings por feature cercanos con Permutation y N=32 incorpora el doble de ventanas a coste operativo viable. N=8 tiene cobertura estacional insuficiente. No se afirma que N=32 sea optimo ni equivalente a TRAIN completo. Sampling N=84 confirma viabilidad tecnica de todo TRAIN, pero el fallback conserva N=32 para no cambiar la referencia al cambiar de estimador.", "",
        "## Coste futuro estimado", "",
        "480 explicaciones neuronales = 2 cultivares x 2 modelos x 10 seeds x 12 meses. Extrapolacion con presupuesto alto y N=32; incluye carga/calentamiento medidos por modelo y setup por seed. No incluye informes, escritura masiva, imports frios ni margen por otros procesos. No se ejecutaron esas explicaciones.", "",
        "| Metodo | Explicaciones | Segundos estimados | Minutos |", "|---|---:|---:|---:|"]
    for estimate in estimates:
        seconds = estimate["seconds_including_measured_load_warmup"]
        lines.append(f"| {estimate['method']} | 480 | {seconds:.2f} | {seconds / 60:.2f} |")
    lines += ["", f"XGBoost TreeExplainer N=32: 240 explicaciones, computo SHAP estimado {tree_compute:.3f} s + setup {tree_setup:.3f} s; carga de los 20 modelos/imports/IO adicionales no medidos en esa extrapolacion. No promesa de tiempo total subsegundo.",
        "Reservar aproximadamente 10-15 minutos para la opcion Permutation en este equipo con el wrapper medido; el tiempo real puede variar entre seeds, muestras y estado del sistema. Ninguna medicion procede de TEST.", "",
        "## Limites metodologicos", "",
        "Dos muestras VAL y una seed de entrenamiento por modelo solo permiten una recomendacion tecnica. No certifican convergencia para todos los meses/seeds. Correlacion alta puede coexistir con diferencias en magnitud; consultar L1 relativa y valores firmados guardados. La estabilidad de background se midio al presupuesto base, no al presupuesto alto para todos los N.",
        "Los tres metodos son model-agnostic y usan perturbaciones marginales. Pueden romper dependencias entre lags duplicados y secuencias, generando combinaciones fuera de la distribucion observada. Las atribuciones responden a ese juego de enmascaramiento, no a una distribucion condicional temporal ni a causalidad. Las bases dependen del cultivar/modelo/background.",
        "Kernel/Sampling incorporan restricciones/correcciones de suma; Permutation acumula diferencias telescopicas. Aditividad pequena por si sola no demuestra una estimacion precisa de cada Shapley value.",
        "Para el futuro: explicar las 10 seeds, congelar primero D52/D55, preservar sample x timestep x feature y signed SHAP. mean|SHAP| por feature = media sobre muestras de la suma de absolutos sobre tiempo; por timestep = media de suma sobre features; por grupo = suma de importancia de sus features. Esto difiere de abs(sum(SHAP)); no confundir cancelacion local con importancia global. NLP/attention/shocks siguen las decisiones ya cerradas, sin interpretacion oficial en este benchmark.", "",
        "## Fuentes y reproducibilidad", "",
        "- Codigo ejecutado: `benchmark_explainers_shap_v2.py`; reporte/verificacion: `resumir_benchmark_shap_v2.py`.",
        "- Fuente de ventanas: constructor puro TRAIN/VAL del pre-flight, contrastado con `src/training/sequences_gc3_ge.py`; nombres y orden reutilizados del builder oficial y cotejados con 40 configs. Ningun feature recalculado.",
        "- `shap_v2_explainer_benchmark.json`: versiones, git hash, hashes de datasets/scalers/checkpoints/configs/codigo instalado SHAP, indices/fechas/hashes de backgrounds, warnings, errores, tiempos, RSS, parametros, bases, predicciones, reconstruccion y comparaciones por corrida.",
        "- `shap_v2_benchmark_arrays/`: arrays tecnicos TRAIN/VAL NPZ con hashes; SHAP firmado flat/elemental, base, prediction, residual, backgrounds y muestras. No son resultados oficiales ni se escriben en resultados_v2_final/shap/.",
        f"- Git HEAD observado: `{data['git_hash']}`; integridad de {len(data['frozen_hashes_before'])} archivos congelados verificada.",
        f"- SHA256 codigo benchmark: `{data['code_hashes'][str(AUDIT / 'benchmark_explainers_shap_v2.py')]}`.",
        f"- SHA256 JSON final: `{file_hash(JSON_PATH)}`.",
        "- [KernelExplainer oficial](https://shap.readthedocs.io/en/stable/generated/shap.KernelExplainer.html): regresion ponderada; contrastada con _kernel.py local SHAP 0.51.0.",
        "- [SamplingExplainer oficial](https://shap.readthedocs.io/en/stable/generated/shap.SamplingExplainer.html): muestreo model-agnostic del background. Se fijan nsamples explicitamente porque explain() local usa auto=1000*M, distinto de la descripcion heredada de shap_values.",
        "- [PermutationExplainer oficial](https://shap.readthedocs.io/en/stable/generated/shap.PermutationExplainer.html): recorridos antiteticos y seed; se usa __call__(max_evals=...), no la interfaz legacy shap_values.", "",
        "## Decisiones para aprobacion", "",
        "| ID | Decision | Alternativas | Recomendacion | Justificacion | Estado |",
        "|---|---|---|---|---|---|",
        "| D52 | Explainer GC3/GE | Kernel / Sampling / Permutation | Permutation, 16*(2*P+1); fallback Sampling 65536 | Estabilidad elemental, fidelidad y coste medidos en cuatro centinelas | PENDIENTE DE APROBACION FINAL |",
        "| D55 | Background | 8 / 16 / 32 TRAIN; Sampling 84 | 32 TRAIN equiespaciados por cultivar | Cobertura, sensibilidad y coste; misma referencia en fallback | PENDIENTE DE APROBACION FINAL |", "",
        "D51, D53, D54 y D56-D61 estan CERRADAS por autorizacion expresa del usuario. Esta recomendacion no cierra D52/D55 ni autoriza SHAP oficial TEST.", ""]
    MD_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"verified": data["verification"], "cost_estimates": data["cost_estimates"]}, indent=2))


if __name__ == "__main__":
    main()
