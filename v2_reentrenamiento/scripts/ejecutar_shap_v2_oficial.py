"""Frozen D51-D61 runner. Default: preflight, NEVER TEST explanations.

All writes stay in resultados_v2_final/shap. Official execution requires the
explicit --execute-test flag plus an approved preflight and its SHA256.
No fit, scaling, metrics, feature engineering, or seed selection is performed.
"""
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import io
import json
import os
import platform
import random
import subprocess
import sys
import tempfile
import time
import traceback
import warnings
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

sys.dont_write_bytecode = True
# PYTHONHASHSEED must take effect at interpreter startup, not after imports.
if __name__ == '__main__' and os.environ.get('PYTHONHASHSEED') != '1729':
    child_env = dict(os.environ, PYTHONHASHSEED='1729', PYTHONDONTWRITEBYTECODE='1')
    raise SystemExit(subprocess.run([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]], env=child_env).returncode)
ROOT = Path(__file__).resolve().parents[2]
V2 = ROOT / 'v2_reentrenamiento'
RESULTS = V2 / 'resultados_v2_final'
OUTPUT = RESULTS / 'shap'
AUDIT = V2 / 'auditorias'
sys.path.insert(0, str(ROOT))
os.environ.update(TF_ENABLE_ONEDNN_OPTS='0', TF_DETERMINISTIC_OPS='1',
                  TF_CPP_MIN_LOG_LEVEL='2', CUDA_VISIBLE_DEVICES='-1')

import joblib
import numpy as np
import pandas as pd
import shap
import tensorflow as tf
import xgboost
from tensorflow import keras
from v2_reentrenamiento.src.models.dual_lstm_attention import BahdanauAttention
from v2_reentrenamiento.src.models.features_gc3_ge import (
    build_feature_spec, NASA_BASE, INDECI_BASE, NLP_LAGGED, TEMPORAL_FEATURES, LAGS,
)
from v2_reentrenamiento.src.models.features_gc2_xgboost import build_gc2_feature_spec
from v2_reentrenamiento.src.training.sequences_gc3_ge import build_sequences, load_feature_dataframe
from v2_reentrenamiento.src.training.tabular_gc2_xgboost import build_gc2_tabular_bundle
from v2_reentrenamiento.src.evaluation.shocks_d35 import OFFICIAL_TEST_SHOCKS_2025

CULTIVARS = ('sutil', 'dulce')
MODELS = ('GC3', 'GE', 'XGBoost')
SEEDS = tuple(range(10))
RNG = 1729
MAX_EVALS = {'GC3': 7120, 'GE': 8272}
TEST_DATES = [f'2025-{m:02d}-01' for m in range(1, 13)]
# Fixed BEFORE inference: CSV float32 round-trip and CPU float arithmetic.
# No relative tolerance, no tolerance tuning based on observed errors.
PRED_ATOL = 1e-6
RECON_ATOL = 1e-5
BENCHMARK_SHA = 'c29b4ffa1fa7b03daa15db52a770214ba92587fd4d152cbaa3cd82cea7f18641'
BG_HASHES = {
    'sutil/GC3': '4d95df53dfeaa745e0752465ead086efced9947d7af4d83ee64d6fcc14d8c5a3',
    'sutil/GE': 'a8a25c6720e2e3d1a8c9dd5ca111cade33260403d4ea92637d3072a8ade36818',
    'dulce/GC3': 'd3c84a671da84253c3e5fb0a8edb7af3d7ac3f8c8dc4bd96c3fcd216d20bd32c',
    'dulce/GE': '01895908a544ead3ab1dcb359b3de99cb0e057d64e09d52c87800248846de69e',
    'sutil/XGBoost': '71d3dbb62dc1fa60e8362b73296b6be423f13fa03f78e57af898c0114ead9980',
    'dulce/XGBoost': '41021ad468e00e8b23dcfc710e6e2922c2539349b84ab616bdbec3546b774be2',
}
POLICY = dict(models=MODELS, seeds=SEEDS, rng=RNG, max_evals=MAX_EVALS,
              background_n=32, batch_size=1024, link='identity',
              neural='PermutationExplainer', tree='TreeExplainer',
              tree_perturbation='interventional', tree_output='raw',
              fallback='SamplingExplainer', nsamples=65536, min_samples_per_feature=100,
              pred_atol_scaled=PRED_ATOL, pred_rtol=0, reconstruction_atol_scaled=RECON_ATOL)


class GuardError(RuntimeError):
    pass


class TechnicalFailure(RuntimeError):
    """Only explanation calculation failures qualify; never integrity failures."""


def require(ok, message):
    if not ok:
        raise GuardError(message)


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def array_hash(a):
    a = np.ascontiguousarray(a)
    return hashlib.sha256(json.dumps({'shape': a.shape, 'dtype': a.dtype.str}).encode()
                          + a.tobytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write_json(path, obj):
    # Exclusive creation: no overwrite, including partial results.
    with Path(path).open('x', encoding='utf-8') as f:
        json.dump(obj, f, indent=2, ensure_ascii=True, allow_nan=False)


def save_npz(path, arrays):
    require(all(np.asarray(a).dtype.kind != 'O' for a in arrays.values()), 'Object NPZ forbidden')
    with Path(path).open('xb') as f:
        np.savez_compressed(f, **arrays)
    with np.load(path, allow_pickle=False) as restored:
        require(set(restored.files) == set(arrays), 'NPZ keys mismatch')
        for name, a in arrays.items():
            require(np.array_equal(restored[name], a), f'NPZ roundtrip: {name}')


def check_write_path(path):
    if isinstance(path, int):
        return
    if os.fsdecode(path).lower() == os.devnull.lower():
        return  # OS null device is not a filesystem artifact.
    resolved = Path(os.fsdecode(path)).resolve()
    require(resolved.is_relative_to(OUTPUT.resolve()), f'Write outside SHAP directory: {resolved}')


def readonly_hook(event, args):
    if event == 'open':
        path, mode, flags = args
        if (mode and any(c in mode for c in 'wax+')) or (flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC)):
            check_write_path(path)
    elif event in ('os.mkdir', 'os.remove', 'os.rmdir', 'os.chmod', 'os.utime'):
        check_write_path(args[0])
    elif event in ('os.rename', 'os.link', 'os.symlink'):
        check_write_path(args[0])
        check_write_path(args[1])


def environment():
    return dict(python=platform.python_version(), numpy=np.__version__, shap=shap.__version__,
                tensorflow=tf.__version__, keras=keras.__version__, xgboost=xgboost.__version__,
                joblib=joblib.__version__, pandas=pd.__version__, os=platform.platform(),
                cpu=platform.processor(), devices=[str(d) for d in tf.config.get_visible_devices()],
                TF_ENABLE_ONEDNN_OPTS=os.environ['TF_ENABLE_ONEDNN_OPTS'],
                TF_DETERMINISTIC_OPS=os.environ['TF_DETERMINISTIC_OPS'],
                intra_threads=tf.config.threading.get_intra_op_parallelism_threads(),
                inter_threads=tf.config.threading.get_inter_op_parallelism_threads(),
                pythonhashseed=os.environ.get('PYTHONHASHSEED'))


def git_info():
    def call(*args):
        p = subprocess.run(['git', *args], cwd=ROOT, capture_output=True, text=True)
        return p.stdout.strip() + ('\n' + p.stderr.strip() if p.stderr.strip() else '')
    return dict(head=call('rev-parse', 'HEAD'), status_sb=call('status', '-sb'))


def inventory():
    items = []
    for c in CULTIVARS:
        for m in MODELS:
            parent = RESULTS / ('official_gc2_xgboost' if m == 'XGBoost' else 'official_gc3_ge') / c / ('GC2_XGBoost' if m == 'XGBoost' else m)
            require(sorted(p.name for p in parent.glob('seed_*')) == [f'seed_{s:02d}' for s in SEEDS],
                    f'Missing/extra seeds: {c}/{m}')
            for s in SEEDS:
                d = parent / f'seed_{s:02d}'
                p = d / ('model.joblib' if m == 'XGBoost' else 'checkpoint_best.keras')
                require(p.is_file(), f'Missing official model: {p}')
                items.append((c, m, s, d, p))
    require(len(items) == 60, 'Expected exactly 60 official models')
    return items


def snapshot():
    # Include all frozen results, experiments, datasets, model/training/evaluation
    # code and historical audits. Exclude this task's output directory only.
    paths = set()
    for directory in (RESULTS, V2 / 'experimentos', V2 / 'data/processed', V2 / 'src'):
        paths.update(p for p in directory.rglob('*') if p.is_file()
                     and not p.is_relative_to(OUTPUT) and '__pycache__' not in p.parts)
    paths.update(p for p in AUDIT.rglob('*') if p.is_file() and '__pycache__' not in p.parts
                 and 'ejecucion_shap_oficial' not in p.name.lower()
                 and 'preflight_oficial' not in p.name.lower()
                 and p.name != 'shap_v2_preflight_ejecucion_oficial.json')
    paths.update([V2 / 'HANDOFF_SESION.md', V2 / 'DECISIONES_METODOLOGICAS.md', Path(__file__)])
    return {str(p.resolve()): sha256(p) for p in sorted(paths)}


def verify_hashes(expected):
    require(bool(expected), 'Empty critical hash manifest')
    for path, digest in expected.items():
        require(Path(path).is_file() and sha256(path) == digest, f'Critical SHA256 changed: {path}')


def historical_checks(bench, report):
    require(sha256(AUDIT / 'shap_v2_explainer_benchmark.json') == BENCHMARK_SHA, 'Historical benchmark changed')
    require(bench['frozen_hashes_before'] == bench['frozen_hashes_after'], 'Historical freeze inconsistent')
    verify_hashes(bench['frozen_hashes_before'])
    verify_hashes(bench['code_hashes'])
    comp = read_json(RESULTS / 'comparacion_final/manifest_comparacion_final_modelos.json')
    require(comp['all_controls_ok'] and comp['status'] == 'approved', 'Comparison not frozen')
    verify_hashes({comp['sources'][k]: h for k, h in comp['hashes'].items()})
    require(comp['models'] == ['Naive', 'SARIMA_rolling', 'XGBoost', 'GC3', 'GE'], 'Frozen model families')
    sar = read_json(comp['sources']['sarima_manifest'])
    require(sar['all_controls_ok'] and sar['models_evaluated'] == 2 and sar['predictions_total'] == 24,
            'SARIMA rolling not frozen')
    neu = read_json(comp['sources']['gc3_ge_manifest'])
    require(neu['checkpoints_evaluated'] == 40 and all(neu['sanity_checks'].values()), 'Neural freeze')
    xgb = read_json(comp['sources']['xgb_manifest'])
    require(xgb['status'] == 'approved' and xgb['models_evaluated'] == 20, 'XGBoost freeze')
    report['frozen_families'] = comp['models']
    report['historical_hashes_verified'] = len(bench['frozen_hashes_before']) + len(bench['code_hashes']) + len(comp['hashes'])
    return comp, neu, xgb


def bounded_frame(c):
    path = V2 / f'data/processed/master_dataset_{c}_v2_escalado.csv'
    with path.open(encoding='utf-8-sig', newline='') as f:
        rows = list(islice(csv.DictReader(f), 102))  # NEVER requests record 103.
    frame = pd.DataFrame(rows).apply(pd.to_numeric, errors='raise')
    frame['fecha'] = pd.to_datetime(dict(year=frame['a\u00f1o'], month=frame['mes'], day=1))
    require(frame.fecha.tolist() == pd.date_range('2016-07-01', '2024-12-01', freq='MS').tolist(), 'Bounded TRAIN/VAL calendar')
    return frame


def flatten(a, b):
    return np.hstack([a.reshape(len(a), -1), b.reshape(len(b), -1)]).astype(np.float32)


def elemental(values, m):
    if m == 'XGBoost':
        return values
    return np.concatenate([values[:, :24].reshape(-1, 6, 4),
                           values[:, 24:].reshape(-1, 6, 33 if m == 'GC3' else 39)], axis=2)


def feature_map(c, m):
    spec = build_feature_spec(c, 'GC3' if m == 'XGBoost' else m)
    names = list(spec.all_inputs)
    require(len(names) == (43 if m == 'GE' else 37), 'Conceptual feature count')
    if m == 'XGBoost':
        require(tuple(names) == build_gc2_feature_spec(c).predictors, 'XGBoost exact feature order')
    groups = []
    for name in names:
        if name in spec.rama_a:
            groups.append('produccion_historica')
        elif name in TEMPORAL_FEATURES:
            groups.append('temporalidad')
        elif name in [f'{b}_lag{l}' for b in NASA_BASE for l in LAGS]:
            groups.append('NASA_clima')
        elif name in [f'{b}_lag{l}' for b in INDECI_BASE for l in LAGS]:
            groups.append('INDECI')
        elif m == 'GE' and name in NLP_LAGGED:
            groups.append('NLP')
        else:
            raise GuardError(f'Unmapped feature: {name}')
    mapping = []
    branches = [('tabular', names)] if m == 'XGBoost' else [('A', list(spec.rama_a)), ('B', list(spec.rama_b))]
    for branch, features in branches:
        for step in ([0] if m == 'XGBoost' else range(6)):
            for name in features:
                j = names.index(name)
                mapping.append(dict(flat_index=len(mapping), branch=branch, timestep=step,
                                    offset_from_origin=0 if m == 'XGBoost' else step - 5,
                                    feature_index=j, feature=name, group=groups[j]))
    require(len(mapping) == (37 if m == 'XGBoost' else 222 if m == 'GC3' else 258), 'Flatten feature mapping')
    return names, groups, mapping


def load_background(c, m, frame, bench):
    # Reuse the exact approved numeric artifact, then independently verify TRAIN provenance.
    if m == 'XGBoost':
        row = next(r for r in bench['tree_runs'] if r['cultivar'] == c and r['background_n'] == 32 and r['repeat'] == 'reference')
        path = ROOT / row['array_path']
        require(sha256(path) == row['array_sha256'], 'Tree background NPZ file hash')
        bundle = build_gc2_tabular_bundle(frame, build_gc2_feature_spec(c))
        train, val = bundle.train.X, bundle.validation.X
        dates = bundle.train.target_dates.dt.strftime('%Y-%m-%d').tolist()
        with np.load(path, allow_pickle=False) as f:
            bg = f['background'].copy()
    else:
        info = next(r for r in bench['models'] if r['cultivar'] == c and r['model'] == m)
        row = next(r for r in info['backgrounds'] if r['n'] == 32)
        path = ROOT / row['path']
        require(sha256(path) == row['file_sha256'], 'Neural background NPZ file hash')
        with np.load(path, allow_pickle=False) as f:
            bg, saved_indices = f['background'].copy(), f['indices'].copy()
        spec = build_feature_spec(c, m)
        a = frame.loc[:, spec.rama_a].to_numpy(float)
        b = frame.loc[:, spec.rama_b].to_numpy(float)
        # Pure TRAIN/VAL slices; same indexed inputs as official builder.
        all_x = flatten(np.stack([a[i-6:i] for i in range(6,102)]),
                        np.stack([b[i-6:i] for i in range(6,102)]))
        train, val = all_x[:84], all_x[84:]
        dates = frame.fecha.iloc[6:90].dt.strftime('%Y-%m-%d').tolist()
        require(saved_indices.tolist() == row['indices'], 'Stored background indices')
    idx = np.rint(np.linspace(0, len(train)-1, 32)).astype(int)
    expected_idx = bench['recommendation']['D55']['indices_xgboost' if m == 'XGBoost' else 'indices_neural']
    require(idx.tolist() == expected_idx and len(set(idx)) == 32, 'D55 exact indices')
    selected_dates = [dates[i] for i in idx]
    require(all(d <= '2023-12-01' for d in selected_dates), 'Background includes VAL/TEST')
    require(np.array_equal(bg, train[idx]), 'Background differs from TRAIN references')
    require(array_hash(bg) == BG_HASHES[f'{c}/{m}'], 'D55 approved background hash')
    return bg, val, dict(path=str(path), file_sha256=sha256(path), sha256_array=array_hash(bg),
                         indices=idx.tolist(), target_dates=selected_dates, train_n=len(train),
                         source='TRAIN', shape=list(bg.shape), selection='rint(linspace(0,n_train-1,32))')


def original_xgb_test_builder():
    # Reuse the pure original function verbatim; do not import/run its evaluator.
    path = AUDIT / 'evaluar_test_gc2_xgboost.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_build_test_arrays')
    ns = {'np': np, 'pd': pd, 'Any': object}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), 'exec'), ns)
    return ns['_build_test_arrays']


def validate_dates(dates):
    require(list(dates) == TEST_DATES and len(dates) == 12, 'TEST must be exactly Jan-Dec 2025, 12 ordered observations')


def validate_predictions(actual, frozen):
    actual, frozen = np.asarray(actual), np.asarray(frozen)
    require(actual.shape == frozen.shape == (12,), 'Prediction shape != (12,)')
    require(np.isfinite(actual).all() and np.isfinite(frozen).all(), 'Nonfinite predictions')
    error = float(np.max(np.abs(actual - frozen)))
    require(error <= PRED_ATOL, f'Frozen prediction mismatch: {error} > {PRED_ATOL} scaled (rtol=0)')
    return error


class PredictWrapper:
    def __init__(self, model, n_b):
        self.model = model
        @tf.function(input_signature=[tf.TensorSpec([None, 24+6*n_b], tf.float32)])
        def forward(x):
            return tf.reshape(model([tf.reshape(x[:, :24], [-1,6,4]),
                                     tf.reshape(x[:,24:], [-1,6,n_b])], training=False), [-1])
        self.forward = forward

    def __call__(self, x):
        x = np.asarray(x, dtype=np.float32)
        return np.concatenate([self.forward(x[i:i+1024]).numpy() for i in range(0,len(x),1024)]).astype(np.float64)


def aggregates(phi, groups, names, shock=None):
    # ABSOLUTE AT ELEMENT LEVEL FIRST. Signed phi is never overwritten.
    a = np.abs(phi)
    feat = a.mean(0) if a.ndim == 2 else a.sum(1).mean(0)
    order = list(dict.fromkeys(groups))
    out = dict(feature=feat.tolist(), elemental_mean_abs=a.mean(0).tolist(),
               timestep=None if a.ndim == 2 else a.sum(2).mean(0).tolist(),
               group={g: float(feat[np.array(groups) == g].sum()) for g in order})
    nlp = [i for i,n in enumerate(names) if n in NLP_LAGGED]
    if nlp:
        total = float(feat.sum())
        out['nlp'] = dict(names=[names[i] for i in nlp], mean_abs=feat[nlp].tolist(),
                          relative_total=None if total == 0 else (feat[nlp]/total).tolist(),
                          block_share=None if total == 0 else float(feat[nlp].sum()/total),
                          rank_names=[names[i] for i in sorted(nlp, key=lambda j: -feat[j])])
    if shock is not None:
        shock = np.asarray(shock, dtype=bool)
        require(shock.shape == (len(phi),) and shock.any() and (~shock).any(), 'Shock split empty/invalid')
        out['shock'] = aggregates(phi[shock], groups, names)
        out['nonshock'] = aggregates(phi[~shock], groups, names)
        out['shock_minus_nonshock_feature'] = (np.array(out['shock']['feature'])-out['nonshock']['feature']).tolist()
        out['shock_minus_nonshock_group'] = {g: out['shock']['group'][g]-out['nonshock']['group'][g] for g in order}
    return out


def seed_summary(by_seed):
    require(sorted(by_seed) == list(SEEDS), 'Seed selection/incomplete seed aggregation forbidden')
    stack = np.asarray([by_seed[s] for s in SEEDS], dtype=float)
    return dict(seeds=list(SEEDS), mean=stack.mean(0).tolist(), median=np.median(stack,axis=0).tolist(),
                sd=stack.std(0,ddof=1).tolist(), minimum=stack.min(0).tolist(), maximum=stack.max(0).tolist(),
                interpretation='Descriptive variability across all seeds; not a temporal confidence interval')


def summarize_all_aggregates(by_seed):
    """Summarize numeric A-E aggregates AFTER absolute aggregation per seed.

    Undefined zero-denominator shares remain undefined; no imputation or
    omission of seeds. Signed local values remain in each raw.npz.
    """
    require(sorted(by_seed)==list(SEEDS),'All seeds required for every aggregate')
    def walk(values):
        first=values[0]
        if isinstance(first,dict):
            require(all(set(v)==set(first) for v in values),'Inconsistent aggregate schema across seeds')
            return {k:walk([v[k] for v in values]) for k in first}
        if any(v is None for v in values):
            return dict(status='undefined',by_seed=values)
        a=np.asarray(values)
        if a.dtype.kind in 'fiu':
            return seed_summary(dict(enumerate(values)))
        return dict(by_seed=values)  # Names/ranks are labels, never averaged.
    return walk([by_seed[s] for s in SEEDS])


def model_memory_hash(model,m):
    if m=='XGBoost':
        return hashlib.sha256(model.get_booster().save_raw(raw_format='ubj')).hexdigest()
    return hashlib.sha256(''.join(array_hash(w) for w in model.get_weights()).encode()).hexdigest()


def explain(model, predict, bg, samples, m, *, scope, dates, official_authorized=False, sampling=False):
    if scope == 'VAL':
        require(all(str(d).startswith('2024-') for d in dates), 'TEST reached VAL explainer')
    elif scope == 'TEST':
        require(official_authorized, 'TEST explainer prohibited without explicit execution authorization')
        validate_dates(dates)
    else:
        raise GuardError('Unknown explanation scope')
    require(len(samples) == len(dates), 'Explanation dates/rows mismatch')
    random.seed(RNG)
    np.random.seed(RNG)
    started = time.perf_counter()
    method = 'TreeExplainer' if m == 'XGBoost' else 'SamplingExplainer' if sampling else 'PermutationExplainer'
    require(not sampling or m != 'XGBoost', 'No Tree fallback')
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            if m == 'XGBoost':
                ex = shap.TreeExplainer(model, data=bg, feature_perturbation='interventional', model_output='raw')
            elif sampling:
                ex = shap.SamplingExplainer(predict, bg, link='identity')
            else:
                ex = shap.PermutationExplainer(predict, shap.maskers.Independent(bg, max_samples=32),
                                               link=shap.links.identity, seed=RNG)
            values, bases, durations = [], [], []
            for sample in samples:
                t = time.perf_counter()
                if m == 'XGBoost':
                    result = ex(sample[None,:], check_additivity=True)
                    value, base = result.values, result.base_values
                elif sampling:
                    value = ex.shap_values(sample[None,:], nsamples=65536, min_samples_per_feature=100, silent=True)
                    base = ex.expected_value
                else:
                    result = ex(sample[None,:], max_evals=MAX_EVALS[m], batch_size=1024, silent=True)
                    value, base = result.values, result.base_values
                values.append(np.asarray(value).reshape(-1))
                bases.append(float(np.asarray(base).reshape(-1)[0]))
                durations.append(time.perf_counter()-t)
            values, bases = np.asarray(values), np.asarray(bases)
            pred = np.asarray(predict(samples), dtype=float).reshape(-1)
            reconstructed = bases + values.sum(1)
            if values.shape != samples.shape or not all(np.isfinite(a).all() for a in (values,bases,pred)):
                raise ValueError('Invalid SHAP shape/nonfinite values')
            residual = np.abs(reconstructed-pred)
            if residual.max() > RECON_ATOL:
                raise ValueError(f'Reconstruction {residual.max()} > {RECON_ATOL} scaled')
            if np.max(np.abs(bases-np.mean(predict(bg)))) > RECON_ATOL:
                raise ValueError('Expected value differs from TRAIN background mean prediction')
        except TimeoutError as exc:
            raise GuardError('Timeout/slowness is not an authorized fallback trigger') from exc
        except GuardError:
            raise
        except Exception as exc:
            raise TechnicalFailure(f'{method}: {type(exc).__name__}: {exc}') from exc
    return dict(flat_shap=values, elemental_shap=elemental(values,m), base_values=bases,
                predictions_explainer=reconstructed, predictions_model_scaled=pred,
                reconstruction_abs_error=residual), dict(method=method, scope=scope, seconds=time.perf_counter()-started,
                per_sample_seconds=durations, warnings=[str(w.message) for w in caught], rng=RNG,
                max_evals=MAX_EVALS.get(m) if method == 'PermutationExplainer' else None,
                nsamples=65536 if sampling else None, min_samples_per_feature=100 if sampling else None,
                batch_size=1024 if method == 'PermutationExplainer' else None, link='identity',
                masker='Independent' if method == 'PermutationExplainer' else None,
                max_samples=32, feature_perturbation='interventional' if m == 'XGBoost' else None,
                model_output='raw' if m == 'XGBoost' else 'scaled target')


def synthetic_checks(directory):
    tests = []
    def reject(name, fn):
        try:
            fn()
        except (GuardError, FileExistsError):
            tests.append(name)
        else:
            raise GuardError(f'Negative test did not reject: {name}')
    reject('wrong TEST date', lambda: validate_dates(['2024-12-01']+TEST_DATES[1:]))
    reject('11 TEST rows', lambda: validate_dates(TEST_DATES[:-1]))
    reject('prediction mismatch', lambda: validate_predictions(np.ones(12),np.zeros(12)))
    reject('missing seed', lambda: seed_summary({s:[1.] for s in range(9)}))
    reject('write frozen artifact', lambda: check_write_path(V2/'DECISIONES_METODOLOGICAS.md'))
    reject('TEST in VAL call', lambda: explain(None,None,None,np.zeros((12,222)),'GC3',scope='VAL',dates=TEST_DATES))
    reject('unauthorized TEST call', lambda: explain(None,None,None,np.zeros((12,222)),'GC3',scope='TEST',dates=TEST_DATES))
    reject('changed hash', lambda: verify_hashes({str(Path(__file__)): '0'*64}))
    for m in MODELS:
        names, groups, mapping = feature_map('sutil',m)
        p = len(mapping)
        flat = np.arange(2*p,dtype=float).reshape(2,p)-p
        phi = elemental(flat,m)
        for r in mapping:
            cell = phi[:,r['feature_index']] if m == 'XGBoost' else phi[:,r['timestep'],r['feature_index']]
            require(np.array_equal(cell, flat[:,r['flat_index']]), 'Branch/timestep/feature mapping roundtrip')
        summary = aggregates(phi,groups,names,[True,False])
        require(np.isclose(sum(summary['feature']),np.abs(flat).sum(1).mean()), 'Absolute conservation')
        require(np.isclose(sum(summary['group'].values()),sum(summary['feature'])), 'Group conservation')
        opposite = {s: aggregates(phi if s%2 else -phi,groups,names)['feature'] for s in SEEDS}
        require(np.allclose(seed_summary(opposite)['mean'],summary['feature']), 'Signed cancellation between seeds')
        if m == 'GE':
            require(aggregates(np.zeros_like(phi),groups,names)['nlp']['block_share'] is None, 'NLP zero denominator')
        all_aggs={s:aggregates(phi if s%2 else -phi,groups,names,[True,False]) for s in SEEDS}
        all_summary=summarize_all_aggregates(all_aggs)
        require(np.allclose(all_summary['feature']['mean'],summary['feature']),'All-seed aggregation')
        path = directory/f'synthetic_{m}.npz'
        save_npz(path,dict(flat_shap=flat,elemental_shap=phi,feature_names=np.array(names)))
        reject(f'overwrite {m}',lambda: save_npz(path,dict(flat_shap=flat)))
        tests.append(f'{m}: elemental mapping, absolute aggregates, shocks, signed seed cancellation, NPZ')
        # Exercise the COMPLETE official serializer with explicitly synthetic
        # arrays, 12 rows, and VAL dates. No model or explainer called here.
        fake_flat=np.zeros((12,p),dtype=float)
        fake_data=dict(mapping=mapping,names=names,groups=groups,dates=[f'2024-{i:02d}-01' for i in range(1,13)],
            contexts=[['synthetic']]*12,x=fake_flat,bg=np.zeros((32,p)),mean=0.,scale=1.,
            bgmeta=dict(indices=list(range(32)),source='SYNTHETIC'))
        fake_frozen=pd.DataFrame(dict(y_pred_scaled=np.zeros(12),y_pred_ton=np.zeros(12),seed=np.zeros(12,dtype=int),
            is_shock=[True]*3+[False]*9))
        fake_arrays=dict(flat_shap=fake_flat,elemental_shap=elemental(fake_flat,m),base_values=np.zeros(12),
            predictions_explainer=np.zeros(12),predictions_model_scaled=np.zeros(12),reconstruction_abs_error=np.zeros(12))
        dest=directory/f'synthetic_full_serializer_{m}'
        store_official_seed(dest,fake_data,fake_frozen,fake_arrays,dict(scope='SYNTHETIC',method='NONE'),
                            dict(environment={},hashes={}),f'SYNTHETIC/{m}/seed_00')
        require(read_json(dest/'metadata.json')['explainer']['scope']=='SYNTHETIC','Synthetic output label')
        tests.append(f'{m}: complete raw/metadata/aggregates serializer (synthetic, not TEST SHAP)')
    return tests


def validation(report, run_dir, *, technical_probes):
    bench = read_json(AUDIT/'shap_v2_explainer_benchmark.json')
    comp, neu_manifest, xgb_manifest = historical_checks(bench,report)
    report['controls'].append(dict(name='Historical freeze: five families and historical SHA256',status='PASS'))
    items = inventory()
    report['controls'].append(dict(name='40 neural + 20 XGBoost; all seeds 0..9',status='PASS'))
    report['test_reads'].append(dict(path=comp['sources']['gc3_ge_predictions'],purpose='Read frozen predictions, dates, masks and model hashes',rows=480))
    report['test_reads'].append(dict(path=comp['sources']['xgb_predictions'],purpose='Read frozen predictions, dates and masks',rows=240))
    npred = pd.read_csv(comp['sources']['gc3_ge_predictions']).rename(columns={'fecha':'date','modelo':'model'})
    xpred = pd.read_csv(comp['sources']['xgb_predictions'])
    xpred['model'] = xpred['model'].replace({'GC2_XGBoost':'XGBoost'})
    xpred['date'] = pd.to_datetime(xpred.date).dt.strftime('%Y-%m-%d')
    predictions = pd.concat([npred,xpred],ignore_index=True)
    require(len(predictions)==720 and set(zip(predictions.cultivar,predictions.model,predictions.seed)) ==
            {(c,m,s) for c,m,s,_,_ in items}, 'Frozen predictions exact inventory')
    cache, loaded = {}, {}
    for c in CULTIVARS:
        bounded = bounded_frame(c)
        path = V2/f'data/processed/master_dataset_{c}_v2_escalado.csv'
        # Explicit integrity-only TEST exception granted by the user. Original
        # evaluator loader and builder, not an alternate reconstruction.
        report['test_reads'].append(dict(path=str(path),purpose='Integrity only: original TEST builder, dates/shapes/predictions',
                                        rows=114,test_rows=12,first='2016-07-01',last='2025-12-01',explainer_test_calls=0))
        frame = load_feature_dataframe(path)
        require(frame.fecha.tolist()==pd.date_range('2016-07-01','2025-12-01',freq='MS').tolist(), 'Full frozen calendar')
        scaler_path = RESULTS/f'scalers/scaler_{c}_v2c.joblib'
        scaler = joblib.load(scaler_path)
        require(type(scaler).__name__=='StandardScaler' and int(scaler.n_samples_seen_)==90, 'Frozen TRAIN-only scaler')
        target = f'produccion_t_{c}'
        j = list(scaler.feature_names_in_).index(target)
        mean, scale = float(scaler.mean_[j]),float(scaler.scale_[j])
        param = pd.read_csv(RESULTS/f'scalers/scaler_{c}_v2c_parametros.csv').set_index('feature').loc[target]
        require(np.isclose(mean,param.mean_train,rtol=0,atol=1e-10) and np.isclose(scale,param.scale_train,rtol=0,atol=1e-10), 'Scaler CSV/joblib consistency')
        for m in MODELS:
            names, groups, mapping = feature_map(c,m)
            bg,val,bgmeta = load_background(c,m,bounded,bench)
            if m == 'XGBoost':
                test = original_xgb_test_builder()(frame,tuple(names),target)
                x = test['X']
                dates = [d+'-01' for d in test['target_dates']]
                require(test['x_dates']==pd.date_range('2024-12-01','2025-11-01',freq='MS').strftime('%Y-%m').tolist(), 'XGBoost origins')
                raw_a = raw_b = None
                y = test['y_true_scaled']
            else:
                bundle = build_sequences(frame,build_feature_spec(c,m))
                split = bundle.test
                raw_a,raw_b = split.X_a,split.X_b
                require(raw_a.shape==(12,6,4) and raw_b.shape==(12,6,33 if m=='GC3' else 39), 'Neural TEST tensors')
                x = flatten(raw_a,raw_b)
                dates = split.target_dates.dt.strftime('%Y-%m-%d').tolist()
                require(np.array_equal(val,flatten(bundle.validation.X_a,bundle.validation.X_b)), 'Bounded VAL differs from official builder')
                y = split.y
                # Direct row-by-row identity with the frozen scaled CSV, all 12 contexts.
                for k in range(12):
                    expected = frame.loc[96+k:101+k,names].to_numpy(float)
                    require(np.array_equal(raw_a[k],expected[:,:4]) and np.array_equal(raw_b[k],expected[:,4:]), 'Official TEST context identity')
            validate_dates(dates)
            require(np.isfinite(x).all() and np.isfinite(bg).all(), 'Inputs finite')
            context_dates = [[(pd.Timestamp(d)-pd.DateOffset(months=i)).strftime('%Y-%m-%d')
                              for i in ([1] if m=='XGBoost' else range(6,0,-1))] for d in dates]
            cache[c,m] = dict(x=x, raw_a=raw_a,raw_b=raw_b,y=y,val=val,bg=bg,bgmeta=bgmeta,
                              dates=dates,contexts=context_dates,names=names,groups=groups,mapping=mapping,mean=mean,scale=scale)
            report['backgrounds'][f'{c}/{m}'] = bgmeta
            report['inputs'][f'{c}/{m}'] = dict(test_shape=list(x.shape),test_hash=array_hash(x),
                raw_a_hash=None if raw_a is None else array_hash(raw_a),raw_b_hash=None if raw_b is None else array_hash(raw_b),
                feature_names=names,mapping=mapping,dates=dates,contexts=context_dates,
                scaler_mean=mean,scaler_scale=scale,prediction_atol_ton=PRED_ATOL*scale)
            report['controls'].append(dict(name=f'{c}/{m}: D55 hash/TRAIN, feature map, shapes and t -> t+1 calendar',status='PASS'))
        require(np.array_equal(cache[c,'GC3']['x'],flatten(cache[c,'GE']['raw_a'],cache[c,'GE']['raw_b'][:,:,:33])), 'GC3/GE shared TEST values')
    for c,m,s,d,path in items:
        started = time.perf_counter()
        key = f'{c}/{m}/seed_{s:02d}'
        data = cache[c,m]
        cfg,meta = read_json(d/'config.json'),read_json(d/'metadata.json')
        require(cfg['seed']==meta['seed']==s and cfg['cultivar']==meta['cultivar']==c, f'{key}: identity')
        require(meta['dataset_hash']==sha256(V2/f'data/processed/master_dataset_{c}_v2_escalado.csv') and
                meta['scaler_hash']==sha256(RESULTS/f'scalers/scaler_{c}_v2c.joblib'), f'{key}: training input hashes')
        frozen = predictions[(predictions.cultivar==c)&(predictions.model==m)&(predictions.seed==s)]
        validate_dates(frozen.date.tolist())
        expected_mask = np.array([d[:7] in OFFICIAL_TEST_SHOCKS_2025[c] for d in data['dates']])
        require(frozen.is_shock.dtype == bool and np.array_equal(frozen.is_shock,expected_mask) and expected_mask.sum()==3, f'{key}: frozen shocks')
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            if m == 'XGBoost':
                require(cfg['features']==data['names'], f'{key}: exact 37 features')
                require(sha256(path)==xgb_manifest['hashes']['models'][f'{c}_seed_{s:02d}'], f'{key}: frozen model hash')
                model = joblib.load(path)
                require(isinstance(model,xgboost.XGBRegressor) and model.n_features_in_==37 and model.get_params()['random_state']==s, f'{key}: loaded XGBoost')
                require(all(model.get_params().get(k)==v for k,v in cfg['xgb_params'].items()), f'{key}: loaded XGB config')
                predict = model.predict
                actual = predict(data['x']).astype(float)
                original_error = validate_predictions(actual,frozen.y_pred_scaled.to_numpy(float))
            else:
                require(cfg['features_a']+cfg['features_b']==data['names'] and cfg['training_config']['lookback']==6 and cfg['official_result'], f'{key}: features/lookback')
                require(cfg['test_loaded_for_fit'] is False and cfg['test_used_for_training'] is False, f'{key}: no TEST training')
                require(set(frozen.checkpoint_sha256)=={sha256(path)}, f'{key}: frozen checkpoint hash')
                model = keras.models.load_model(path,custom_objects={'BahdanauAttention':BahdanauAttention},compile=False)
                require(model.input_shape==[(None,6,4),(None,6,33 if m=='GC3' else 39)] and model.output_shape==(None,1), f'{key}: model input/output shapes')
                require(model.count_params()==(6273 if m=='GC3' else 6657), f'{key}: parameter count')
                # Exact evaluator inference on original float64 tensors, then SHAP wrapper.
                original = model.predict([data['raw_a'],data['raw_b']],verbose=0).reshape(-1).astype(float)
                original_error = validate_predictions(original,frozen.y_pred_scaled.to_numpy(float))
                predict = PredictWrapper(model,33 if m=='GC3' else 39)
                actual = predict(data['x'])
                validate_predictions(actual,original)
            error = validate_predictions(actual,frozen.y_pred_scaled.to_numpy(float))
            ton_error = float(np.max(np.abs(actual*data['scale']+data['mean']-frozen.y_pred_ton.to_numpy(float))))
            require(ton_error<=PRED_ATOL*data['scale']+1e-8, f'{key}: frozen tonnes mismatch')
            require(np.allclose(data['y'],frozen.y_true_scaled,rtol=0,atol=1e-12),f'{key}: original target alignment')
        row = dict(key=key,model_sha256=sha256(path),prediction_max_abs_scaled=error,
                   original_evaluator_max_abs_scaled=original_error,prediction_max_abs_ton=ton_error,
                   load_warnings=[str(w.message) for w in caught],seconds=time.perf_counter()-started)
        weights_before=model_memory_hash(model,m)
        loaded[c,m,s] = (model,predict,frozen)
        if technical_probes:
            # Only January VAL 2024. Every model/seed, fixed before any results.
            arrays,exmeta = explain(model,predict,data['bg'],data['val'][:1],m,scope='VAL',dates=['2024-01-01'])
            probe_path=run_dir/f'VAL_{c}_{m}_seed_{s:02d}.npz'
            save_npz(probe_path,arrays)
            row['val_probe'] = dict(explainer=exmeta,path=str(probe_path),sha256=sha256(probe_path),
                                    reconstruction_max=float(arrays['reconstruction_abs_error'].max()))
            report['val_explanations'] += 1
        require(model_memory_hash(model,m)==weights_before,f'{key}: model changed in memory')
        row['model_memory_hash']=weights_before
        row['model_unchanged_in_memory']=True
        report['models'].append(row)
        report['controls'].append(dict(name=f'{key}: load, features, hashes, prediction integrity, shocks'+(', VAL explainer/NPZ' if technical_probes else ''),status='PASS'))
        print(f'PASS {key}: prediction integrity; '+('VAL probe' if technical_probes else 'no SHAP'),flush=True)
    require(len(loaded)==60,'All 60 models must pass before official explanation')
    return cache,loaded


def store_official_seed(path,data,frozen,arrays,exmeta,report,key):
    path.mkdir(parents=True,exist_ok=False)
    mapping=data['mapping']
    arrays.update(predictions_frozen_scaled=frozen.y_pred_scaled.to_numpy(float),
                  predictions_frozen_ton=frozen.y_pred_ton.to_numpy(float),
                  target_dates=np.array(data['dates']),context_dates=np.array(data['contexts']),
                  input_scaled=data['x'],background=data['bg'],background_indices=np.array(data['bgmeta']['indices']),
                  feature_names=np.array(data['names']),groups=np.array(data['groups']),
                  flat_feature_names=np.array([r['feature'] for r in mapping]),
                  flat_feature_index=np.array([r['feature_index'] for r in mapping]),
                  flat_timestep=np.array([r['timestep'] for r in mapping]),
                  flat_branch=np.array([r['branch'] for r in mapping]),is_shock=frozen.is_shock.to_numpy(bool))
    save_npz(path/'raw.npz',arrays)  # ALWAYS raw before aggregation.
    write_json(path/'metadata.json',dict(key=key,units='scaled target',seed=int(frozen.seed.iloc[0]),
                background=data['bgmeta'],explainer=exmeta,environment=report['environment'],
                hashes=report['hashes'],raw_sha256=sha256(path/'raw.npz'),feature_mapping=mapping,
                target_inverse=dict(mean=data['mean'],scale=data['scale']),
                signed_shap_preserved=True,causal_interpretation=False,attention_generated=False))
    agg=aggregates(arrays['elemental_shap'],data['groups'],data['names'],frozen.is_shock.to_numpy(bool))
    write_json(path/'aggregates_descriptive.json',agg)
    return agg


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute-test',action='store_true',help='Requires NEW explicit user authorization; absent means preflight')
    p.add_argument('--approved-preflight',type=Path)
    p.add_argument('--preflight-sha256')
    p.add_argument('--resume-technical-failure',type=Path)
    p.add_argument('--failure-sha256')
    # No seed/model/cultivar selectors, method selectors, or adjustable budgets.
    return p


def main():
    args=parser().parse_args()
    tf.config.threading.set_intra_op_parallelism_threads(1)
    tf.config.threading.set_inter_op_parallelism_threads(1)
    tf.config.experimental.enable_op_determinism()
    keras.utils.set_random_seed(RNG)
    OUTPUT.mkdir(parents=True,exist_ok=True)
    # TensorFlow AutoGraph may create Python source files. Keep even those
    # runtime files under SHAP; no permissions widened for frozen artifacts.
    runtime_tmp=OUTPUT/'_runtime_tmp'
    runtime_tmp.mkdir(exist_ok=True)
    tempfile.tempdir=str(runtime_tmp)
    os.environ.update(TEMP=str(runtime_tmp),TMP=str(runtime_tmp),NUMBA_CACHE_DIR=str(runtime_tmp/'numba'))
    sys.addaudithook(readonly_hook)
    official=args.execute_test
    if not official:
        require(not any((args.approved_preflight,args.preflight_sha256,args.resume_technical_failure,args.failure_sha256)), 'Execution-only flags in preflight')
    if official:
        require(args.approved_preflight and args.preflight_sha256,'Approved preflight path AND SHA256 required')
        require(args.approved_preflight.resolve().is_relative_to((OUTPUT/'_preflight').resolve()),'Preflight must be an immutable SHAP preflight artifact')
        require(sha256(args.approved_preflight)==args.preflight_sha256,'Approved preflight file changed')
        approved=read_json(args.approved_preflight)
        require(approved['status']=='LISTO PARA EJECUCION SHAP TEST OFICIAL' and approved['test_explanations']==0,'Preflight not approved')
        verify_hashes(approved['hashes'])  # Before any TEST parsing/inference/explanation.
        require(approved['environment']==environment(),'Frozen execution environment changed')
        require(approved['policy']==json.loads(json.dumps(POLICY)),'Frozen protocol changed')
        run_dir=OUTPUT/'official_test_2025'
    else:
        run_dir=OUTPUT/'_preflight'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    resume=None
    if args.resume_technical_failure:
        require(args.failure_sha256 and args.resume_technical_failure.resolve()==(run_dir/'technical_failure.json').resolve(),'Invalid technical failure record')
        require(sha256(args.resume_technical_failure)==args.failure_sha256,'Technical failure record hash changed')
        resume=read_json(args.resume_technical_failure)
        require(resume['preflight_sha256']==args.preflight_sha256 and resume['method']=='PermutationExplainer' and resume['exception_type']=='TechnicalFailure','Fallback without actual technical failure forbidden')
        require(not (run_dir/'complete.json').exists(),'Official run already complete')
        verify_hashes(resume['completed_artifact_hashes']) if resume['completed_artifact_hashes'] else None
    else:
        require(not args.failure_sha256,'Failure SHA without failure record')
        run_dir.mkdir(parents=True,exist_ok=False)
    report=dict(status='RUNNING',started_utc=datetime.now(timezone.utc).isoformat(),mode='official' if official else 'preflight',
                policy=POLICY,environment=environment(),git=git_info(),controls=[],test_reads=[],inputs={},backgrounds={},models=[],
                test_explanations=0,val_explanations=0,hashes=snapshot(),output_directory=str(run_dir),
                no_training=True,no_seed_selection=True,no_predictive_metrics=True,attention_generated=False,
                official_methods={},fallback_event=resume)
    # Capture native TensorFlow diagnostics as well as Python stderr.
    stderr_file=(run_dir/('resume_stderr.log' if resume else 'execution_stderr.log')).open('x',encoding='utf-8')
    saved_stderr=os.dup(2)
    os.dup2(stderr_file.fileno(),2)
    write_json(run_dir/f'start_{report["started_utc"].replace(":","-")}.json',report)
    try:
        if not official:
            report['synthetic_tests']=synthetic_checks(run_dir)
            report['controls'].append(dict(name='Adversarial guards, mapping, serialization and aggregation tests',status='PASS'))
        cache,loaded=validation(report,run_dir,technical_probes=not official)
        verify_hashes(report['hashes'])
        report['controls'].append(dict(name='All frozen SHA256 unchanged after model loading and validation',status='PASS'))
        if official:
            require({k:v['test_hash'] for k,v in report['inputs'].items()} ==
                    {k:v['test_hash'] for k,v in approved['inputs'].items()}, 'TEST tensor hash changed since preflight')
            require(snapshot()==approved['hashes'],'Frozen inventory changed since preflight')
            for c in CULTIVARS:
                for m in MODELS:
                    per_seed={}
                    for s in SEEDS:
                        key=f'{c}/{m}/seed_{s:02d}'
                        dest=run_dir/c/m/f'seed_{s:02d}'
                        data=cache[c,m]
                        model,predict,frozen=loaded[c,m,s]
                        if resume and (dest/'aggregates_descriptive.json').exists():
                            require(str(dest/'raw.npz') in resume['completed_artifact_hashes'],'Unverified prior raw output')
                            per_seed[s]=read_json(dest/'aggregates_descriptive.json')
                            report['official_methods'][key]=read_json(dest/'metadata.json')['explainer']['method']
                            report['test_explanations']+=12
                            continue
                        sampling=bool(resume and resume['key']==key)
                        weights_before=model_memory_hash(model,m)
                        try:
                            arrays,exmeta=explain(model,predict,data['bg'],data['x'],m,scope='TEST',dates=data['dates'],
                                                  official_authorized=True,sampling=sampling)
                        except TechnicalFailure as exc:
                            failure=dict(key=key,exception_type='TechnicalFailure',error=str(exc),traceback=traceback.format_exc(),
                                method='TreeExplainer' if m=='XGBoost' else 'SamplingExplainer' if sampling else 'PermutationExplainer',
                                preflight_sha256=args.preflight_sha256,timestamp_utc=datetime.now(timezone.utc).isoformat(),
                                completed_artifact_hashes={str(p):sha256(p) for c2 in CULTIVARS for p in (run_dir/c2).rglob('*') if p.is_file()},
                                action='STOP. Documented technical failure; no automatic fallback. Explicit resume with record SHA256 required.')
                            write_json(run_dir/('fallback_failure.json' if resume else 'technical_failure.json'),failure)
                            raise
                        require(model_memory_hash(model,m)==weights_before,f'{key}: model changed during explanation')
                        exmeta['model_memory_sha256']=weights_before
                        exmeta['model_unchanged_in_memory']=True
                        report['official_methods'][key]=exmeta['method']
                        agg=store_official_seed(dest,data,frozen,arrays,exmeta,report,key)
                        per_seed[s]=agg
                        report['test_explanations']+=12
                    summary_path=run_dir/c/m/'seed_variability_all_aggregates.json'
                    if not summary_path.exists():
                        write_json(summary_path,summarize_all_aggregates(per_seed))
            require(report['test_explanations']==720,'Incomplete official explanation inventory')
        verify_hashes(report['hashes'])
        report['controls'].append(dict(name='No frozen writes; all SHA256 unchanged at exit',status='PASS'))
        report['controls'].append(dict(name='No TEST SHAP in preflight; only 60 fixed VAL probes',status='PASS' if official or (report['test_explanations']==0 and report['val_explanations']==60) else 'FAIL'))
        require(all(c['status']=='PASS' for c in report['controls']),'Preflight controls failed')
        report['status']='SHAP TEST COMPLETE' if official else 'LISTO PARA EJECUCION SHAP TEST OFICIAL'
    except Exception as exc:
        report['status']='BLOQUEADO'
        report['controls'].append(dict(name=str(exc),status='FAIL'))
        report['exception']=traceback.format_exc()
        raise
    finally:
        try:
            verify_hashes(report['hashes'])
        except Exception as integrity_error:
            report['status']='BLOQUEADO'
            report['controls'].append(dict(name=f'Exit integrity: {integrity_error}',status='FAIL'))
        os.dup2(saved_stderr,2)
        os.close(saved_stderr)
        stderr_file.close()
        report['finished_utc']=datetime.now(timezone.utc).isoformat()
        report['script_sha256']=sha256(__file__)
        target=run_dir/('complete.json' if official and report['status']=='SHAP TEST COMPLETE' else
                        'resume_report.json' if resume else 'preflight.json' if not official else 'official_report.json')
        write_json(target,report)
        print(json.dumps(dict(status=report['status'],report=str(target),sha256=sha256(target),
                              test_explanations=report['test_explanations'],val_explanations=report['val_explanations'])),flush=True)


if __name__=='__main__':
    main()
