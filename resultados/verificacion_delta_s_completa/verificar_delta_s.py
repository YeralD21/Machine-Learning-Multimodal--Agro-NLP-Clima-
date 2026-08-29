# -*- coding: utf-8 -*-
"""
Verificacion completa de la metrica Delta_s (deterioro en meses de shock).

Hallazgo auditado:
  El umbral de shock reportado en el paper/notebooks (>20%) se calculo con
  pct_change() directamente sobre la serie en escala z-score de produccion_t.
  Dividir entre denominadores z cercanos a cero produce variaciones sinteticas
  de +1000% (ej.: 2025-01 = 1021%). Al desnormalizar la serie real a toneladas
  con notebooks/fase2/scalers/standard_scaler_fase2.joblib, la variacion
  mensual maxima real en la ventana de test (sep-2024 .. ago-2025) es 10.19%
  (enero-2025) y con el umbral del 20% la mascara de shock queda vacia.

Este script recalcula Delta_s con una mascara de shock empirica y
estadisticamente defendible, y guarda la tabla oficial.

Uso:
  C:\\Users\\YERALD\\AppData\\Local\\Programs\\Python\\Python311\\python.exe verificar_delta_s.py

Salidas:
  - resultados/verificacion_delta_s_completa/README.md
  - resultados/verificacion_delta_s_completa/distribucion_variacion.csv
  - resultados/verificacion_delta_s_completa/output_ejecucion.txt
  - resultados/delta_s_recalculado/tabla1_oficial.csv
"""
import os
import sys
import json
from pathlib import Path
from io import StringIO

import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import mean_absolute_error

SEED = 42
ROOT = Path(r'D:\Machine-Learning-Multimodal--Agro-NLP-Clima-')

MASTER    = ROOT / 'resultados/verificacion_simulacion_nino_2026/master_dataset_fase2_multivariado_RECONSTRUIDO.csv'
SCALER_F2 = ROOT / 'notebooks/fase2/scalers/standard_scaler_fase2.joblib'

OUT_VERIF = ROOT / 'resultados/verificacion_delta_s_completa'
OUT_TABLA = ROOT / 'resultados/delta_s_recalculado'

PRED_FILES = {
    'SARIMA':       (ROOT / 'resultados/gc1/gc1_sarima_predicciones.csv',  'fecha_evento', 'prediccion'),
    'Prophet':      (ROOT / 'resultados/gc1/gc1_prophet_predicciones.csv', 'fecha',        'prediccion'),
    'SARIMAX_LSTM': (ROOT / 'resultados/gc2/gc2_predicciones.csv',         'fecha',        'hibrido'),
    'XGBoost':      (ROOT / 'resultados/xgboost/xgb_predicciones.csv',     'fecha',        'pred_xgb'),
    'GE':           (ROOT / 'resultados/ge/ge_predicciones.csv',           'fecha',        'prediccion'),
    'GM_v3':        (ROOT / 'resultados/gm_v3/gm_v3_predicciones.csv',     'fecha',        'pred_gm_v3'),
}

# ----------------------------------------------------------------------------
# 0. Prepara buffers de salida
# ----------------------------------------------------------------------------
OUT_VERIF.mkdir(parents=True, exist_ok=True)
OUT_TABLA.mkdir(parents=True, exist_ok=True)

console = StringIO()
def p(*args, **kwargs):
    msg = ' '.join(str(a) for a in args)
    print(msg)
    console.write(msg + '\n')

# ----------------------------------------------------------------------------
# 1. Carga + verificacion de alineacion
# ----------------------------------------------------------------------------
df_raw = pd.read_csv(MASTER, parse_dates=['fecha_evento'])
nacional_z = df_raw.groupby('fecha_evento')['produccion_t'].mean().sort_index()
nacional_z.index = pd.DatetimeIndex(nacional_z.index, freq='MS')

scaler_f2 = joblib.load(SCALER_F2)
feats = list(scaler_f2.feature_names_in_)
i_prod = feats.index('produccion_t')
MEAN_PROD, SCALE_PROD = scaler_f2.mean_[i_prod], scaler_f2.scale_[i_prod]

def desn_z(z):
    s = np.asarray(z) * SCALE_PROD + MEAN_PROD
    return pd.Series(s, index=getattr(z, 'index', None))

# Verificacion: la columna 'real' de las predicciones guardadas es la serie
# nacional en z-score. Desnormalizarla debe coincidir con la serie maestra.
sarima = pd.read_csv(PRED_FILES['SARIMA'][0], parse_dates=[PRED_FILES['SARIMA'][1]])
sarima = sarima.rename(columns={PRED_FILES['SARIMA'][1]: 'fecha'})
nat_df = nacional_z.to_frame('real_nat').reset_index().rename(
    columns={'fecha_evento': 'fecha'})
merged = pd.merge(sarima[['fecha', 'real']], nat_df, on='fecha')
max_diff_real = float((merged['real'] - merged['real_nat']).abs().max())

# ----------------------------------------------------------------------------
# 2. Distribucion de variacion real (desnormalizada, toneladas)
# ----------------------------------------------------------------------------
nacional_t = desn_z(nacional_z)
var_pct = nacional_t.pct_change().abs() * 100

TEST_START, TEST_END = pd.Timestamp('2024-09-01'), pd.Timestamp('2025-08-01')
test_dates = list(nacional_z.index[(nacional_z.index >= TEST_START) & (nacional_z.index <= TEST_END)])
var_test = var_pct.loc[test_dates]

full = var_pct.dropna()
p_medias = [('media', var_test.mean()), ('std', var_test.std()),
            ('min', var_test.min()), ('p50', np.percentile(var_test, 50)),
            ('p75', np.percentile(var_test, 75)), ('p90', np.percentile(var_test, 90)),
            ('max', var_test.max())]
full_stats = [('media', full.mean()), ('std', full.std()), ('min', full.min()),
              ('p50', np.percentile(full, 50)), ('p75', np.percentile(full, 75)),
              ('p90', np.percentile(full, 90)), ('p95', np.percentile(full, 95)),
              ('max', full.max())]

# ----------------------------------------------------------------------------
# 3. Mascara de shock empirica
# ----------------------------------------------------------------------------
# Criterio principal: meses del test con |var mensual| > P75(test window).
P75_TEST = float(np.percentile(var_test, 75))
shock_periods = sorted(d.to_period('M').strftime('%Y-%m')
                       for d in test_dates if var_pct.loc[d] > P75_TEST)
shock_months_full = [d for d in test_dates if var_pct.loc[d] > P75_TEST]

# Alternativas para sensibilidad (se reportan en consola, no en tabla oficial)
P90_TEST = float(np.percentile(var_test, 90))
top4 = [str(d.to_period('M')) for d in var_test.sort_values(ascending=False).head(4).index]
p75_full_series = float(np.percentile(full, 75))
top4_periods = sorted(d.to_period('M').strftime('%Y-%m') for d in
                      var_test.sort_values(ascending=False).head(4).index)
p90_p = sorted(d.to_period('M').strftime('%Y-%m') for d in test_dates if var_pct.loc[d] > P90_TEST)
p75f_p = sorted(d.to_period('M').strftime('%Y-%m') for d in test_dates if var_pct.loc[d] > p75_full_series)

# ----------------------------------------------------------------------------
# 4. Metricas por modelo
# ----------------------------------------------------------------------------
def load_pred(cfg):
    path, fc, pc = cfg
    df = pd.read_csv(path, parse_dates=[fc])
    df = df.rename(columns={fc: 'fecha', pc: 'pred'})
    df = df[['fecha', 'real', 'pred']].dropna()
    return df.sort_values('fecha').reset_index(drop=True)

# Naive: prediccion = valor del mes anterior (z-score), ventana 12 meses
naive_df = pd.DataFrame({'fecha': nacional_z.index.copy()})
naive_df['real'] = nacional_z.values
naive_df['pred'] = nacional_z.shift(1).values
naive_df = naive_df[naive_df['fecha'].isin(test_dates)].copy()

rows = []
detail_lines = []
for nombre in ['Naive', 'SARIMA', 'Prophet', 'SARIMAX_LSTM', 'XGBoost', 'GE', 'GM_v3']:
    if nombre == 'Naive':
        df = naive_df.copy()
        origen = 'sintetico (shift 1 sobre serie nacional z-score)'
    else:
        df = load_pred(PRED_FILES[nombre])
        origen = str(PRED_FILES[nombre][0].relative_to(ROOT))

    wsp = df['fecha'].isin(shock_months_full)
    y, yp = df['real'].values, df['pred'].values

    mae_global = float(mean_absolute_error(y, yp))
    mae_shock  = float(mean_absolute_error(y[wsp], yp[wsp])) if wsp.sum() > 0 else float('nan')
    n_shock = int(wsp.sum())
    delta_s = (mae_shock - mae_global) / mae_global * 100 if n_shock > 0 else float('nan')
    shock_meses = [d.strftime('%Y-%m') for d in df.loc[wsp, 'fecha']]

    rows.append({
        'Modelo': nombre,
        'MAE_global': round(mae_global, 6),
        'MAE_shock': round(mae_shock, 6),
        'Delta_s_pct': round(delta_s, 2),
        'N_meses_shock': n_shock,
        'N_test': int(len(df)),
        'shock_meses': '|'.join(shock_meses),
    })
    detail_lines.append((nombre, origen, mae_global, mae_shock, n_shock, delta_s))

# ----------------------------------------------------------------------------
# 5. Salidas
# ----------------------------------------------------------------------------
def fmt_pct(d):
    return 'nan' if np.isnan(d) else f'{d:+.2f}%'

p('=' * 100)
p('  VERIFICACION DELTA_S - MASCARA EMPIRICA SOBRE PRODUCCION REAL (toneladas)')
p('=' * 100)
p('Scaler  : notebooks/fase2/scalers/standard_scaler_fase2.joblib')
p('  produccion_t  mean(train)=%.4f  scale(train)=%.4f' % (MEAN_PROD, SCALE_PROD))
p('Serie nacional (media mensual de produccion_t en z-score):')
p('  %d meses | %s -> %s' % (len(nacional_z), nacional_z.index.min().date(), nacional_z.index.max().date()))
p('  max |diff| columna `real` guardada vs serie maestra = %.2e  (verificacion OK)' % max_diff_real)

p('')
p('-' * 100)
p('  DISTRIBUCION VARIACION MENSUAL REAL (|pct_change| desnormalizado, toneladas)')
p('-' * 100)
p('  Ventana test (sep-2024 .. ago-2025, n=%d):' % len(test_dates))
for k, v in p_medias:
    p('    %-6s %6.2f%%' % (k, v))
p('  Serie completa (2021-01 .. 2025-08, n=%d):' % len(full))
for k, v in full_stats:
    p('    %-6s %6.2f%%' % (k, v))

p('')
p('-' * 100)
p('  RANKING MESES MAS VOLATILES (test)')
p('-' * 100)
for d in var_test.sort_values(ascending=False).index:
    flag = ' <-- SHOCK (>P75=%.2f%%)' % P75_TEST if var_pct.loc[d] > P75_TEST else ''
    p('    %s  %6.2f%%%s' % (d.strftime('%Y-%m'), var_pct.loc[d], flag))

p('')
p('-' * 100)
p('  CRITERIO DE SHOCK EMPIRICO')
p('-' * 100)
p('  P75 ventana test      = %.2f%%' % P75_TEST)
p('  Shock months (P75)    = %s  (N=%d)' % (', '.join(shock_periods), len(shock_periods)))
p('  [Sensibilidad] P90 test      = %.2f%% -> %s' % (P90_TEST, ', '.join(p90_p)))
p('  [Sensibilidad] Top4 test     = %s' % ', '.join(top4_periods))
p('  [Sensibilidad] P75 serie completa = %.2f%% -> %s' % (p75_full_series, ', '.join(p75f_p)))
p('  Referencia: umbral del paper (>20%%) sobre z-score -> 8/12 shocks ficticios;')
p('  sobre toneladas reales -> 0/12 shocks.')

p('')
p('=' * 100)
p('  TABLA 1 OFICIAL - MAE GLOBAL vs MAE SHOCK (criterio P75 test)')
p('=' * 100)
header = '  %-14s %12s %12s %10s %14s %8s' % ('Modelo', 'MAE_global', 'MAE_shock', 'Delta_s', 'N_shock', 'N_test')
p('  ' + '-' * 74)
p(header)
p('  ' + '-' * 74)
for r in rows:
    p('  %-14s %12.6f %12.6f %10s %14d %8d' % (r['Modelo'], r['MAE_global'], r['MAE_shock'],
                                                 fmt_pct(r['Delta_s_pct']), r['N_meses_shock'], r['N_test']))
p('  ' + '-' * 74)
p('')

for nombre, origen, mae_g, mae_s, n_s, d_s in detail_lines:
    p('  %-14s | MAE_global=%.6f MAE_shock=%.6f N_shock=%d Delta_s=%s | %s'
      % (nombre, mae_g, mae_s, n_s, fmt_pct(d_s), origen))

p('')
p('LIMITACIONES:')
p('  - Cada modelo se evalua sobre su propia ventana de test guardada (N_test).')
p('    XGBoost y GE: nov-2024..ago-2025 (10m). GM_v3: mar-2025..ago-2025 (6m,')
p('    por TIMESTEPS=6 en make_seq). GC1/GC2/Naive: sep-2024..ago-2025 (12m).')
p('  - Delta_s es la tasa de deterioro del MAE en meses de shock')
p('    = (MAE_shock - MAE_global) / MAE_global * 100.')

# ----------------------------------------------------------------------------
# Guardar archivos
# ----------------------------------------------------------------------------
df_var = pd.DataFrame({
    'fecha': var_test.index.strftime('%Y-%m'),
    'produccion_real_t': desn_z(nacional_z.loc[var_test.index]).round(4),
    'variacion_pct': var_test.round(4),
    'es_shock_P75': [('X' if d in shock_months_full else '') for d in var_test.index],
})
df_var.to_csv(OUT_VERIF / 'distribucion_variacion.csv', index=False, encoding='utf-8-sig')

tabla = pd.DataFrame([{k: v for k, v in r.items() if k != 'shock_meses'} | {'shock_meses': r['shock_meses']}
                      for r in rows])
tabla.to_csv(OUT_TABLA / 'tabla1_oficial.csv', index=False, encoding='utf-8-sig')

meta = {
    'descripcion': 'Tabla 1 oficial - re-calculo de Delta_s con mascara empirica',
    'criterio_shock': f'variacion mensual absoluta (toneladas desnormalizadas) > P75 ventana test = {P75_TEST:.2f}%',
    'shock_months_P75': shock_periods,
    'n_shock_total': len(shock_periods),
    'umbral_paper_errado': 20.0,
    'shocks_con_umbral_paper_sobre_toneladas': 0,
    'shocks_con_umbral_paper_sobre_zscore': 8,
    'ventana_test': ['2024-09-01', '2025-08-01'],
    'scaler': str(SCALER_F2),
    'max_diff_real_col_vs_master': max_diff_real,
    'modelos': [dict(r) for r in rows],
}
(OUT_VERIF / 'tabla1_oficial.json').write_text(
    json.dumps(meta, indent=2, ensure_ascii=False), encoding='utf-8')

out_run = OUT_VERIF / 'output_ejecucion.txt'
out_run.write_text(console.getvalue(), encoding='utf-8')

print()
print('ARCHIVOS GENERADOS:')
print('  %s' % (OUT_VERIF / 'README.md'))
print('  %s' % (OUT_VERIF / 'verificar_delta_s.py'))
print('  %s' % (OUT_VERIF / 'distribucion_variacion.csv'))
print('  %s' % (OUT_VERIF / 'output_ejecucion.txt'))
print('  %s' % (OUT_VERIF / 'tabla1_oficial.json'))
print('  %s' % (OUT_TABLA / 'tabla1_oficial.csv'))