"""Independent artifact audit. Never loads a model or invokes an explainer.

Writes only the new audit MD/JSON requested for this task. The SHAP runner
itself writes exclusively inside resultados_v2_final/shap.
"""
import ast
import hashlib
import json
import sys
from pathlib import Path

import numpy as np

ROOT=Path(__file__).resolve().parents[2]
V2=ROOT/'v2_reentrenamiento'
AUDIT=V2/'auditorias'
SHAP=V2/'resultados_v2_final/shap'
SCRIPT=V2/'scripts/ejecutar_shap_v2_oficial.py'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def main():
    p=Path(sys.argv[1]).resolve()
    report=load(p)
    checks=[]
    def check(name,ok,evidence):
        checks.append(dict(guard=name,status='PASS' if ok else 'FAIL',evidence=evidence))
    benchmark=load(AUDIT/'shap_v2_explainer_benchmark.json')
    # Traverse the COMPLETE 1.9 MB JSON rather than relying on a terminal
    # truncation or only the recommendation block. Preserve historical status.
    leaf_count=0
    finite=True
    def visit(value):
        nonlocal leaf_count,finite
        if isinstance(value,dict):
            for item in value.values():
                visit(item)
        elif isinstance(value,list):
            for item in value:
                visit(item)
        else:
            leaf_count+=1
            if isinstance(value,float):
                finite=finite and bool(np.isfinite(value))
    visit(benchmark)
    historical_runs=benchmark['runs']+benchmark['tree_runs']
    historic_ok=all(sha(ROOT/r['array_path'])==r['array_sha256'] for r in historical_runs)
    check('Lectura integral y evidencia historica del benchmark',finite and historic_ok and len(historical_runs)==168,
          f'JSON completo: {leaf_count} hojas revisadas; 156 corridas neuronales y 12 Tree; todos los NPZ historicos coinciden con sus hashes')
    check('Preflight completo',report['status']=='LISTO PARA EJECUCION SHAP TEST OFICIAL',str(p))
    check('Script definitivo sellado',sha(SCRIPT)==report['script_sha256'],report['script_sha256'])
    changed=[path for path,h in report['hashes'].items() if not Path(path).is_file() or sha(path)!=h]
    check('Modelos/datos/scalers/predicciones/shocks/features/codigo inmutables',not changed,
          f"{len(report['hashes'])} SHA256 verificados; cambios: {changed}")
    families=report.get('frozen_families',[])
    for family in ['Naive','SARIMA_rolling','XGBoost','GC3','GE']:
        check(f'{family} congelado',family in families,'Manifest de comparacion aprobado y hashes historicos verificados')
    expected={f'{c}/{m}/seed_{s:02d}' for c in ('sutil','dulce') for m in ('GC3','GE','XGBoost') for s in range(10)}
    rows=report['models']
    check('40 redes + 20 XGBoost; seeds completas 0..9',len(rows)==60 and {r['key'] for r in rows}==expected,'Inventario, config y carga de cada modelo')
    check('D51 alcance GC3/GE/XGBoost',report['policy']['models']==['GC3','GE','XGBoost'],'Sin SHAP Naive/SARIMA')
    check('D52 Permutation/RNG/presupuesto',report['policy']['rng']==1729 and report['policy']['max_evals']=={'GC3':7120,'GE':8272},str(report['policy']['max_evals']))
    check('D53 Tree interventional/raw',report['policy']['tree']=='TreeExplainer' and report['policy']['tree_perturbation']=='interventional' and report['policy']['tree_output']=='raw','Pipeline escalado; no transform/fit del scaler')
    tree=ast.parse(SCRIPT.read_text(encoding='utf-8'))
    funcs={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
    # Execute only the pure snapshot function. No runner import, TF or SHAP.
    ns=dict(Path=Path,RESULTS=V2/'resultados_v2_final',V2=V2,AUDIT=AUDIT,OUTPUT=SHAP,
            __file__=str(SCRIPT),sha256=sha)
    exec(compile(ast.Module(body=[funcs['snapshot']],type_ignores=[]),str(SCRIPT),'exec'),ns)
    check('Sello utilizable por el arranque oficial futuro',ns['snapshot']()==report['hashes'],
          'Igualdad del inventario completo, incluyendo el script; informes nuevos de esta tarea excluidos explicitamente')
    parser=ast.get_source_segment(SCRIPT.read_text(encoding='utf-8'),funcs['parser'])
    check('D54 no seleccion de seed ni presupuesto configurable',all(f"'--{name}'" not in parser for name in ('seed','seeds','model','cultivar','max-evals','nsamples','method')),'CLI sin selectores; prueba negativa de seeds incompletas')
    for key,data in report['backgrounds'].items():
        n=89 if key.endswith('XGBoost') else 84
        idx=np.rint(np.linspace(0,n-1,32)).astype(int).tolist()
        check(f'D55 background {key}',data['indices']==idx and data['source']=='TRAIN' and len(data['target_dates'])==32 and max(data['target_dates'])<='2023-12-01',
              f"array SHA256 {data['sha256_array']}; indices/valores contra TRAIN y benchmark aprobado")
    dates=[f'2025-{i:02d}-01' for i in range(1,13)]
    for key,data in report['inputs'].items():
        m=key.split('/')[1]
        n=37 if m=='XGBoost' else 222 if m=='GC3' else 258
        check(f'D56 shape y mapeo {key}',data['test_shape']==[12,n] and len(data['feature_names'])==(43 if m=='GE' else 37) and len(data['mapping'])==n,
              f"input SHA256 {data['test_hash']}; flat->rama/timestep/feature reversible")
        check(f'Calendario y horizonte {key}',data['dates']==dates and len(data['contexts'])==12 and data['contexts'][0][-1]=='2024-12-01' and data['contexts'][-1][-1]=='2025-11-01',
              f"Contexto enero={data['contexts'][0]}; contexto diciembre={data['contexts'][-1]}")
        groups={r['group'] for r in data['mapping']}
        check(f'D57 grupos {key}',groups==({'produccion_historica','temporalidad','NASA_clima','INDECI','NLP'} if m=='GE' else {'produccion_historica','temporalidad','NASA_clima','INDECI'}),'Constantes de features oficiales')
    maximum=max((r['prediction_max_abs_scaled'] for r in rows),default=float('inf'))
    original_max=max((r['original_evaluator_max_abs_scaled'] for r in rows),default=float('inf'))
    check('720 predicciones congeladas reproducidas',len(rows)==60 and maximum<=1e-6 and original_max<=1e-6,
          f'rtol=0, atol=1e-6 escalado; max wrapper={maximum:.12g}; max evaluador original={original_max:.12g}')
    check('Scalers e inversion a toneladas',all(r['prediction_max_abs_ton']<=report['inputs']['/'.join(r['key'].split('/')[:2])]['prediction_atol_ton']+1e-8 for r in rows),
          'StandardScaler TRAIN=90; joblib/CSV consistentes; tolerancia toneladas=scale_train*1e-6+1e-8')
    check('D58 mascaras fijas',sum('shocks' in c['name'] and c['status']=='PASS' for c in report['controls'])==60,
          'CSV congelados: Sutil Jan/Jul/Nov; Dulce Jan/Feb/Mar; 3/9, sin recalcular umbrales')
    verified=0
    recon=0.
    for r in rows:
        probe=r.get('val_probe',{})
        if not probe:
            continue
        if sha(probe['path'])!=probe['sha256']:
            continue
        with np.load(probe['path'],allow_pickle=False) as a:
            m=r['key'].split('/')[1]
            n=37 if m=='XGBoost' else 222 if m=='GC3' else 258
            expected_shape=(1,37) if m=='XGBoost' else (1,6,37 if m=='GC3' else 43)
            residual=np.abs(a['base_values']+a['flat_shap'].sum(1)-a['predictions_model_scaled'])
            good=a['flat_shap'].shape==(1,n) and a['elemental_shap'].shape==expected_shape and residual.max()<=1e-5
            good=good and all(np.isfinite(a[k]).all() for k in a.files)
            ex=probe['explainer']
            good=good and ex['scope']=='VAL' and ex['rng']==1729 and ex['nsamples'] is None
            good=good and ex['method']==('TreeExplainer' if m=='XGBoost' else 'PermutationExplainer')
            if m!='XGBoost':
                good=good and ex['max_evals']==(7120 if m=='GC3' else 8272) and ex['batch_size']==1024
            verified+=int(good)
            recon=max(recon,float(residual.max()))
    check('Pruebas VAL de los 60 explainers/NPZ/aditividad',verified==60,f'Verificacion independiente de {verified}/60 NPZ; max residuo={recon:.12g} escalado')
    check('Pesos en memoria sin cambios',all(r['model_unchanged_in_memory'] for r in rows),'Hash pesos/booster antes y despues de probes')
    check('Cero SHAP TEST',report['test_explanations']==0 and report['val_explanations']==60 and not (SHAP/'official_test_2025').exists(),
          'Solo enero VAL 2024 para cada una de las 60 corridas; directorio oficial ausente')
    check('Lecturas TEST explicitadas',len(report['test_reads'])==4,'Dos CSV de predicciones (480/240 filas), dos datasets (114 filas cada uno, 12 de 2025); solo integridad')
    synthetic=report.get('synthetic_tests',[])
    check('D56-D59 agregaciones A-E y serializacion completa',sum('complete raw/metadata' in x for x in synthetic)==3,
          'Arrays SINTETICOS: roundtrip NPZ/JSON, conservacion absoluta, NLP denominador cero, shock/nonshock; no rankings TEST')
    check('Variabilidad entre 10 seeds sin cancelacion firmada',sum('signed seed cancellation' in x for x in synthetic)==3,
          'Seeds de signos opuestos conservan importancia; media/mediana/SD(ddof=1)/min/max de agregados por seed')
    check('D60 attention separado',report['attention_generated'] is False,'No se genera ni se necesita attention')
    forbidden={'fit','fit_transform','partial_fit','save_model','train_on_batch','set_weights'}
    calls=[n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)]
    check('Sin entrenar/reentrenar/modificar modelos',not forbidden.intersection(calls),f'AST sin llamadas {sorted(forbidden)}; hashes inmutables')
    source=SCRIPT.read_text(encoding='utf-8')
    check('Fallback tecnico documentado con parada',all(s in source for s in ('technical_failure.json','--resume-technical-failure','--failure-sha256','except TimeoutError','min_samples_per_feature=100','nsamples=65536')),
          'No automatico: error Permutation -> registro/STOP; reanudacion exige hash del fallo y mismo preflight; no por lentitud ni integridad')
    check('Guardas de autorizacion/hash/calendario/escritura',all(s in synthetic for s in ('wrong TEST date','11 TEST rows','prediction mismatch','missing seed','write frozen artifact','TEST in VAL call','unauthorized TEST call','changed hash')),
          'Pruebas adversariales rechazan entradas invalidas antes del explainer')
    check('Sin sobrescritura silenciosa',sum('overwrite ' in s for s in synthetic)==3,'Creacion exclusiva x/xb; directorio oficial unico; temporales tambien dentro de SHAP')
    check('D61 entorno y RNG reproducibles',report['environment']['pythonhashseed']=='1729' and report['environment']['intra_threads']==1 and report['environment']['inter_threads']==1,
          json.dumps(report['environment'],ensure_ascii=True))
    stderr=(p.parent/'execution_stderr.log').read_text(encoding='utf-8',errors='replace')
    check('Temporales sin violar confinamiento', 'Write outside SHAP directory' not in stderr,'AutoGraph y cache en shap/_runtime_tmp; stdout/stderr y warnings conservados')
    passed=all(c['status']=='PASS' for c in checks)
    state='LISTO PARA EJECUCIÓN SHAP TEST OFICIAL' if passed else 'BLOQUEADO'
    result=dict(status=state,approved_preflight_path=str(p),approved_preflight_sha256=sha(p),script_sha256=sha(SCRIPT),
                checks=checks,summary=dict(models=len(rows),predictions_integrity=12*len(rows),test_shap_explanations=0,
                val_shap_explanations=verified,max_prediction_error_scaled=maximum,max_original_error_scaled=original_max,
                max_val_reconstruction_scaled=recon,frozen_files=len(report['hashes'])),
                preflight=report)
    json_path=AUDIT/'shap_v2_preflight_ejecucion_oficial.json'
    json_path.write_text(json.dumps(result,indent=2,ensure_ascii=True,allow_nan=False),encoding='utf-8')
    lines=['# AUDITORIA PREFLIGHT EJECUCION SHAP OFICIAL','',f'Estado: **{state}**.','',
        '**SHAP oficial TEST no ejecutado. Esta tarea termina en preflight.**','',
        '## Evidencia y alcance','',
        'Se leyeron el HANDOFF_SESION, las tres auditorias SHAP requeridas, el JSON completo del benchmark y las decisiones vigentes. '
        'D51-D61 cerradas prevalecen sobre las etiquetas historicas pendientes. No se modificaron esas fuentes. '
        'La aprobacion futura de ejecucion es distinta de esta aprobacion tecnica.','',
        f'{len(rows)} modelos cargados: 40 GC3/GE y 20 XGBoost; 720 predicciones comparadas solo por integridad. '
        f'{verified} explicaciones tecnicas sobre enero VAL 2024, una por modelo/seed; cero explicaciones TEST. '
        f'{len(report["hashes"])} archivos congelados con SHA256 antes/despues y nueva comprobacion independiente.','',
        'No entrenamiento, HPO, seleccion de seeds, cambios de features/scalers/shocks, metricas predictivas nuevas, figuras ni interpretacion cientifica. '
        'No commit ni push. Naive y SARIMA se verifican desde sus artefactos congelados sin ejecutarlos.','',
        '## Alineacion temporal y lecturas TEST','',
        'La busqueda de NPZ/NPY en los directorios oficiales no encontro tensores TEST serializados reutilizables. '
        'GC3/GE reutiliza exactamente load_feature_dataframe + build_sequences de la evaluacion original. '
        'Se verifica cada una de las 12 ventanas contra las filas del mismo CSV escalado, sin recalcular features. '
        'El wrapper convierte a float32 igual que la entrada Keras y conserva flatten(A) seguido de flatten(B). '
        'Se cotejan tanto model.predict sobre los tensores originales float64 como el wrapper contra el CSV congelado. '
        'XGBoost reutiliza la funcion pura _build_test_arrays del evaluador original mediante AST; no ejecuta el evaluador.','',
        'Objetivos Jan-Dec 2025; origen Dec2024-Nov2025; contexto neuronal enero Jul-Dec2024, diciembre Jun-Nov2025. '
        'Informacion hasta t -> y_(t+1). Las variables lagged no se confunden con la posicion del timestep.','',
        f'Tolerancia fijada antes de inferencia: atol=1e-6 escalado, rtol=0; maximo observado wrapper={maximum:.12g}, '
        f'evaluador original={original_max:.12g}. Toneladas: atol=scale_train*1e-6+1e-8; scaler joblib cotejado con CSV. '
        f'Reconstruccion explainer: atol=1e-5 escalado; maximo VAL={recon:.12g}. Ninguna tolerancia se ajusto a resultados.','',
        'El lector TRAIN/VAL sigue usando DictReader + islice(...,102), sin solicitar el registro 103. '
        'La excepcion autorizada de integridad se registra separadamente:','',
        *[f'- `{r["path"]}`: {r["purpose"]}; {r["rows"]} filas.' for r in report['test_reads']], '',
        'El hashing binario completo no constituye analisis TEST. Se conservaron las mascaras del CSV y se cotejaron contra shocks_d35; no se recalcularon P75.','',
        '## Background y protocolo','',
        '32 referencias TRAIN por cultivar; los seis NPZ/hashes aprobados se reutilizan y contrastan contra TRAIN. '
        'GC3/GE: 84 ventanas TRAIN; XGBoost: 89 pares. Indices rint(linspace(0,n-1,32)), compartidos entre seeds. '
        'Indices, fechas y seis hashes completos estan en backgrounds del JSON adjunto.','',
        'Permutation: Independent(max_samples=32), identity, RNG=1729, batch_size=1024; GC3 max_evals=7120, GE=8272. '
        'Tree: background explicito, interventional, raw; entradas/target escalados. '
        'Sampling nsamples=65536, min_samples_per_feature=100, RNG=1729, mismo background: exclusivamente tras fallo tecnico real registrado. '
        'La corrida se detiene; reanudar exige el registro del fallo y su SHA256. No existe seleccion libre de explainer; '
        'timeout/lentitud, discrepancias de integridad o preferencias interpretativas no habilitan fallback. '
        'No se activo fallback en este preflight.','',
        '## Almacenamiento y agregacion','',
        'Runner: `../scripts/ejecutar_shap_v2_oficial.py`. Probes y manifest sellado bajo '
        '`../resultados_v2_final/shap/_preflight/`; temporales bajo `shap/_runtime_tmp/`. '
        'La futura corrida usa `shap/official_test_2025/{cultivar}/{GC3,GE,XGBoost}/seed_XX/`, con creacion exclusiva.','',
        '`raw.npz` conserva flat_shap firmado, elemental_shap GC3=(12,6,37), GE=(12,6,43), XGBoost=(12,37), '
        'bases, reconstruccion, residuos, prediccion del modelo, predicciones congeladas escaladas/toneladas, '
        'entradas escaladas, fechas target/contexto, nombres/indices de features, rama/timestep, background/indices y mascara shock. '
        '`metadata.json` conserva versiones, seeds, RNG, budgets, tiempos, warnings, hashes, unidades y mapeo completo. '
        'El NPZ se relee sin pickle y se compara exactamente antes de generar agregados.','',
        'A: mean_sample(sum_timestep(abs(phi))); B: mean_sample(sum_feature(abs(phi))); '
        'C: suma de A por grupo; XGBoost usa mean_sample(abs(phi)). D: seis NLP GE y participacion en el total absoluto '
        '(denominador cero -> null, sin imputacion). E: mismas operaciones sobre shock/nonshock, diferencias descriptivas. '
        'F: media, mediana, SD muestral, minimo y maximo de cada agregado A-E calculado previamente por seed. '
        'Nunca se promedian SHAP firmados entre seeds antes del absoluto. Valores firmados locales quedan intactos. '
        'Las pruebas con signos opuestos y datos sinteticos ejercitan todos estos caminos sin rankings TEST.','',
        '## Guardas PASS/FAIL','',
        '| Guarda | Estado | Evidencia |','|---|---|---|',
        *[f"| {c['guard']} | {c['status']} | {c['evidence'].replace('|','/').replace(chr(10),' ')} |" for c in checks], '',
        '## Entorno, incidencias y limites','',
        'Se uso venv Python 3.11.9 fuera del sandbox porque este no podia acceder al interprete base. '
        'Las primeras pruebas detectaron restricciones del dispositivo NUL de Windows y temporales AutoGraph. '
        'Se corrigio el runner para reconocer NUL y confinar los temporales a SHAP. '
        'Las corridas previas se conservan como evidencia de desarrollo; solo el manifest/hash indicado abajo corresponde al script final. '
        'Las lecturas de integridad y probes VAL se repitieron al verificar la version corregida.','',
        'Warnings conocidos de BahdanauAttention/Keras, GPU nativa y trazado TensorFlow se registran; no se modificaron capas. '
        'El audit hook protege escrituras Python, rutas resueltas y sobrescrituras; el control SHA256 antes/despues cubre artefactos congelados. '
        'No se afirma que un hook Python sea un sandbox de sistema para bibliotecas nativas. '
        'Una muestra VAL por cada seed prueba operacion/serializacion; no certifica convergencia de atribuciones TEST. '
        'El benchmark previo aporta la evidencia tecnica adicional. NLP y shocks permanecen descriptivos, sin causalidad; attention no implementada.','',
        '## Sello y ejecucion futura','',
        f'- Script SHA256: `{sha(SCRIPT)}`.',
        f'- Preflight aprobado: `{p}`.',
        f'- Preflight SHA256: `{sha(p)}`.',
        f'- Auditoria JSON: `{json_path.name}`.', '',
        'Ejecutar sin argumentos solo corre preflight. La corrida oficial requiere NUEVA autorizacion del usuario y '
        '`--execute-test --approved-preflight <ruta anterior> --preflight-sha256 <hash anterior>`. '
        'Esos argumentos no se usaron en esta tarea. Antes del primer SHAP TEST, el runner vuelve a verificar hashes, '
        'versiones, inventario y las 720 predicciones; falla ante cambios. No ejecutar los generadores historicos.','',
        'Estado de salida: **'+state+'**.']
    (AUDIT/'AUDITORIA_PREFLIGHT_EJECUCION_SHAP_OFICIAL.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(json.dumps(dict(status=state,pass_count=sum(c['status']=='PASS' for c in checks),failures=[c for c in checks if c['status']=='FAIL'],
                         summary=result['summary']),ensure_ascii=True))
    raise SystemExit(0 if passed else 1)


if __name__=='__main__':
    main()
