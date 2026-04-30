import os, sys, subprocess

def execute(path):
    print(f"\nEjecutando: {path}")
    r = subprocess.run([sys.executable,'-m','jupyter','nbconvert','--to','notebook',
        '--execute','--inplace','--ExecutePreprocessor.kernel_name=python3', path],
        capture_output=True)
    ok = r.returncode == 0
    print(f"  {'OK' if ok else 'ERROR'}")
    if not ok:
        stderr = (r.stderr or b'').decode('utf-8', errors='replace')
        safe_stderr = stderr.encode('ascii', 'ignore').decode('ascii')
        print('\n'.join(safe_stderr.strip().split('\n')[-15:]))
    return ok

nbs = [
    "notebooks/actividad_01_configuracion.ipynb",
    "notebooks/actividad_02_lectura_datos.ipynb"
]

results = {nb: execute(nb) for nb in nbs}

print("\n" + "="*55)
for nb, ok in results.items():
    print(f"  {os.path.basename(nb):<25}: {'OK' if ok else 'Error'}")
