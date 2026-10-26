"""Etapa 07: GC3 y GE. Cada corrida (ventana, cultivar, modelo, semilla) en un subproceso nuevo.

Una corrida:   python 07_lstm.py --anio 2025 --cultivar sutil --modelo GC3 --semilla 0 [--salida DIR]
Orquestador:   python 07_lstm.py --todas   (Fase C; requiere aprobacion)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.entorno import BR, preparar_entorno  # noqa: E402

preparar_entorno()

import argparse  # noqa: E402
import json  # noqa: E402
import subprocess  # noqa: E402


def una(args) -> None:
    from lib.integridad import Integridad
    from lib.modelo_lstm import entrenar_y_predecir

    Integridad().verificar()
    raiz = Path(args.salida) if args.salida else BR / "04_modelos"
    carpeta = {"GC3": "gc3_lstm_sin_nlp", "GE": "ge_lstm_con_nlp"}[args.modelo]
    destino = raiz / carpeta / args.cultivar / str(args.anio) / f"seed_{args.semilla:02d}"
    estado = entrenar_y_predecir(args.anio, args.cultivar, args.modelo, args.semilla, destino)
    print(json.dumps(estado, default=str))


def todas(args) -> None:
    from lib.ventanas import todas as ventanas

    for v in ventanas():
        for c in ("sutil", "dulce"):
            for m in ("GC3", "GE"):
                if not v.viable(m):
                    continue
                for s in range(10):
                    cmd = [sys.executable, __file__, "--anio", str(v.anio), "--cultivar", c, "--modelo", m, "--semilla", str(s)]
                    if args.salida:
                        cmd += ["--salida", args.salida]
                    r = subprocess.run(cmd)
                    if r.returncode != 0:
                        raise SystemExit(f"Fallo {v.anio}/{c}/{m}/seed {s}: se detiene la corrida (no se cambia el protocolo).")
            print(f"[ventana {v.anio}] GC3/GE completas", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--anio", type=int)
    ap.add_argument("--cultivar", choices=("sutil", "dulce"))
    ap.add_argument("--modelo", choices=("GC3", "GE"))
    ap.add_argument("--semilla", type=int)
    ap.add_argument("--salida")
    ap.add_argument("--todas", action="store_true")
    a = ap.parse_args()
    todas(a) if a.todas else una(a)
