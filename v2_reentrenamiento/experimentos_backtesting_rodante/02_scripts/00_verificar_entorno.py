"""Etapa 00: entorno, semillas, hashes congelados y manifest.json.

Uso:
  python 00_verificar_entorno.py --congelar   # solo la primera vez (Fase B): fija los hashes de referencia
  python 00_verificar_entorno.py              # verifica; aborta si algun archivo congelado cambio
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.entorno import preparar_entorno, semillas_globales  # noqa: E402

preparar_entorno()
semillas_globales(42)

import json  # noqa: E402
import os  # noqa: E402

from lib.integridad import HASHES_CONGELADOS, Integridad, escribir_manifest  # noqa: E402


def main() -> None:
    integ = Integridad()
    if "--congelar" in sys.argv:
        if HASHES_CONGELADOS.exists():
            raise SystemExit("hashes_congelados.json ya existe; no se sobrescribe (borrarlo exige aprobacion).")
        h = integ.congelar()
        estado = {"accion": "congelado", "n_archivos": {k: len(v) for k, v in h.items()}}
    else:
        estado = {"accion": "verificado", **integ.verificar()}
    faltan = [k for k, v in {"TF_ENABLE_ONEDNN_OPTS": "0", "TF_DETERMINISTIC_OPS": "1", "PYTHONHASHSEED": "42", "HF_HUB_OFFLINE": "1"}.items()
              if os.environ.get(k) != v]
    if faltan:
        raise SystemExit(f"Variables de entorno incorrectas: {faltan}")
    m = escribir_manifest({"integridad": estado})
    print(json.dumps({"integridad": estado, "variables_entorno": m["variables_entorno"], "librerias": m["librerias"]}, indent=2))


if __name__ == "__main__":
    main()
