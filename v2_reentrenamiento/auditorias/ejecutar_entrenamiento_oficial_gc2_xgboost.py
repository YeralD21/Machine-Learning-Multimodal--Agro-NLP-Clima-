"""Execute the 20 official GC2/XGBoost v2 training runs."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / "venv" / "Scripts" / "python.exe"
MANIFEST_PATH = REPO_ROOT / "v2_reentrenamiento/auditorias/entrenamiento_oficial_gc2_xgboost_manifest.json"

CULTIVARS = ("sutil", "dulce")
SEEDS = tuple(range(10))


def run_one(cultivar: str, seed: int) -> dict:
    code = (
        "import json\n"
        "from pathlib import Path\n"
        "from v2_reentrenamiento.src.training.official_runner_gc2_xgboost import run_official_training\n"
        f"result = run_official_training(Path(r'{REPO_ROOT}'), '{cultivar}', {seed})\n"
        "print(json.dumps(result, sort_keys=True))\n"
    )
    start = time.perf_counter()
    result = subprocess.run(
        [str(PYTHON), "-c", code],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - start
    run_dir = (
        REPO_ROOT
        / "v2_reentrenamiento/resultados_v2_final/official_gc2_xgboost"
        / cultivar
        / "GC2_XGBoost"
        / f"seed_{seed:02d}"
    )
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (logs_dir / "subprocess_stdout.log").write_text(result.stdout, encoding="utf-8")
    (logs_dir / "subprocess_stderr.log").write_text(result.stderr, encoding="utf-8")

    record = {
        "cultivar": cultivar,
        "model_name": "GC2_XGBoost",
        "seed": seed,
        "returncode": result.returncode,
        "subprocess_seconds": elapsed,
        "run_dir": str(run_dir),
        "stdout_log": str(logs_dir / "subprocess_stdout.log"),
        "stderr_log": str(logs_dir / "subprocess_stderr.log"),
        "status": "failed",
        "result": None,
        "error_tail": result.stderr[-4000:],
    }
    if result.returncode == 0:
        json_line = result.stdout.strip().splitlines()[-1]
        parsed = json.loads(json_line)
        record["status"] = parsed.get("status", "complete")
        record["result"] = parsed
        record["error_tail"] = ""
    return record


def main() -> None:
    started = datetime.now(timezone.utc).isoformat()
    started_perf = time.perf_counter()
    records = []
    for cultivar in CULTIVARS:
        for seed in SEEDS:
            print(f"[RUN] cultivar={cultivar} model=GC2_XGBoost seed={seed:02d}", flush=True)
            record = run_one(cultivar, seed)
            records.append(record)
            MANIFEST_PATH.write_text(
                json.dumps(
                    {
                        "started_utc": started,
                        "updated_utc": datetime.now(timezone.utc).isoformat(),
                        "records": records,
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            if record["status"] != "complete" or record["returncode"] != 0:
                print(json.dumps(record, indent=2, sort_keys=True), flush=True)
                raise SystemExit(1)
            metrics = record["result"]["metrics"]["validation"]
            print(
                "[OK] {cultivar}/GC2_XGBoost/seed_{seed:02d} val_mae={mae:.6f} val_rmse={rmse:.6f}".format(
                    cultivar=cultivar,
                    seed=seed,
                    mae=metrics["mae"],
                    rmse=metrics["rmse"],
                ),
                flush=True,
            )

    manifest = {
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": time.perf_counter() - started_perf,
        "records": records,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"manifest": str(MANIFEST_PATH), "runs": len(records)}, indent=2), flush=True)


if __name__ == "__main__":
    sys.path.insert(0, str(REPO_ROOT))
    main()
