# Backtesting rodante (rolling-origin) — v2

Experimento de backtesting con orígenes anuales (diciembre del año previo) y test de 12 meses
(Y = 2018..2025) para comparar modelos en meses de shock y meses normales, con barrido de umbrales
y evidencia para un artículo MDPI (Forecasting).

**Estado:** Fase B completada (verificaciones). Fase C pendiente de aprobación.

- Plan y preregistro: `00_plan/`
- Reproducción: `venv\Scripts\python.exe v2_reentrenamiento\experimentos_backtesting_rodante\02_scripts\run_all.py --fase B` (las fases C y D se añadirán al aprobarse).
- Orden de ejecución: ver `00_plan/plan_fase_A.md` y `02_scripts/run_all.py`.
- Reportes por fase: `09_reportes/`.

Lee `v2_reentrenamiento/src` en solo lectura (SHA-256 verificado). No modifica nada fuera de esta carpeta.
Requiere `v4_benchmark_moderno/vendor/` (paquete Chronos) y los pesos de Chronos en la caché local de Hugging Face.
