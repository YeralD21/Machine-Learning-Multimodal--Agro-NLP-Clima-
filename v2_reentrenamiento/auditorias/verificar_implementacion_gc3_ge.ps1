Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")

$Lags = @(1, 3, 6)
$NASA = @("T2M", "T2M_MAX", "WS2M", "PRECTOTCORR", "RH2M")
$INDECI = @(
  "num_emergencias",
  "personas_afectadas",
  "personas_damnificadas",
  "hectareas_cultivo_perdidas",
  "hectareas_cultivo_afectadas"
)
$NLP = @(
  "avg_sentiment_lag1",
  "avg_sentiment_lag3",
  "avg_sentiment_lag6",
  "n_noticias_lag1",
  "n_noticias_lag3",
  "n_noticias_lag6"
)

function Expand-Lagged($names) {
  $out = @()
  foreach ($name in $names) {
    foreach ($lag in $Lags) {
      $out += "${name}_lag${lag}"
    }
  }
  return $out
}

function Get-FeatureSpec($cultivar, $model) {
  $ramaA = @(
    "produccion_t_${cultivar}",
    "produccion_t_${cultivar}_lag1",
    "produccion_t_${cultivar}_lag3",
    "produccion_t_${cultivar}_lag6"
  )
  $ramaB = @("mes_sin", "mes_cos", "t_index") + (Expand-Lagged $NASA) + (Expand-Lagged $INDECI)
  if ($model -eq "GE") {
    $ramaB = $ramaB + $NLP
  }
  return [PSCustomObject]@{
    RamaA = $ramaA
    RamaB = $ramaB
    All = $ramaA + $ramaB
    Target = "produccion_t_${cultivar}"
  }
}

function Assert($condition, $message) {
  if (-not $condition) {
    throw $message
  }
}

function Assert-FeatureSpec($spec, $model) {
  Assert ($spec.RamaA.Count -eq 4) "Rama A debe tener 4 features."
  if ($model -eq "GC3") {
    Assert ($spec.RamaB.Count -eq 33) "Rama B GC3 debe tener 33 features."
    Assert ($spec.All.Count -eq 37) "GC3 debe tener 37 inputs."
  } else {
    Assert ($spec.RamaB.Count -eq 39) "Rama B GE debe tener 39 features."
    Assert ($spec.All.Count -eq 43) "GE debe tener 43 inputs."
  }
  $forbidden = @("precio_chacra_kg", "n_provincias", "total_afectados")
  foreach ($feature in $spec.All) {
    foreach ($bad in $forbidden) {
      Assert (-not $feature.Contains($bad)) "Feature prohibida detectada: $feature"
    }
  }
}

function Get-ParamCount($featuresA, $featuresB) {
  $u = 16
  $att = 16
  $lstmA = 4 * $u * ($featuresA + $u + 1)
  $lstmB = 4 * $u * ($featuresB + $u + 1)
  $attention = (2 * $u * $att) + $att
  $dense16 = (2 * $u * 16) + 16
  $dense8 = (16 * 8) + 8
  $output = 8 + 1
  return $lstmA + $lstmB + $attention + $attention + $dense16 + $dense8 + $output
}

function Test-Cultivar($cultivar) {
  $path = Join-Path $Root "data/processed/master_dataset_${cultivar}_v2_features.csv"
  $rows = Import-Csv $path
  $columns = $rows[0].PSObject.Properties.Name
  $YearCol = $columns[0]
  $MonthCol = "mes"

  Assert ($rows.Count -eq 114) "${cultivar}: se esperaban 114 filas features."
  Assert (-not ($rows | Where-Object { [int]$_.$YearCol -eq 2026 })) "${cultivar}: 2026 no debe existir."

  foreach ($model in @("GC3", "GE")) {
    $spec = Get-FeatureSpec $cultivar $model
    Assert-FeatureSpec $spec $model
    foreach ($feature in $spec.All + @($spec.Target)) {
      Assert ($columns -contains $feature) "${cultivar}/${model}: falta columna $feature"
    }
  }

  $trainRows = @($rows | Where-Object { [int]$_.$YearCol -le 2023 })
  $valRows = @($rows | Where-Object { [int]$_.$YearCol -eq 2024 })
  $testRows = @($rows | Where-Object { [int]$_.$YearCol -eq 2025 })
  Assert ($trainRows.Count -eq 90) "${cultivar}: TRAIN features debe tener 90 filas."
  Assert ($valRows.Count -eq 12) "${cultivar}: VAL debe tener 12 filas."
  Assert ($testRows.Count -eq 12) "${cultivar}: TEST debe tener 12 filas."

  $targetDates = @()
  for ($i = 6; $i -lt $rows.Count; $i++) {
    $targetDates += [datetime]::new([int]$rows[$i].$YearCol, [int]$rows[$i].$MonthCol, 1)
  }
  $trainTargets = @($targetDates | Where-Object { $_.Year -le 2023 })
  $valTargets = @($targetDates | Where-Object { $_.Year -eq 2024 })
  $testTargets = @($targetDates | Where-Object { $_.Year -eq 2025 })
  Assert ($trainTargets.Count -eq 84) "${cultivar}: TRAIN debe tener 84 secuencias."
  Assert ($valTargets.Count -eq 12) "${cultivar}: VAL debe tener 12 targets."
  Assert ($testTargets.Count -eq 12) "${cultivar}: TEST debe tener 12 targets."
  Assert ($trainTargets[0].ToString("yyyy-MM") -eq "2017-01") "${cultivar}: primer target TRAIN incorrecto."
  Assert ($trainTargets[-1].ToString("yyyy-MM") -eq "2023-12") "${cultivar}: ultimo target TRAIN incorrecto."
  Assert ($valTargets[0].ToString("yyyy-MM") -eq "2024-01") "${cultivar}: primer target VAL incorrecto."
  Assert ($testTargets[0].ToString("yyyy-MM") -eq "2025-01") "${cultivar}: primer target TEST incorrecto."

  $scalerParams = Import-Csv (Join-Path $Root "resultados_v2_final/scalers/scaler_${cultivar}_v2c_parametros.csv")
  Assert ($scalerParams.Count -eq 44) "${cultivar}: scaler vigente debe registrar 44 features escaladas."
}

Test-Cultivar "sutil"
Test-Cultivar "dulce"

$gc3Params = Get-ParamCount 4 33
$geParams = Get-ParamCount 4 39
Assert ($gc3Params -eq 6273) "GC3 parametros esperados 6273; obtenido $gc3Params"
Assert ($geParams -eq 6657) "GE parametros esperados 6657; obtenido $geParams"

Write-Output "OK_IMPLEMENTACION_GC3_GE"
Write-Output "GC3_PARAMS=$gc3Params"
Write-Output "GE_PARAMS=$geParams"
Write-Output "TRAIN_SEQUENCES=84"
Write-Output "VAL_TARGETS=12"
Write-Output "TEST_TARGETS=12"
