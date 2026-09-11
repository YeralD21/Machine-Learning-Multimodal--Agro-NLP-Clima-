Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$V2Root = Split-Path -Parent $ScriptDir
$Root = Split-Path -Parent $V2Root
$OutDir = Join-Path $V2Root "resultados_v2_final/auditoria_d35"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$Cultivos = @(
    @{
        Key = "sutil"
        Nombre = "Limon Sutil"
        Fuente = "v2_reentrenamiento/data/processed/master_dataset_sutil_v2.csv"
        Target = "produccion_t_sutil"
    },
    @{
        Key = "dulce"
        Nombre = "Limon Dulce"
        Fuente = "v2_reentrenamiento/data/processed/master_dataset_dulce_v2.csv"
        Target = "produccion_t_dulce"
    }
)

function ToDouble($x) {
    return [double]::Parse([string]$x, [System.Globalization.CultureInfo]::InvariantCulture)
}

function Mean($xs) {
    if ($xs.Count -eq 0) { return [double]::NaN }
    $s = 0.0
    foreach ($x in $xs) { $s += [double]$x }
    return $s / $xs.Count
}

function Median($xs) {
    if ($xs.Count -eq 0) { return [double]::NaN }
    $a = @($xs | Sort-Object)
    $n = $a.Count
    if ($n % 2 -eq 1) { return [double]$a[[int](($n - 1) / 2)] }
    return ([double]$a[$n / 2 - 1] + [double]$a[$n / 2]) / 2.0
}

function StdSample($xs) {
    if ($xs.Count -lt 2) { return [double]::NaN }
    $m = Mean $xs
    $ss = 0.0
    foreach ($x in $xs) { $ss += ([double]$x - $m) * ([double]$x - $m) }
    return [math]::Sqrt($ss / ($xs.Count - 1))
}

function Metrics($actual, $pred) {
    if ($actual.Count -ne $pred.Count) { throw "actual/pred length mismatch" }
    $n = $actual.Count
    $sae = 0.0
    $sse = 0.0
    for ($i = 0; $i -lt $n; $i++) {
        $e = [double]$actual[$i] - [double]$pred[$i]
        $sae += [math]::Abs($e)
        $sse += $e * $e
    }
    return @{
        N = $n
        MAE = $sae / $n
        MSE = $sse / $n
        RMSE = [math]::Sqrt($sse / $n)
    }
}

function Acf($xs, $maxLag) {
    $n = $xs.Count
    $m = Mean $xs
    $den = 0.0
    foreach ($x in $xs) { $den += ([double]$x - $m) * ([double]$x - $m) }
    $out = @()
    for ($lag = 0; $lag -le $maxLag; $lag++) {
        $num = 0.0
        for ($t = $lag; $t -lt $n; $t++) {
            $num += ([double]$xs[$t] - $m) * ([double]$xs[$t - $lag] - $m)
        }
        $out += [pscustomobject]@{ Lag = $lag; ACF = if ($den -eq 0) { [double]::NaN } else { $num / $den } }
    }
    return $out
}

function PacfDurbinLevinson($acf, $maxLag) {
    $pacf = @([pscustomobject]@{ Lag = 0; PACF = 1.0 })
    $phiPrev = @{}
    $sigmaPrev = 1.0
    for ($k = 1; $k -le $maxLag; $k++) {
        $sum = 0.0
        for ($j = 1; $j -le ($k - 1); $j++) {
            $sum += $phiPrev[$j] * $acf[$k - $j].ACF
        }
        $phiKK = ($acf[$k].ACF - $sum) / $sigmaPrev
        $phiNew = @{}
        for ($j = 1; $j -le ($k - 1); $j++) {
            $phiNew[$j] = $phiPrev[$j] - $phiKK * $phiPrev[$k - $j]
        }
        $phiNew[$k] = $phiKK
        $sigmaPrev = $sigmaPrev * (1.0 - $phiKK * $phiKK)
        if ($sigmaPrev -le 1e-12) { $sigmaPrev = 1e-12 }
        $phiPrev = $phiNew
        $pacf += [pscustomobject]@{ Lag = $k; PACF = $phiKK }
    }
    return $pacf
}

function LinTrend($ys) {
    $n = $ys.Count
    $xs = 0..($n - 1)
    $mx = Mean $xs
    $my = Mean $ys
    $num = 0.0
    $den = 0.0
    for ($i = 0; $i -lt $n; $i++) {
        $dx = [double]$xs[$i] - $mx
        $num += $dx * ([double]$ys[$i] - $my)
        $den += $dx * $dx
    }
    $slope = $num / $den
    $intercept = $my - $slope * $mx
    return @{ Intercept = $intercept; SlopePerMonth = $slope; SlopePerYear = $slope * 12.0 }
}

function Fmt($x, [int]$d = 4) {
    if ([double]::IsNaN([double]$x)) { return "NA" }
    return ([double]$x).ToString("N$d", [System.Globalization.CultureInfo]::InvariantCulture)
}

function WriteAcfSvg($acf, $path, $title, $n) {
    $w = 860; $h = 420; $ml = 60; $mr = 25; $mt = 45; $mb = 55
    $plotW = $w - $ml - $mr; $plotH = $h - $mt - $mb
    $zeroY = $mt + $plotH / 2
    $maxLag = ($acf | Measure-Object Lag -Maximum).Maximum
    $barW = [math]::Max(4, ($plotW / ($maxLag + 1)) * 0.55)
    $conf = 1.96 / [math]::Sqrt($n)
    $confY1 = $zeroY - ($conf * ($plotH / 2))
    $confY2 = $zeroY + ($conf * ($plotH / 2))
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine("<svg xmlns='http://www.w3.org/2000/svg' width='$w' height='$h' viewBox='0 0 $w $h'>")
    [void]$sb.AppendLine("<rect width='100%' height='100%' fill='white'/>")
    [void]$sb.AppendLine("<text x='$($w/2)' y='24' text-anchor='middle' font-family='Arial' font-size='18' font-weight='700'>$title</text>")
    [void]$sb.AppendLine("<line x1='$ml' y1='$zeroY' x2='$($w-$mr)' y2='$zeroY' stroke='#333' stroke-width='1'/>")
    [void]$sb.AppendLine("<line x1='$ml' y1='$confY1' x2='$($w-$mr)' y2='$confY1' stroke='#999' stroke-dasharray='5,5'/>")
    [void]$sb.AppendLine("<line x1='$ml' y1='$confY2' x2='$($w-$mr)' y2='$confY2' stroke='#999' stroke-dasharray='5,5'/>")
    [void]$sb.AppendLine("<text x='$($w-$mr)' y='$($confY1-5)' text-anchor='end' font-family='Arial' font-size='11' fill='#666'>+/-1.96/sqrt(N)</text>")
    foreach ($r in $acf) {
        $x = $ml + ($r.Lag / $maxLag) * $plotW
        $y = $zeroY - ($r.ACF * ($plotH / 2))
        $top = [math]::Min($zeroY, $y)
        $bh = [math]::Abs($zeroY - $y)
        $color = if ($r.ACF -ge 0) { "#2f6fbb" } else { "#b94a48" }
        [void]$sb.AppendLine("<rect x='$($x-$barW/2)' y='$top' width='$barW' height='$bh' fill='$color'/>")
    }
    foreach ($tick in 0,6,12,18,24) {
        $x = $ml + ($tick / $maxLag) * $plotW
        [void]$sb.AppendLine("<line x1='$x' y1='$zeroY' x2='$x' y2='$($zeroY+5)' stroke='#333'/>")
        [void]$sb.AppendLine("<text x='$x' y='$($h-22)' text-anchor='middle' font-family='Arial' font-size='12'>$tick</text>")
    }
    [void]$sb.AppendLine("<text x='$($w/2)' y='$($h-6)' text-anchor='middle' font-family='Arial' font-size='12'>Lag mensual</text>")
    [void]$sb.AppendLine("<text x='18' y='$($h/2)' transform='rotate(-90 18,$($h/2))' text-anchor='middle' font-family='Arial' font-size='12'>ACF</text>")
    [void]$sb.AppendLine("</svg>")
    Set-Content -Path $path -Value $sb.ToString() -Encoding UTF8
}

function WriteMonthlySvg($monthlyRows, $path, $title) {
    $w = 900; $h = 430; $ml = 70; $mr = 30; $mt = 45; $mb = 70
    $plotW = $w - $ml - $mr; $plotH = $h - $mt - $mb
    $means = @($monthlyRows | ForEach-Object { [double]$_.Media })
    $yMin = ($means | Measure-Object -Minimum).Minimum
    $yMax = ($means | Measure-Object -Maximum).Maximum
    $pad = ($yMax - $yMin) * 0.12
    if ($pad -eq 0) { $pad = 1 }
    $yMin -= $pad; $yMax += $pad
    $months = @("Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic")
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine("<svg xmlns='http://www.w3.org/2000/svg' width='$w' height='$h' viewBox='0 0 $w $h'>")
    [void]$sb.AppendLine("<rect width='100%' height='100%' fill='white'/>")
    [void]$sb.AppendLine("<text x='$($w/2)' y='24' text-anchor='middle' font-family='Arial' font-size='18' font-weight='700'>$title</text>")
    [void]$sb.AppendLine("<line x1='$ml' y1='$mt' x2='$ml' y2='$($h-$mb)' stroke='#333'/>")
    [void]$sb.AppendLine("<line x1='$ml' y1='$($h-$mb)' x2='$($w-$mr)' y2='$($h-$mb)' stroke='#333'/>")
    $points = @()
    for ($i = 0; $i -lt 12; $i++) {
        $x = $ml + ($i + 0.5) * ($plotW / 12.0)
        $y = $mt + (($yMax - $means[$i]) / ($yMax - $yMin)) * $plotH
        $points += "$x,$y"
        [void]$sb.AppendLine("<circle cx='$x' cy='$y' r='4' fill='#2f6fbb'/>")
        [void]$sb.AppendLine("<text x='$x' y='$($h-42)' text-anchor='middle' font-family='Arial' font-size='12'>$($months[$i])</text>")
    }
    [void]$sb.AppendLine("<polyline points='$($points -join " ")' fill='none' stroke='#2f6fbb' stroke-width='2'/>")
    foreach ($frac in 0,0.25,0.5,0.75,1) {
        $y = $mt + $frac * $plotH
        $val = $yMax - $frac * ($yMax - $yMin)
        [void]$sb.AppendLine("<line x1='$ml' y1='$y' x2='$($w-$mr)' y2='$y' stroke='#eee'/>")
        [void]$sb.AppendLine("<text x='$($ml-8)' y='$($y+4)' text-anchor='end' font-family='Arial' font-size='11'>$(Fmt $val 0)</text>")
    }
    [void]$sb.AppendLine("<text x='$($w/2)' y='$($h-8)' text-anchor='middle' font-family='Arial' font-size='12'>Mes calendario</text>")
    [void]$sb.AppendLine("<text x='20' y='$($h/2)' transform='rotate(-90 20,$($h/2))' text-anchor='middle' font-family='Arial' font-size='12'>Produccion media TRAIN (t)</text>")
    [void]$sb.AppendLine("</svg>")
    Set-Content -Path $path -Value $sb.ToString() -Encoding UTF8
}

$Results = @{}
$AllMd = New-Object System.Collections.Generic.List[string]

foreach ($c in $Cultivos) {
    $path = Join-Path $Root $c.Fuente
    $rowsAll = Import-Csv $path
    $YearCol = @($rowsAll[0].PSObject.Properties.Name)[0]
    $MonthCol = @($rowsAll[0].PSObject.Properties.Name)[1]
    $rows = @($rowsAll | Where-Object {
        $yy = [int]$_.PSObject.Properties[$YearCol].Value
        $mm = [int]$_.PSObject.Properties[$MonthCol].Value
        ($yy -gt 2016 -or ($yy -eq 2016 -and $mm -ge 1)) -and
        ($yy -lt 2023 -or ($yy -eq 2023 -and $mm -le 12))
    } | Sort-Object @{Expression={[int]$_.PSObject.Properties[$YearCol].Value}}, @{Expression={[int]$_.PSObject.Properties[$MonthCol].Value}})

    if ($rows.Count -ne 96) { throw "No se reconstruyeron 96 observaciones TRAIN para $($c.Key); N=$($rows.Count)" }

    $dates = @()
    $ys = @()
    foreach ($r in $rows) {
        $dates += [datetime]::new([int]$r.PSObject.Properties[$YearCol].Value, [int]$r.PSObject.Properties[$MonthCol].Value, 1)
        $ys += ToDouble $r.($c.Target)
    }

    $expected = @()
    for ($i = 0; $i -lt 96; $i++) { $expected += ([datetime]::new(2016,1,1)).AddMonths($i) }
    $missing = @()
    for ($i = 0; $i -lt $expected.Count; $i++) {
        if ($dates[$i] -ne $expected[$i]) { $missing += $expected[$i].ToString("yyyy-MM") }
    }
    if ($missing.Count -gt 0) { throw "Faltan meses o frecuencia irregular para $($c.Key): $($missing -join ', ')" }

    $dateKeys = @{}
    $dup = 0
    foreach ($d in $dates) {
        $k = $d.ToString("yyyy-MM")
        if ($dateKeys.ContainsKey($k)) { $dup++ } else { $dateKeys[$k] = $true }
    }
    $nan = 0
    foreach ($y in $ys) { if ([double]::IsNaN($y)) { $nan++ } }

    $trend = LinTrend $ys
    $desc = @{
        Media = Mean $ys
        Mediana = Median $ys
        Std = StdSample $ys
        Min = ($ys | Measure-Object -Minimum).Minimum
        Max = ($ys | Measure-Object -Maximum).Maximum
        CV = (StdSample $ys) / (Mean $ys)
        SlopeMonth = $trend.SlopePerMonth
        SlopeYear = $trend.SlopePerYear
    }

    $acf = Acf $ys 24
    $pacf = PacfDurbinLevinson $acf 24
    $topAcf = @($acf | Where-Object { $_.Lag -gt 0 } | Sort-Object @{Expression={[math]::Abs($_.ACF)}} -Descending | Select-Object -First 8)

    $actual1 = @(); $pred1 = @()
    for ($i = 1; $i -lt $ys.Count; $i++) { $actual1 += $ys[$i]; $pred1 += $ys[$i-1] }
    $m1 = Metrics $actual1 $pred1

    $actual12 = @(); $pred12 = @()
    for ($i = 12; $i -lt $ys.Count; $i++) { $actual12 += $ys[$i]; $pred12 += $ys[$i-12] }
    $m12 = Metrics $actual12 $pred12

    $actualCommon = @(); $pred1Common = @(); $pred12Common = @()
    for ($i = 12; $i -lt $ys.Count; $i++) {
        $actualCommon += $ys[$i]
        $pred1Common += $ys[$i-1]
        $pred12Common += $ys[$i-12]
    }
    $m1c = Metrics $actualCommon $pred1Common
    $m12c = Metrics $actualCommon $pred12Common

    $monthly = @()
    for ($m = 1; $m -le 12; $m++) {
        $vals = @()
        for ($i = 0; $i -lt $dates.Count; $i++) {
            if ($dates[$i].Month -eq $m) { $vals += $ys[$i] }
        }
        $monthly += [pscustomobject]@{
            Mes = $m
            N = $vals.Count
            Media = Mean $vals
            Mediana = Median $vals
            Std = StdSample $vals
        }
    }
    $monthMeans = @($monthly | ForEach-Object { $_.Media })
    $amp = ((($monthMeans | Measure-Object -Maximum).Maximum - ($monthMeans | Measure-Object -Minimum).Minimum) / $desc.Media) * 100.0

    $acfPath = Join-Path $OutDir "acf_$($c.Key).svg"
    $monthlyPath = Join-Path $OutDir "patron_mensual_$($c.Key).svg"
    WriteAcfSvg $acf $acfPath "ACF TRAIN $($c.Nombre)" $ys.Count
    WriteMonthlySvg $monthly $monthlyPath "Patron mensual TRAIN $($c.Nombre)"

    $Results[$c.Key] = @{
        Config = $c
        Rows = $rows
        Dates = $dates
        Y = $ys
        Nan = $nan
        Duplicados = $dup
        Desc = $desc
        Acf = $acf
        Pacf = $pacf
        TopAcf = $topAcf
        M1 = $m1
        M12 = $m12
        M1Common = $m1c
        M12Common = $m12c
        RatioMae = $m12c.MAE / $m1c.MAE
        RatioRmse = $m12c.RMSE / $m1c.RMSE
        Monthly = $monthly
        AmpPct = $amp
        AcfFig = "v2_reentrenamiento/resultados_v2_final/auditoria_d35/acf_$($c.Key).svg"
        MonthlyFig = "v2_reentrenamiento/resultados_v2_final/auditoria_d35/patron_mensual_$($c.Key).svg"
    }
}

function MarkdownDescTable($res) {
    $d = $res.Desc
    return @"
| Estadistico | Valor |
|---|---:|
| Media | $(Fmt $d.Media 4) |
| Mediana | $(Fmt $d.Mediana 4) |
| Desviacion estandar | $(Fmt $d.Std 4) |
| Minimo | $(Fmt $d.Min 4) |
| Maximo | $(Fmt $d.Max 4) |
| Coeficiente de variacion | $(Fmt $d.CV 4) |
| Tendencia lineal, pendiente t/mes | $(Fmt $d.SlopeMonth 4) |
| Tendencia lineal, pendiente t/anio | $(Fmt $d.SlopeYear 4) |
"@
}

function MarkdownAcfTable($res) {
    $lags = @(1,6,12,13,24)
    $lines = @("| Lag | ACF | PACF |", "|---:|---:|---:|")
    foreach ($lag in $lags) {
        $a = ($res.Acf | Where-Object { $_.Lag -eq $lag }).ACF
        $p = ($res.Pacf | Where-Object { $_.Lag -eq $lag }).PACF
        $lines += "| $lag | $(Fmt $a 4) | $(Fmt $p 4) |"
    }
    return ($lines -join [Environment]::NewLine)
}

function MarkdownTopAcf($res) {
    $lines = @("| Rank | Lag | ACF | Abs(ACF) |", "|---:|---:|---:|---:|")
    $i = 1
    foreach ($r in $res.TopAcf) {
        $lines += "| $i | $($r.Lag) | $(Fmt $r.ACF 4) | $(Fmt ([math]::Abs($r.ACF)) 4) |"
        $i++
    }
    return ($lines -join [Environment]::NewLine)
}

function MarkdownMonthly($res) {
    $names = @("Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre")
    $lines = @("| Mes | N | Media | Mediana | Desv. est. |", "|---|---:|---:|---:|---:|")
    foreach ($r in $res.Monthly) {
        $lines += "| $($names[$r.Mes-1]) | $($r.N) | $(Fmt $r.Media 4) | $(Fmt $r.Mediana 4) | $(Fmt $r.Std 4) |"
    }
    return ($lines -join [Environment]::NewLine)
}

function MarkdownBench($res) {
    return @"
| Benchmark | Periodo efectivo | N efectivo | MAE | RMSE | MSE |
|---|---|---:|---:|---:|---:|
| Naive t-1 | t=2..N | $($res.M1.N) | $(Fmt $res.M1.MAE 4) | $(Fmt $res.M1.RMSE 4) | $(Fmt $res.M1.MSE 4) |
| Seasonal Naive t-12 | t=13..N | $($res.M12.N) | $(Fmt $res.M12.MAE 4) | $(Fmt $res.M12.RMSE 4) | $(Fmt $res.M12.MSE 4) |
"@
}

function MarkdownCommon($res) {
    return @"
| Benchmark | Periodo comun | N efectivo | MAE | RMSE |
|---|---|---:|---:|---:|
| Naive t-1 | t=13..N | $($res.M1Common.N) | $(Fmt $res.M1Common.MAE 4) | $(Fmt $res.M1Common.RMSE 4) |
| Seasonal Naive t-12 | t=13..N | $($res.M12Common.N) | $(Fmt $res.M12Common.MAE 4) | $(Fmt $res.M12Common.RMSE 4) |

| Ratio | Valor | Lectura descriptiva |
|---|---:|---|
| MAE_snaive12_common / MAE_naive1_common | $(Fmt $res.RatioMae 4) | $(if ($res.RatioMae -lt 1) { "Seasonal Naive tuvo menor MAE TRAIN en el periodo comun." } else { "Naive t-1 tuvo menor MAE TRAIN en el periodo comun." }) |
| RMSE_snaive12_common / RMSE_naive1_common | $(Fmt $res.RatioRmse 4) | $(if ($res.RatioRmse -lt 1) { "Seasonal Naive tuvo menor RMSE TRAIN en el periodo comun." } else { "Naive t-1 tuvo menor RMSE TRAIN en el periodo comun." }) |
"@
}

function MarkdownDenoms($res) {
    return @"
| Denominador candidato | Formula en TRAIN | N efectivo | Valor |
|---|---|---:|---:|
| D_MASE_1 | mean(abs(y_t - y_t-1)), t=2..N | $($res.M1.N) | $(Fmt $res.M1.MAE 4) |
| D_MASE_12 | mean(abs(y_t - y_t-12)), t=13..N | $($res.M12.N) | $(Fmt $res.M12.MAE 4) |
| D_RMSSE_1 | mean((y_t - y_t-1)^2), t=2..N | $($res.M1.N) | $(Fmt $res.M1.MSE 4) |
| D_RMSSE_12 | mean((y_t - y_t-12)^2), t=13..N | $($res.M12.N) | $(Fmt $res.M12.MSE 4) |
"@
}

$sutil = $Results["sutil"]
$dulce = $Results["dulce"]
$md = @"
# AUDITORIA D35 - Estacionalidad TRAIN para MASE/RMSSE

## 1. Alcance

Esta auditoria empirica previa a D35 evalua, usando exclusivamente TRAIN, si las series mensuales de produccion de Limon Sutil y Limon Dulce muestran evidencia descriptiva relevante para comparar dos denominadores candidatos de errores escalados:

- m=1: benchmark Naive no estacional `yhat_t = y_(t-1)`.
- m=12: benchmark Seasonal Naive anual `yhat_t = y_(t-12)`.

No se entrenan modelos, no se selecciona un benchmark oficial y no se cierra D35. La decision metodologica final queda pendiente.

## 2. Datos utilizados

| Cultivar | Archivo fuente | Columna objetivo | Fecha inicial | Fecha final | N | NaN target | Duplicados fecha | Frecuencia mensual | Meses faltantes |
|---|---|---|---|---|---:|---:|---:|---|---|
| Sutil | $($sutil.Config["Fuente"]) | $($sutil.Config["Target"]) | $($sutil.Dates[0].ToString("yyyy-MM")) | $($sutil.Dates[-1].ToString("yyyy-MM")) | $($sutil.Y.Count) | $($sutil.Nan) | $($sutil.Duplicados) | regular MS | 0 |
| Dulce | $($dulce.Config["Fuente"]) | $($dulce.Config["Target"]) | $($dulce.Dates[0].ToString("yyyy-MM")) | $($dulce.Dates[-1].ToString("yyyy-MM")) | $($dulce.Y.Count) | $($dulce.Nan) | $($dulce.Duplicados) | regular MS | 0 |

Se reconstruyeron inequivocamente las 96 observaciones TRAIN originales para ambos cultivares: 2016-01 a 2023-12.

## 3. Verificacion anti-leakage

- Ninguna observacion posterior a 2023-12 participa en los calculos.
- No se usan estadisticas de validation 2024.
- No se usan estadisticas de test 2025.
- No se usa el scaler v2 para decidir estacionalidad.
- Todos los calculos se hacen sobre valores originales de produccion en toneladas.
- La auditoria se realiza sobre la serie objetivo TRAIN original de 96 meses, no sobre las 90 filas posteriores al drop de lags.

## 4. Estadisticos descriptivos

### Sutil

$(MarkdownDescTable $sutil)

### Dulce

$(MarkdownDescTable $dulce)

## 5. ACF/PACF

La ACF se calcula hasta lag 24 sobre TRAIN. La PACF se calcula mediante recursion Durbin-Levinson sobre la misma ACF, sin instalar dependencias nuevas.

### Sutil - lags solicitados

$(MarkdownAcfTable $sutil)

### Sutil - mayores autocorrelaciones absolutas

$(MarkdownTopAcf $sutil)

Figura: $($sutil.AcfFig)

### Dulce - lags solicitados

$(MarkdownAcfTable $dulce)

### Dulce - mayores autocorrelaciones absolutas

$(MarkdownTopAcf $dulce)

Figura: $($dulce.AcfFig)

## 6. Naive t-1

Benchmark in-sample: yhat_t = y_(t-1) para t=2..N.

| Cultivar | N efectivo | MAE_naive1_train | RMSE_naive1_train | MSE_naive1_train |
|---|---:|---:|---:|---:|
| Sutil | $($sutil.M1.N) | $(Fmt $sutil.M1.MAE 4) | $(Fmt $sutil.M1.RMSE 4) | $(Fmt $sutil.M1.MSE 4) |
| Dulce | $($dulce.M1.N) | $(Fmt $dulce.M1.MAE 4) | $(Fmt $dulce.M1.RMSE 4) | $(Fmt $dulce.M1.MSE 4) |

## 7. Seasonal Naive t-12

Benchmark in-sample: yhat_t = y_(t-12) para t=13..N.

| Cultivar | N efectivo | MAE_snaive12_train | RMSE_snaive12_train | MSE_snaive12_train |
|---|---:|---:|---:|---:|
| Sutil | $($sutil.M12.N) | $(Fmt $sutil.M12.MAE 4) | $(Fmt $sutil.M12.RMSE 4) | $(Fmt $sutil.M12.MSE 4) |
| Dulce | $($dulce.M12.N) | $(Fmt $dulce.M12.MAE 4) | $(Fmt $dulce.M12.RMSE 4) | $(Fmt $dulce.M12.MSE 4) |

## 8. Comparacion en periodo comun

Comparacion obligatoria sobre exactamente el mismo subconjunto temporal: t=13..N.

### Sutil

$(MarkdownCommon $sutil)

### Dulce

$(MarkdownCommon $dulce)

## 9. Denominadores candidatos MASE/RMSSE

No se calcula MASE/RMSSE de ningun modelo. Solo se reportan los denominadores candidatos en unidades originales.

### Sutil

$(MarkdownDenoms $sutil)

### Dulce

$(MarkdownDenoms $dulce)

## 10. Patron mensual

La amplitud estacional descriptiva se calcula como `(max(media_mensual) - min(media_mensual)) / media_global`.

| Cultivar | Amplitud estacional descriptiva |
|---|---:|
| Sutil | $(Fmt $sutil.AmpPct 2)% |
| Dulce | $(Fmt $dulce.AmpPct 2)% |

### Sutil

$(MarkdownMonthly $sutil)

Figura: $($sutil.MonthlyFig)

### Dulce

$(MarkdownMonthly $dulce)

Figura: $($dulce.MonthlyFig)

## 11. STL, si se pudo ejecutar

No se ejecuto STL. Motivo: el entorno Python/venv no esta funcional en esta sesion (`py` no encuentra Python instalado y `python` no esta en PATH), y no se instalaron ni modificaron dependencias. Para respetar la restriccion de no modificar entorno, se omite esta seccion opcional.

## 12. Resultados Sutil

- TRAIN reconstruido: 2016-01..2023-12, N=96, sin NaN, sin duplicados y sin meses faltantes.
- ACF lag 12: $(Fmt (($sutil.Acf | Where-Object { $_.Lag -eq 12 }).ACF) 4).
- ACF lag 24: $(Fmt (($sutil.Acf | Where-Object { $_.Lag -eq 24 }).ACF) 4).
- Naive t-1 completo: MAE=$(Fmt $sutil.M1.MAE 4), RMSE=$(Fmt $sutil.M1.RMSE 4), N=$($sutil.M1.N).
- Seasonal Naive t-12 completo: MAE=$(Fmt $sutil.M12.MAE 4), RMSE=$(Fmt $sutil.M12.RMSE 4), N=$($sutil.M12.N).
- En periodo comun t=13..N, ratio_MAE=$(Fmt $sutil.RatioMae 4) y ratio_RMSE=$(Fmt $sutil.RatioRmse 4).
- Amplitud mensual descriptiva: $(Fmt $sutil.AmpPct 2)%.

## 13. Resultados Dulce

- TRAIN reconstruido: 2016-01..2023-12, N=96, sin NaN, sin duplicados y sin meses faltantes.
- ACF lag 12: $(Fmt (($dulce.Acf | Where-Object { $_.Lag -eq 12 }).ACF) 4).
- ACF lag 24: $(Fmt (($dulce.Acf | Where-Object { $_.Lag -eq 24 }).ACF) 4).
- Naive t-1 completo: MAE=$(Fmt $dulce.M1.MAE 4), RMSE=$(Fmt $dulce.M1.RMSE 4), N=$($dulce.M1.N).
- Seasonal Naive t-12 completo: MAE=$(Fmt $dulce.M12.MAE 4), RMSE=$(Fmt $dulce.M12.RMSE 4), N=$($dulce.M12.N).
- En periodo comun t=13..N, ratio_MAE=$(Fmt $dulce.RatioMae 4) y ratio_RMSE=$(Fmt $dulce.RatioRmse 4).
- Amplitud mensual descriptiva: $(Fmt $dulce.AmpPct 2)%.

## 14. Interpretacion estrictamente descriptiva

- Sutil: en TRAIN comun t=13..N, el ratio MAE seasonal/naive es $(Fmt $sutil.RatioMae 4). Esto describe que $(if ($sutil.RatioMae -lt 1) { "Seasonal Naive obtuvo menor MAE TRAIN que Naive t-1 en ese periodo." } else { "Naive t-1 obtuvo menor MAE TRAIN que Seasonal Naive en ese periodo." })
- Sutil: en TRAIN comun t=13..N, el ratio RMSE seasonal/naive es $(Fmt $sutil.RatioRmse 4). Esto describe que $(if ($sutil.RatioRmse -lt 1) { "Seasonal Naive obtuvo menor RMSE TRAIN que Naive t-1 en ese periodo." } else { "Naive t-1 obtuvo menor RMSE TRAIN que Seasonal Naive en ese periodo." })
- Dulce: en TRAIN comun t=13..N, el ratio MAE seasonal/naive es $(Fmt $dulce.RatioMae 4). Esto describe que $(if ($dulce.RatioMae -lt 1) { "Seasonal Naive obtuvo menor MAE TRAIN que Naive t-1 en ese periodo." } else { "Naive t-1 obtuvo menor MAE TRAIN que Seasonal Naive en ese periodo." })
- Dulce: en TRAIN comun t=13..N, el ratio RMSE seasonal/naive es $(Fmt $dulce.RatioRmse 4). Esto describe que $(if ($dulce.RatioRmse -lt 1) { "Seasonal Naive obtuvo menor RMSE TRAIN que Naive t-1 en ese periodo." } else { "Naive t-1 obtuvo menor RMSE TRAIN que Seasonal Naive en ese periodo." })
- Estas observaciones no cierran D35 ni determinan automaticamente el denominador oficial.

## 15. Limitaciones

- La auditoria usa solo TRAIN, como corresponde para evitar leakage, pero por ello no evalua desempeno fuera de muestra.
- La ACF/PACF es diagnostico descriptivo, no prueba formal definitiva de estacionalidad.
- La comparacion de benchmarks in-sample no decide por si sola el denominador metodologico.
- No se ejecuto STL por falta de entorno Python funcional y por la restriccion de no modificar dependencias.
- No se probaron periodos alternativos; solo m=1 vs m=12.

## 16. Preguntas pendientes para D35

1. Si se reporta MASE, debe escalarse contra Naive t-1 o contra Seasonal Naive t-12?
2. Si se reporta RMSSE, debe compartir el mismo m que MASE o justificarse por separado?
3. El denominador sera unico por cultivar y calculado solo en TRAIN?
4. Como se explicara la tension entre benchmark oficial Baseline=Naive t-1 y posible escalado estacional m=12?
5. Se reportara RelMAE como metrica secundaria frente al baseline oficial?
6. Como se comunicara que esta auditoria aporta evidencia empirica TRAIN, pero no decide D35 automaticamente?
"@

$ReportPath = Join-Path $ScriptDir "AUDITORIA_D35_ESTACIONALIDAD_TRAIN.md"
Set-Content -Path $ReportPath -Value $md -Encoding UTF8

"Reporte generado: $ReportPath"
"Figuras generadas: $OutDir"
