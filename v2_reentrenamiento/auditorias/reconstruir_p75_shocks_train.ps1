$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$V2Root = Join-Path $RepoRoot "v2_reentrenamiento"

$Cultivares = @(
    @{
        Nombre = "Sutil"
        Archivo = Join-Path $V2Root "data\processed\master_dataset_sutil_v2.csv"
        Target = "produccion_t_sutil"
        P75HistoricoPct = 23.2
        ShocksHistoricos = @("2025-01", "2025-07", "2025-11")
    },
    @{
        Nombre = "Dulce"
        Archivo = Join-Path $V2Root "data\processed\master_dataset_dulce_v2.csv"
        Target = "produccion_t_dulce"
        P75HistoricoPct = 33.9
        ShocksHistoricos = @("2025-01", "2025-02", "2025-03")
    }
)

function Format-Invariant([double]$Value, [string]$Pattern = "0.#################") {
    return $Value.ToString($Pattern, [System.Globalization.CultureInfo]::InvariantCulture)
}

function Get-QuantileType7 {
    param(
        [Parameter(Mandatory = $true)][double[]]$Values,
        [Parameter(Mandatory = $true)][double]$Probability
    )

    if ($Probability -lt 0 -or $Probability -gt 1) {
        throw "Probability must be between 0 and 1."
    }

    $Sorted = @($Values | Sort-Object)
    $N = $Sorted.Count
    if ($N -eq 0) {
        throw "Cannot compute quantile over an empty vector."
    }
    if ($N -eq 1) {
        return [double]$Sorted[0]
    }

    # Hyndman-Fan type 7, equivalent to pandas/numpy default linear quantile:
    # zero-based position h = (N - 1) * p, then linear interpolation.
    $H = ($N - 1) * $Probability
    $Lower = [int][math]::Floor($H)
    $Upper = [int][math]::Ceiling($H)
    $Fraction = $H - $Lower

    if ($Lower -eq $Upper) {
        return [double]$Sorted[$Lower]
    }

    return [double]$Sorted[$Lower] + $Fraction * ([double]$Sorted[$Upper] - [double]$Sorted[$Lower])
}

function Import-Serie {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Target
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Source file not found: $Path"
    }

    $Rows = Import-Csv -LiteralPath $Path
    $Parsed = foreach ($Row in $Rows) {
        $Properties = @($Row.PSObject.Properties.Name)
        $YearColumn = $Properties[0]
        $MonthColumn = $Properties[1]
        $Year = [int]$Row.$YearColumn
        $Month = [int]$Row.$MonthColumn
        $RawTarget = $Row.$Target
        if ([string]::IsNullOrWhiteSpace($RawTarget)) {
            $Y = [double]::NaN
        } else {
            $Y = [double]::Parse($RawTarget, [System.Globalization.CultureInfo]::InvariantCulture)
        }

        [pscustomobject]@{
            Fecha = [datetime]::new($Year, $Month, 1)
            FechaYM = "{0:D4}-{1:D2}" -f $Year, $Month
            Y = $Y
        }
    }

    return @($Parsed | Sort-Object Fecha)
}

function Assert-RegularMonthlyWindow {
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][datetime]$Start,
        [Parameter(Mandatory = $true)][datetime]$End,
        [Parameter(Mandatory = $true)][string]$TargetName,
        [Parameter(Mandatory = $true)][int]$ExpectedCount
    )

    $Window = @($Rows | Where-Object { $_.Fecha -ge $Start -and $_.Fecha -le $End } | Sort-Object Fecha)
    if ($Window.Count -ne $ExpectedCount) {
        throw "${TargetName}: expected $ExpectedCount rows, found $($Window.Count)."
    }

    $Duplicates = @($Window | Group-Object FechaYM | Where-Object { $_.Count -gt 1 })
    if ($Duplicates.Count -gt 0) {
        throw "${TargetName}: duplicate months found: $($Duplicates.Name -join ', ')."
    }

    $ExpectedMonths = New-Object System.Collections.Generic.List[string]
    $Cursor = $Start
    while ($Cursor -le $End) {
        $ExpectedMonths.Add($Cursor.ToString("yyyy-MM"))
        $Cursor = $Cursor.AddMonths(1)
    }

    $Observed = @($Window | ForEach-Object { $_.FechaYM })
    $Missing = @($ExpectedMonths | Where-Object { $Observed -notcontains $_ })
    if ($Missing.Count -gt 0) {
        throw "${TargetName}: missing months found: $($Missing -join ', ')."
    }

    $NanTargets = @($Window | Where-Object { [double]::IsNaN($_.Y) })
    if ($NanTargets.Count -gt 0) {
        throw "${TargetName}: NaN target values found."
    }

    for ($I = 1; $I -lt $Window.Count; $I++) {
        if ($Window[$I].Fecha -ne $Window[$I - 1].Fecha.AddMonths(1)) {
            throw "${TargetName}: non-monthly sequence at $($Window[$I].FechaYM)."
        }
    }

    return $Window
}

function Get-RelativeChanges {
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][datetime]$Start,
        [Parameter(Mandatory = $true)][datetime]$End,
        [Parameter(Mandatory = $true)][string]$TargetName
    )

    $Changes = @()
    $Sorted = @($Rows | Sort-Object Fecha)

    for ($I = 1; $I -lt $Sorted.Count; $I++) {
        $Current = $Sorted[$I]
        if ($Current.Fecha -lt $Start -or $Current.Fecha -gt $End) {
            continue
        }

        $Previous = $Sorted[$I - 1]
        if ([double]::IsNaN($Previous.Y) -or [double]::IsNaN($Current.Y)) {
            throw "${TargetName}: NaN found when computing relative change for $($Current.FechaYM)."
        }
        if ($Previous.Y -eq 0.0) {
            throw "${TargetName}: previous production is zero for $($Current.FechaYM)."
        }

        $R = [math]::Abs(($Current.Y - $Previous.Y) / $Previous.Y)
        $Changes += [pscustomobject]@{
            Fecha = $Current.Fecha
            FechaYM = $Current.FechaYM
            Y = $Current.Y
            YAnterior = $Previous.Y
            R = $R
            RPct = 100.0 * $R
        }
    }

    return @($Changes)
}

$Results = New-Object System.Collections.Generic.List[object]
$TestTables = @{}

foreach ($Cultivar in $Cultivares) {
    $Serie = Import-Serie -Path $Cultivar.Archivo -Target $Cultivar.Target
    $Train = Assert-RegularMonthlyWindow `
        -Rows $Serie `
        -Start ([datetime]"2016-01-01") `
        -End ([datetime]"2023-12-01") `
        -TargetName $Cultivar.Nombre `
        -ExpectedCount 96

    $TrainChanges = Get-RelativeChanges `
        -Rows $Train `
        -Start ([datetime]"2016-02-01") `
        -End ([datetime]"2023-12-01") `
        -TargetName $Cultivar.Nombre

    if ($TrainChanges.Count -ne 95) {
        throw "$($Cultivar.Nombre): expected 95 TRAIN relative changes, found $($TrainChanges.Count)."
    }

    $P75 = Get-QuantileType7 -Values ([double[]]@($TrainChanges | ForEach-Object { $_.R })) -Probability 0.75
    $P75Pct = 100.0 * $P75

    $TestChanges = Get-RelativeChanges `
        -Rows $Serie `
        -Start ([datetime]"2025-01-01") `
        -End ([datetime]"2025-12-01") `
        -TargetName $Cultivar.Nombre

    if ($TestChanges.Count -ne 12) {
        throw "$($Cultivar.Nombre): expected 12 test changes, found $($TestChanges.Count)."
    }

    $RoundedOneDecimalPct = [math]::Round($P75Pct, 1, [System.MidpointRounding]::AwayFromZero)
    $RoundedOneDecimalProportion = $RoundedOneDecimalPct / 100.0
    $Rows2025 = foreach ($Change in $TestChanges) {
        $ShockExact = $Change.R -gt $P75
        $ShockRounded = $Change.R -gt $RoundedOneDecimalProportion
        [pscustomobject]@{
            Cultivar = $Cultivar.Nombre
            Fecha = $Change.FechaYM
            RExacto = $Change.R
            RPct = $Change.RPct
            P75Exacto = $P75
            P75Pct = $P75Pct
            Distancia = $Change.R - $P75
            Shock = $ShockExact
            ShockConP75Pct1Decimal = $ShockRounded
            CambiaPorRedondeo = ($ShockExact -ne $ShockRounded)
        }
    }

    $ShocksTrainOnly = @($Rows2025 | Where-Object { $_.Shock } | ForEach-Object { $_.Fecha })
    $Historicos = @($Cultivar.ShocksHistoricos)
    $Permanecen = @($ShocksTrainOnly | Where-Object { $Historicos -contains $_ })
    $Salen = @($Historicos | Where-Object { $ShocksTrainOnly -notcontains $_ })
    $Nuevos = @($ShocksTrainOnly | Where-Object { $Historicos -notcontains $_ })

    $Results.Add([pscustomobject]@{
        Cultivar = $Cultivar.Nombre
        P75HistoricoPct = [double]$Cultivar.P75HistoricoPct
        P75TrainOnlyExacto = $P75
        P75TrainOnlyPct = $P75Pct
        DiferenciaPP = $P75Pct - [double]$Cultivar.P75HistoricoPct
        DiferenciaRelativa = ($P75Pct - [double]$Cultivar.P75HistoricoPct) / [double]$Cultivar.P75HistoricoPct
        NTrain = $Train.Count
        NCambiosTrain = $TrainChanges.Count
        ShocksHistoricos = ($Historicos -join ", ")
        ShocksTrainOnly = ($ShocksTrainOnly -join ", ")
        Permanecen = ($Permanecen -join ", ")
        Salen = if ($Salen.Count -eq 0) { "ninguno" } else { $Salen -join ", " }
        Nuevos = if ($Nuevos.Count -eq 0) { "ninguno" } else { $Nuevos -join ", " }
        NShockHistorico = $Historicos.Count
        NShockTrainOnly = $ShocksTrainOnly.Count
        CoincideExactamenteConHistorico = ([math]::Abs($P75Pct - [double]$Cultivar.P75HistoricoPct) -eq 0.0)
        CoincideRedondeado1DecimalPct = ([math]::Round($P75Pct, 1, [System.MidpointRounding]::AwayFromZero) -eq [double]$Cultivar.P75HistoricoPct)
        P75Pct1DecimalPresentacion = $RoundedOneDecimalPct
        CambiosPorRedondeoEnTest = @($Rows2025 | Where-Object { $_.CambiaPorRedondeo }).Count
    })

    $TestTables[$Cultivar.Nombre] = @($Rows2025)
}

Write-Host ""
Write-Host "P75 TRAIN-only reconstruido"
$Results |
    Select-Object `
        Cultivar,
        @{Name = "P75 historico"; Expression = { "{0:N1}%" -f $_.P75HistoricoPct }},
        @{Name = "P75 TRAIN-only exacto"; Expression = { Format-Invariant $_.P75TrainOnlyExacto }},
        @{Name = "P75 TRAIN-only %"; Expression = { "{0}%" -f (Format-Invariant $_.P75TrainOnlyPct) }},
        @{Name = "Diferencia pp"; Expression = { Format-Invariant $_.DiferenciaPP }},
        @{Name = "n_shock historico"; Expression = { $_.NShockHistorico }},
        @{Name = "n_shock TRAIN-only"; Expression = { $_.NShockTrainOnly }} |
    Format-Table -AutoSize

foreach ($Result in $Results) {
    Write-Host ""
    Write-Host "$($Result.Cultivar): shocks TRAIN-only = $($Result.ShocksTrainOnly)"
}

foreach ($Cultivar in $Cultivares) {
    Write-Host ""
    Write-Host "Aplicacion congelada a test 2025 - $($Cultivar.Nombre)"
    $TestTables[$Cultivar.Nombre] |
        Select-Object `
            Fecha,
            @{Name = "r_t exacto"; Expression = { Format-Invariant $_.RExacto }},
            @{Name = "r_t %"; Expression = { Format-Invariant $_.RPct }},
            @{Name = "P75 exacto"; Expression = { Format-Invariant $_.P75Exacto }},
            @{Name = "distancia"; Expression = { Format-Invariant $_.Distancia }},
            Shock,
            CambiaPorRedondeo |
        Format-Table -AutoSize
}

Write-Host ""
Write-Host '"P75 fue reconstruido exclusivamente con TRAIN 2016-2023."'
Write-Host '"Validation 2024 y test 2025 NO participaron en la estimacion del umbral."'
Write-Host '"El umbral computacional utiliza precision completa y no el porcentaje redondeado."'
Write-Host '"NO recalcule metricas de modelos."'
Write-Host '"NO entrene modelos."'
Write-Host '"D35-b permanece pendiente de aprobacion externa."'
