$base = "C:\Users\JSchell\OneDrive - NWSchell\Documents\GitHub\OT-NLP\data\translations"

# SQLite scrollmapper files
foreach ($db in @('t_kjv', 't_ylt', 't_web')) {
    $out = Join-Path $base "$db.db"
    if (Test-Path $out) {
        Write-Host "SKIP $db (already exists)"
        continue
    }
    $url = "https://github.com/scrollmapper/bible_databases/raw/master/sqlite/$db.db"
    Write-Host "Downloading $db..."
    Invoke-WebRequest -Uri $url -OutFile $out -UseBasicParsing
    Write-Host "Done $db ($([int](Get-Item $out).Length) bytes)"
}

# ULT
$ult = Join-Path $base "ult"
if (-not (Test-Path $ult)) {
    Write-Host "Cloning ULT..."
    git clone --depth 1 https://git.door43.org/unfoldingWord/en_ult $ult
    Write-Host "Done ULT"
} else { Write-Host "SKIP ULT (already exists)" }

# UST
$ust = Join-Path $base "ust"
if (-not (Test-Path $ust)) {
    Write-Host "Cloning UST..."
    git clone --depth 1 https://git.door43.org/unfoldingWord/en_ust $ust
    Write-Host "Done UST"
} else { Write-Host "SKIP UST (already exists)" }

Write-Host "All downloads complete."
