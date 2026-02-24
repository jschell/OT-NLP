# check-env.ps1
# Assesses WSL + Docker + Claude Code readiness for OT-NLP pipeline.
# Run from PowerShell: .\check-env.ps1

$pass = 0; $fail = 0
function OK  ($msg) { Write-Host "  PASS  $msg" -ForegroundColor Green;  $script:pass++ }
function FAIL($msg) { Write-Host "  FAIL  $msg" -ForegroundColor Red;    $script:fail++ }
function HEAD($msg) { Write-Host "`n$msg" -ForegroundColor Cyan }

# ── WSL ───────────────────────────────────────────────────────────────────
HEAD "WSL"
$wslVersion = wsl --version 2>$null | Select-String "WSL version"
if ($wslVersion) { OK  "WSL installed: $($wslVersion -replace '.*: ','')" }
else             { FAIL "WSL not installed (run: wsl --install)" }

$distros = wsl --list --quiet 2>$null
if ($distros) { OK  "Distros found: $($distros -join ', ')" }
else          { FAIL "No WSL distros installed" }

$wsl2 = wsl --list --verbose 2>$null | Select-String "2\s*Running|2\s*Stopped"
if ($wsl2) { OK  "WSL 2 distro present" }
else       { FAIL "No WSL 2 distro — IDE sandboxing and Docker integration require WSL 2" }

# ── Docker ────────────────────────────────────────────────────────────────
HEAD "Docker"
$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCmd) { OK  "docker CLI found: $($dockerCmd.Source)" }
else            { FAIL "docker not found (install Docker Desktop)" }

$dockerInfo = docker info 2>$null
if ($LASTEXITCODE -eq 0) { OK  "Docker daemon running" }
else                     { FAIL "Docker daemon not running — start Docker Desktop" }

$composeVersion = docker compose version 2>$null
if ($LASTEXITCODE -eq 0) { OK  "docker compose: $composeVersion" }
else                     { FAIL "docker compose plugin missing" }

# ── Docker WSL integration ────────────────────────────────────────────────
HEAD "Docker WSL integration"
$wslDocker = wsl -- docker ps 2>$null
if ($LASTEXITCODE -eq 0) { OK  "docker accessible from WSL" }
else                     { FAIL "docker not accessible in WSL — enable WSL integration in Docker Desktop > Resources > WSL Integration" }

$wslCompose = wsl -- docker compose version 2>$null
if ($LASTEXITCODE -eq 0) { OK  "docker compose accessible from WSL" }
else                     { FAIL "docker compose not accessible in WSL" }

# ── Project location ──────────────────────────────────────────────────────
HEAD "Project filesystem"
$onLinux = wsl -- test -d "~/OT-NLP" 2>$null; $linuxExists = $LASTEXITCODE -eq 0
$onWin   = Test-Path "$env:USERPROFILE\OT-NLP"
if ($linuxExists) { OK  "Project found on Linux filesystem ~/OT-NLP (good)" }
elseif ($onWin)   { FAIL "Project is on Windows filesystem — move to Linux filesystem for best performance" }
else              { FAIL "Project not found at ~/OT-NLP in WSL or $env:USERPROFILE\OT-NLP on Windows" }

# ── Claude Code ───────────────────────────────────────────────────────────
HEAD "Claude Code"
$claudeVer = wsl -- claude --version 2>$null
if ($LASTEXITCODE -eq 0) { OK  "claude installed in WSL: $claudeVer" }
else                     { FAIL "claude not installed in WSL (run in WSL: curl -fsSL https://claude.ai/install.sh | bash)" }

# ── uv ────────────────────────────────────────────────────────────────────
HEAD "Python toolchain"
$uvVer = wsl -- uv --version 2>$null
if ($LASTEXITCODE -eq 0) { OK  "uv installed in WSL: $uvVer" }
else                     { FAIL "uv not installed in WSL (run in WSL: curl -LsSf https://astral.sh/uv/install.sh | sh)" }

# ── Summary ───────────────────────────────────────────────────────────────
HEAD "Summary"
$total = $pass + $fail
Write-Host "  $pass / $total checks passed"
if ($fail -gt 0) {
    Write-Host "  $fail issue(s) need attention (see FAIL lines above)" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "  Environment ready." -ForegroundColor Green
    exit 0
}
