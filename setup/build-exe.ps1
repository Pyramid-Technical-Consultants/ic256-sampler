# Build script for IC256 Sampler executable
# This script uses PyInstaller to create a standalone Windows executable

param(
    [string]$Version = "1.0.0",
    [switch]$Clean = $false
)

$ErrorActionPreference = "Stop"

# Get script directory (setup/)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$SpecFile = Join-Path $ScriptDir "ic256-sampler.spec"
$DistDir = Join-Path $ProjectRoot "dist"
$BuildDir = Join-Path $ProjectRoot "build"

Write-Host "Building IC256 Sampler v$Version" -ForegroundColor Green
Write-Host "Project Root: $ProjectRoot" -ForegroundColor Cyan

# Clean previous builds if requested
if ($Clean) {
    Write-Host "Cleaning previous builds..." -ForegroundColor Yellow
    if (Test-Path $DistDir) { Remove-Item -Recurse -Force $DistDir }
    if (Test-Path $BuildDir) { Remove-Item -Recurse -Force $BuildDir }
}

# Check if PyInstaller is installed
try {
    $null = Get-Command pyinstaller -ErrorAction Stop
} catch {
    Write-Host "PyInstaller not found. Installing..." -ForegroundColor Yellow
    pip install pyinstaller
}

# Check if spec file exists, create if not
if (-not (Test-Path $SpecFile)) {
    Write-Host "Creating PyInstaller spec file..." -ForegroundColor Yellow
    pyinstaller --name ic256-sampler `
        --onefile `
        --windowed `
        --icon "$(Join-Path $ScriptDir 'logo.ico')" `
        --add-data "$(Join-Path $ProjectRoot 'ic256_sampler' 'assets' 'images');ic256_sampler/assets/images" `
        --hidden-import=PIL `
        --hidden-import=PIL._imaging `
        --hidden-import=portalocker `
        --hidden-import=websocket `
        --distpath $DistDir `
        --workpath $BuildDir `
        --specpath $ScriptDir `
        "$(Join-Path $ProjectRoot 'run.py')" `
        --noconfirm
}

# Build using spec file
Write-Host "Building executable..." -ForegroundColor Green
pyinstaller --clean $SpecFile

if ($LASTEXITCODE -eq 0) {
    $ExePath = Join-Path $DistDir "ic256-sampler.exe"
    if (Test-Path $ExePath) {
        Write-Host "`nBuild successful!" -ForegroundColor Green
        Write-Host "Executable: $ExePath" -ForegroundColor Cyan
        Write-Host "Size: $([math]::Round((Get-Item $ExePath).Length / 1MB, 2)) MB" -ForegroundColor Cyan
    } else {
        Write-Host "Build completed but executable not found!" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}
