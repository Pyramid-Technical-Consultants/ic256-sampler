# Build script for IC256 Sampler executable
# This script uses PyInstaller to create a standalone Windows executable

param(
    [string]$Version = "",
    [switch]$Clean = $false,
    [switch]$SkipValidation = $false
)

$ErrorActionPreference = "Stop"

# Get script directory (scripts/)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$SpecFile = Join-Path $ScriptDir "ic256_sampler.spec"
$DistDir = Join-Path $ProjectRoot "dist"
$BuildDir = Join-Path $ProjectRoot "build"
$PyProjectFile = Join-Path $ProjectRoot "pyproject.toml"
$IconFile = Join-Path $ScriptDir "logo.ico"
$AssetsDir = Join-Path $ProjectRoot (Join-Path "ic256_sampler" (Join-Path "assets" "images"))

# Function to get version from pyproject.toml
function Get-VersionFromPyProject {
    if (Test-Path $PyProjectFile) {
        $content = Get-Content $PyProjectFile -Raw
        if ($content -match 'version\s*=\s*"([^"]+)"') {
            return $matches[1]
        }
    }
    return "1.1.0"  # Fallback if pyproject.toml not found
}

# Get version
if ([string]::IsNullOrEmpty($Version)) {
    $Version = Get-VersionFromPyProject
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  IC256 Sampler Build Script" -ForegroundColor Cyan
Write-Host "  Version: $Version" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project Root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

# Validation checks
if (-not $SkipValidation) {
    Write-Host "Running pre-build validation..." -ForegroundColor Yellow
    
    # Check Python
    try {
        $pythonVersion = python --version 2>&1
        Write-Host "  Python: $pythonVersion" -ForegroundColor Green
    } catch {
        Write-Host "  ERROR: Python not found in PATH" -ForegroundColor Red
        exit 1
    }
    
    # Check PyInstaller
    try {
        $pyinstallerVersion = pyinstaller --version 2>&1
        Write-Host "  PyInstaller: $pyinstallerVersion" -ForegroundColor Green
    } catch {
        Write-Host "  PyInstaller not found. Installing..." -ForegroundColor Yellow
        pip install pyinstaller
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  ERROR: Failed to install PyInstaller" -ForegroundColor Red
            exit 1
        }
    }
    
    # Check spec file
    if (-not (Test-Path $SpecFile)) {
        Write-Host "  ERROR: Spec file not found: $SpecFile" -ForegroundColor Red
        Write-Host "  Please ensure ic256_sampler.spec exists in the scripts directory" -ForegroundColor Red
        exit 1
    }
    Write-Host "  Spec file: Found" -ForegroundColor Green
    
    # Check icon file
    if (-not (Test-Path $IconFile)) {
        Write-Host "  WARNING: Icon file not found: $IconFile" -ForegroundColor Yellow
    } else {
        Write-Host "  Icon file: Found" -ForegroundColor Green
    }
    
    # Check assets directory
    if (-not (Test-Path $AssetsDir)) {
        Write-Host "  WARNING: Assets directory not found: $AssetsDir" -ForegroundColor Yellow
    } else {
        $imageCount = (Get-ChildItem $AssetsDir -File).Count
        Write-Host "  Assets: Found ($imageCount images)" -ForegroundColor Green
    }
    
    # Check entry point
    $EntryPoint = Join-Path $ProjectRoot "run.py"
    if (-not (Test-Path $EntryPoint)) {
        Write-Host "  ERROR: Entry point not found: $EntryPoint" -ForegroundColor Red
        exit 1
    }
    Write-Host "  Entry point: Found" -ForegroundColor Green
    
    # Display version that will be used
    Write-Host "  Version: $Version (from pyproject.toml)" -ForegroundColor Green
    
    Write-Host "Validation complete!" -ForegroundColor Green
    Write-Host ""
}

# Clean previous builds if requested
if ($Clean) {
    Write-Host "Cleaning previous builds..." -ForegroundColor Yellow
    if (Test-Path $DistDir) {
        Remove-Item -Recurse -Force $DistDir
        Write-Host "  Removed: $DistDir" -ForegroundColor Gray
    }
    if (Test-Path $BuildDir) {
        Remove-Item -Recurse -Force $BuildDir
        Write-Host "  Removed: $BuildDir" -ForegroundColor Gray
    }
    Write-Host ""
}

# Build using spec file
Write-Host "Building executable with PyInstaller..." -ForegroundColor Green
Write-Host "  Spec file: $SpecFile" -ForegroundColor Gray
Write-Host "  Output: $DistDir" -ForegroundColor Gray
Write-Host ""

$buildStartTime = Get-Date
pyinstaller --clean --noconfirm $SpecFile
$buildExitCode = $LASTEXITCODE
$buildDuration = (Get-Date) - $buildStartTime

Write-Host ""

if ($buildExitCode -eq 0) {
    $BaseExePath = Join-Path $DistDir "ic256-sampler.exe"
    $VersionedExeName = "ic256-sampler-$Version.exe"
    $VersionedExePath = Join-Path $DistDir $VersionedExeName
    
    if (Test-Path $BaseExePath) {
        # Rename executable to include version
        Rename-Item -Path $BaseExePath -NewName $VersionedExeName -Force
        Write-Host "  Renamed executable to include version" -ForegroundColor Gray
        
        $exeInfo = Get-Item $VersionedExePath
        $exeSizeMB = [math]::Round($exeInfo.Length / 1MB, 2)
        $exeSizeKB = [math]::Round($exeInfo.Length / 1KB, 0)
        
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "  Build Successful!" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "Executable: $VersionedExePath" -ForegroundColor Cyan
        Write-Host "Size: $exeSizeMB MB ($exeSizeKB KB)" -ForegroundColor Cyan
        Write-Host "Build time: $($buildDuration.TotalSeconds.ToString('F1')) seconds" -ForegroundColor Cyan
        Write-Host ""
        # Generate version file for Inno Setup
        $VersionFile = Join-Path $ScriptDir "version.iss"
        "#define AppVersion `"$Version`"" | Out-File -FilePath $VersionFile -Encoding ASCII
        Write-Host "  Generated: $VersionFile" -ForegroundColor Gray
        
        Write-Host "Next steps:" -ForegroundColor Yellow
        Write-Host "  1. Test the executable: & '$VersionedExePath'" -ForegroundColor Gray
        Write-Host "  2. Build installer: iscc scripts\build_for_ic256.iss" -ForegroundColor Gray
    } else {
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "  Build Error!" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "Build completed but executable not found at:" -ForegroundColor Red
        Write-Host "  $BaseExePath" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Check PyInstaller output above for errors." -ForegroundColor Yellow
        exit 1
    }
} else {
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "  Build Failed!" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "PyInstaller exited with code: $buildExitCode" -ForegroundColor Red
    Write-Host ""
    Write-Host "Common issues:" -ForegroundColor Yellow
    Write-Host "  - Missing dependencies: pip install -r requirements.txt" -ForegroundColor Gray
    Write-Host "  - Missing hidden imports: check ic256_sampler.spec" -ForegroundColor Gray
    Write-Host "  - Path issues: ensure all paths in spec file are correct" -ForegroundColor Gray
    exit 1
}
