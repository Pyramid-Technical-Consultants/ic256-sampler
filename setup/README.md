# Build Scripts for IC256 Sampler

This directory contains scripts and configuration files for building standalone executables and installers.

## Files

- **`build-exe.ps1`** - PowerShell script to build Windows executable using PyInstaller
- **`ic256-sampler.spec`** - PyInstaller specification file for reproducible builds
- **`BuildForIC256.iss`** - Inno Setup script for creating Windows installer
- **`logo.ico`** - Application icon used in builds

## Prerequisites

### For Building Executables

1. **PyInstaller**:
   ```powershell
   pip install pyinstaller
   ```

2. **Python dependencies** (install from project root):
   ```powershell
   pip install -r requirements.txt
   ```

### For Building Installers

1. **Inno Setup** (Windows only):
   - Download from: https://jrsoftware.org/isinfo.php
   - Install and ensure `iscc.exe` is in your PATH

## Building Executable

### Quick Build

From the project root:
```powershell
.\setup\build-exe.ps1
```

### Build with Options

```powershell
# Clean build (removes previous builds)
.\setup\build-exe.ps1 -Clean

# Specify version
.\setup\build-exe.ps1 -Version "1.0.1"
```

The executable will be created in `dist/ic256-sampler.exe`.

### Manual Build with PyInstaller

```powershell
cd setup
pyinstaller ic256-sampler.spec
```

## Building Installer

1. First, build the executable (see above)

2. Build the installer with Inno Setup:
   ```powershell
   iscc setup\BuildForIC256.iss
   ```

   Or open `BuildForIC256.iss` in Inno Setup Compiler and click "Build".

The installer will be created in `dist/IC256-Sampler-Setup-{version}.exe`.

## Build Process

1. **PyInstaller** bundles the Python application and dependencies into a single executable
2. **Inno Setup** creates a Windows installer that:
   - Installs the executable to Program Files
   - Creates Start Menu shortcuts
   - Creates desktop shortcut (optional)
   - Sets up data directory
   - Handles uninstallation

## Troubleshooting

### Executable is too large

The spec file includes many hidden imports. If the executable is too large, you can:
- Remove unused hidden imports from `ic256-sampler.spec`
- Use `--exclude-module` to exclude unused modules

### Missing dependencies

If the executable fails to run, check:
- All required modules are in `hiddenimports` in the spec file
- All data files (images) are included in `datas` section
- Check PyInstaller logs for missing modules

### Icon not showing

Ensure `logo.ico` exists in the `setup/` directory and the path in the spec file is correct.

## Best Practices

1. **Always test the executable** on a clean machine before distribution
2. **Version control the spec file** - it ensures reproducible builds
3. **Update version numbers** in both the spec file and Inno Setup script
4. **Test the installer** on a fresh Windows installation
5. **Sign the executable** (optional but recommended for distribution)
