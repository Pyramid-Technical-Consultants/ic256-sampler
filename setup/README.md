# Build Scripts for IC256 Sampler

This directory contains scripts and configuration files for building standalone executables and installers.

## Files

- **`build-exe.ps1`** - Enhanced PowerShell script to build Windows executable using PyInstaller
- **`ic256-sampler.spec`** - PyInstaller specification file for reproducible builds
- **`BuildForIC256.iss`** - Inno Setup script for creating Windows installer
- **`logo.ico`** - Application icon used in builds

## Prerequisites

### For Building Executables

1. **Python 3.8+** installed and in PATH

2. **PyInstaller** (will be auto-installed if missing):
   ```powershell
   pip install pyinstaller
   ```
   Or install build dependencies:
   ```powershell
   pip install -e ".[build]"
   ```

3. **Python dependencies** (install from project root):
   ```powershell
   pip install -r requirements.txt
   ```

### For Building Installers

1. **Inno Setup** (Windows only):
   - Download from: https://jrsoftware.org/isinfo.php
   - Install and ensure `iscc.exe` is in your PATH
   - Recommended version: 6.0 or later

## Building Executable

### Quick Build

From the project root:
```powershell
.\setup\build-exe.ps1
```

The script will:
- Auto-detect version from `pyproject.toml`
- Validate prerequisites (Python, PyInstaller, files)
- Build the executable using the spec file
- Display build statistics

### Build with Options

```powershell
# Clean build (removes previous builds)
.\setup\build-exe.ps1 -Clean

# Skip validation checks (faster, less safe)
.\setup\build-exe.ps1 -SkipValidation

# Combine options
.\setup\build-exe.ps1 -Clean -SkipValidation
```

The executable will be created in `dist/ic256-sampler.exe`.

### Manual Build with PyInstaller

```powershell
cd setup
pyinstaller --clean ic256-sampler.spec
```

## Building Installer

1. **First, build the executable** (see above)

2. **Build the installer** with Inno Setup:
   ```powershell
   iscc setup\BuildForIC256.iss
   ```

   Or open `BuildForIC256.iss` in Inno Setup Compiler and click "Build".

The installer will be created in `dist/IC256-Sampler-Setup-{version}.exe`.

**Note:** The installer script includes a pre-build check to ensure the executable exists.

## Build Process

1. **PyInstaller** bundles the Python application and dependencies into a single executable:
   - Includes all required Python packages
   - Bundles image assets
   - Creates a single-file executable (no external dependencies needed)
   - Configures for Windows GUI (no console window)

2. **Inno Setup** creates a Windows installer that:
   - Installs the executable to Program Files
   - Creates Start Menu shortcuts
   - Creates desktop shortcut (optional)
   - Sets up data directory with proper permissions
   - Handles uninstallation cleanly

## Configuration

### Version Management

The build script automatically reads the version from `pyproject.toml`. To update the version:

1. Edit `pyproject.toml` and update the `version` field
2. The build script will use this version automatically
3. The Inno Setup script uses a hardcoded version - update `#define AppVersion` if needed

### Spec File Customization

The `ic256-sampler.spec` file contains all PyInstaller configuration:

- **`hiddenimports`**: Modules that PyInstaller might miss (e.g., `msgpack`, `PIL`, `websocket`)
- **`datas`**: Data files to include (images are bundled here)
- **`excludes`**: Modules to exclude to reduce size
- **`upx`**: UPX compression (disabled by default - can cause issues)

### Assets

Images are automatically bundled from `ic256_sampler/assets/images/` and placed in `sys._MEIPASS/images/` at runtime. The `ImageLoader` class handles this automatically.

## Troubleshooting

### Build Script Issues

**"Python not found"**
- Ensure Python is installed and in your PATH
- Test with: `python --version`

**"PyInstaller not found"**
- The script will attempt to install it automatically
- Or manually: `pip install pyinstaller`

**"Spec file not found"**
- Ensure you're running from the project root
- Check that `setup/ic256-sampler.spec` exists

### Executable Issues

**Executable is too large**
- The spec file includes exclusions for common unused modules
- You can add more to the `excludes` list in the spec file
- UPX compression is disabled by default (set `upx=True` in spec if needed)

**"Missing module" errors at runtime**
- Add the missing module to `hiddenimports` in the spec file
- Check PyInstaller build logs for warnings
- Test on a clean machine to identify missing dependencies

**Images not loading**
- Images are bundled to `sys._MEIPASS/images/`
- The `ImageLoader` class handles this automatically
- Check that all image files exist in `ic256_sampler/assets/images/`

**Executable crashes on startup**
- Enable console mode temporarily: set `console=True` in spec file
- Check Windows Event Viewer for error details
- Test with: `pyinstaller --debug=all ic256-sampler.spec`

### Installer Issues

**"Executable not found" error**
- Build the executable first using `build-exe.ps1`
- Ensure `dist/ic256-sampler.exe` exists before building installer

**Installer fails to create**
- Check that Inno Setup is installed correctly
- Verify `iscc.exe` is in PATH: `iscc` should run
- Check Inno Setup compiler output for errors

## Best Practices

1. **Always test the executable** on a clean machine before distribution
2. **Version control the spec file** - it ensures reproducible builds
3. **Update version numbers** in `pyproject.toml` (build script reads from here)
4. **Test the installer** on a fresh Windows installation
5. **Use clean builds** (`-Clean` flag) when troubleshooting
6. **Check build output** for warnings about missing modules
7. **Sign the executable** (optional but recommended for distribution)
8. **Test all features** after building to ensure nothing is missing

## Build Output

After a successful build, you'll see:
- Executable path and size
- Build duration
- Next steps (testing, building installer)

Example output:
```
========================================
  Build Successful!
========================================
Executable: C:\...\dist\ic256-sampler.exe
Size: 45.23 MB (46356 KB)
Build time: 12.3 seconds
```

## Advanced Usage

### Custom Build Configuration

Edit `ic256-sampler.spec` to customize:
- Compression settings
- Included/excluded modules
- Binary dependencies
- UPX compression

### Debug Builds

For debugging, modify the spec file:
```python
exe = EXE(
    ...
    console=True,  # Show console for debugging
    debug=True,    # Include debug symbols
    ...
)
```

### Distribution Checklist

Before distributing:
- [ ] Test executable on clean Windows machine
- [ ] Verify all images load correctly
- [ ] Test all application features
- [ ] Check file size is reasonable
- [ ] Build installer and test installation
- [ ] Test uninstallation
- [ ] Verify Start Menu shortcuts work
- [ ] Test on Windows 10 and 11 (if applicable)
