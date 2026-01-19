# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec file for IC256 Sampler
# Generated and maintained for building standalone executables
#
# Entry point: Uses run.py for PyInstaller compatibility
# The package also supports: python -m ic256_sampler (via __main__.py)

import sys
from pathlib import Path

# Get project root (parent of setup directory)
project_root = Path(SPECPATH).parent
package_dir = project_root / "ic256_sampler"
assets_dir = package_dir / "assets" / "images"

block_cipher = None

a = Analysis(
    [str(project_root / "run.py")],  # Entry point script
    pathex=[str(project_root)],
    binaries=[],
    datas=[
        # ImageLoader expects images in sys._MEIPASS/images
        (str(assets_dir), "images"),
    ],
    hiddenimports=[
        # PIL/Pillow for image handling
        "PIL",
        "PIL._imaging",
        "PIL.Image",
        "PIL.ImageTk",
        "PIL.ImageFile",
        "PIL._tkinter_finder",
        # Core dependencies
        "portalocker",
        "msgpack",
        # WebSocket client
        "websocket",
        "websocket._abnf",
        "websocket._app",
        "websocket._core",
        "websocket._exceptions",
        "websocket._handshake",
        "websocket._http",
        "websocket._logging",
        "websocket._socket",
        "websocket._ssl_compat",
        "websocket._url",
        "websocket._utils",
        # Tkinter GUI
        "tkinter",
        "tkinter.ttk",
        "tkinter.filedialog",
        "tkinter.messagebox",
        "tkinter.scrolledtext",
        # HTTP requests
        "requests",
        "urllib3",
        "certifi",
        "charset_normalizer",
        # Standard library modules (explicit for PyInstaller)
        "json",
        "threading",
        "csv",
        "bisect",
        "collections",
        "collections.abc",
        "ipaddress",
        "datetime",
        "tempfile",
        "atexit",
        "pathlib",
        "signal",
        "traceback",
        "sys",
        "os",
        "time",
        "enum",
        "dataclasses",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude unnecessary modules to reduce size
        "matplotlib",
        "numpy",
        "pandas",
        "scipy",
        "pytest",
        "IPython",
        "jupyter",
        "notebook",
        "setuptools",
        "distutils",
        "unittest",
        "doctest",
        "pdb",
        "tkinter.test",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="ic256-sampler",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # UPX compression can cause issues - disable by default, enable if needed
    upx=False,
    upx_exclude=[
        # Exclude problematic binaries from UPX compression
        "vcruntime140.dll",
        "python*.dll",
    ],
    runtime_tmpdir=None,
    console=False,  # No console window (GUI app)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(Path(SPECPATH) / "logo.ico"),
)
