# Project Structure

This document describes the modernized project structure.

## Directory Layout

```
ic256-sampler/
├── ic256_sampler/              # Main Python package
│   ├── __init__.py            # Package initialization
│   ├── main.py                # Application entry point
│   ├── gui.py                 # GUI implementation
│   ├── data_collection.py     # Data collection logic
│   ├── config.py              # Configuration management
│   ├── utils.py               # Utility functions
│   ├── igx_client.py          # WebSocket client
│   ├── device_paths.py        # Device path configuration
│   └── assets/
│       └── images/             # Application images/icons
│
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── conftest.py            # Pytest configuration
│   └── test_utils.py          # Utility function tests
│
├── data/                       # Data output directory (gitignored)
│   └── *.csv                  # Collected data files
│
├── scripts/                    # Build scripts and tools (standard Python convention)
│   ├── build_exe.ps1          # PowerShell script for building executables
│   ├── ic256_sampler.spec     # PyInstaller specification file
│   ├── build_for_ic256.iss    # Inno Setup installer script
│   ├── logo.ico               # Application icon
│   └── README.md              # Build instructions
│
├── pyproject.toml             # Modern Python project configuration
├── requirements.txt           # Python dependencies
├── README.md                  # Project documentation
├── MIGRATION_GUIDE.md         # Migration guide from old structure
├── PROJECT_STRUCTURE.md       # This file
├── .gitignore                 # Git ignore rules
├── .editorconfig              # Editor configuration
├── mypy.ini                    # Type checking configuration
├── .ruff.toml                 # Linting configuration (optional)
├── run.py                     # Entry point script
└── LICENSE                    # License file

```

## Key Components

### Package Structure (`ic256_sampler/`)

All application code is organized in a proper Python package following PEP 8 naming conventions (snake_case):

- **`__init__.py`**: Package metadata and version information
- **`__main__.py`**: Module entry point (enables `python -m ic256_sampler`)
- **`main.py`**: Application entry point with `main()` function
- **`application.py`**: Main application class
- **`config.py`**: Configuration file management
- **`utils.py`**: Utility functions (IP validation, device validation)
- **`igx_client.py`**: WebSocket client for device communication
- **`device_paths.py`**: Device API path configuration
- **`gui/`**: GUI package with modular components
- **`assets/images/`**: Application images and icons

### Configuration Files

- **`pyproject.toml`**: Modern Python project configuration (PEP 518/621)
  - Project metadata
  - Dependencies
  - Build system configuration
  - Tool configurations (ruff, mypy, pytest)

- **`requirements.txt`**: Simple dependency list for pip

- **`.gitignore`**: Comprehensive ignore rules for Python projects

- **`.editorconfig`**: Consistent code formatting across editors

- **`mypy.ini`**: Type checking configuration

### Testing

- **`tests/`**: Test suite directory
- **`conftest.py`**: Pytest configuration and fixtures
- Test files follow `test_*.py` naming convention

### Data and Assets

- **`data/`**: Output directory for collected CSV files (gitignored)
- **`ic256_sampler/assets/`**: Package assets (images, etc.)

## Import Structure

All imports use relative imports within the package:

```python
from .gui import GUI
from .utils import is_valid_device
from .config import update_file_json
```

## Running the Application

### Standard Python Methods

This project supports all standard Python ways to run an application:

```bash
# 1. After installation (recommended for end users)
pip install -e .
ic256-sampler

# 2. As a Python module (standard, recommended)
python -m ic256_sampler

# 3. Direct script execution (development convenience)
python run.py

# 4. Via entry point module
python -m ic256_sampler.main
```

All methods are equivalent and follow Python packaging best practices.

### Configuration

Configuration file location:
- **Development**: `config.json` in project root
- **Installed**: `~/.ic256-sampler/config.json`

Data files are saved to:
- **Development**: `data/` in project root
- **Installed**: User-specified path (defaults to project root `data/` if available)

## Development Workflow

1. **Setup**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -e ".[dev]"
   ```

2. **Testing**:
   ```bash
   pytest
   ```

3. **Linting**:
   ```bash
   ruff check .
   ruff format .
   ```

4. **Type Checking**:
   ```bash
   mypy ic256_sampler
   ```

## Benefits of This Structure

1. **Proper Package**: Follows Python packaging best practices
2. **Installable**: Can be installed as a package with `pip install -e .`
3. **Testable**: Clear test structure with pytest
4. **Maintainable**: Organized code with clear separation of concerns
5. **Modern**: Uses `pyproject.toml` (PEP 518/621) instead of `setup.py`
6. **Type-Safe**: Configured for type checking with mypy
7. **Linted**: Configured for code quality with ruff
8. **Documented**: Comprehensive README and documentation
