# Migration Guide: Old Structure to New Structure

This document explains the changes made to modernize the project structure.

## Key Changes

### 1. Package Structure
- **Before**: All Python files in `src/` directory with flat imports
- **After**: Proper Python package `ic256_sampler/` with relative imports

### 2. Directory Organization
- **Before**: 
  ```
  src/
    ├── *.py
    ├── images/
    └── Data/
  ```
- **After**:
  ```
  ic256_sampler/
    ├── __init__.py
    ├── *.py (modules)
    └── assets/
        └── images/
  data/ (project root)
  tests/
  ```

### 3. Configuration Files
- Added `pyproject.toml` for modern Python project configuration
- Added `requirements.txt` for dependencies
- Added `.editorconfig` for consistent code formatting
- Added `mypy.ini` for type checking configuration
- Updated `.gitignore` with project-specific ignores

### 4. Import Changes
All imports have been updated from absolute to relative imports:
- `from gui import GUI` → `from .gui import GUI`
- `from utils import is_valid_device` → `from .utils import is_valid_device`
- etc.

### 5. Path Updates
- Config file path: Now uses project root instead of script directory
- Image paths: Updated to use package assets directory
- Data directory: Moved to project root `data/` folder

## Running the Application

### Option 1: Install as Package
```bash
pip install -e .
ic256-sampler
```

### Option 2: Run Directly
```bash
python run.py
```

### Option 3: Run as Module
```bash
python -m ic256_sampler.main
```

## Configuration File Location

The `config.json` file is now expected in the project root directory, not in the package directory.

## Data Files

Collected data files are now saved to the `data/` directory in the project root by default.

## Development

To set up for development:
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install package in editable mode with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check .

# Format code
ruff format .
```

## Backward Compatibility

The old `src/` directory structure is preserved for reference but should not be used. All new development should use the `ic256_sampler/` package structure.
