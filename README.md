# IC256 Sampler

[![Build Windows Executable](https://github.com/Pyramid-Technical-Consultants/ic256-sampler/actions/workflows/build.yml/badge.svg)](https://github.com/Pyramid-Technical-Consultants/ic256-sampler/actions/workflows/build.yml)

A modern Python application for collecting and managing data from IC256 and TX2 measurement devices.

## Features

- Real-time data collection from IC256 and TX2 devices
- Modern, responsive GUI built with Tkinter
- Configurable sampling rates (1-6000 Hz)
- Automatic timestamp alignment and data synchronization
- CSV export with comprehensive metadata
- Device validation and connection management
- Environment sensor data collection (temperature, humidity, pressure)

## Requirements

- Python 3.8 or higher
- Network access to IC256/TX2 devices
- Windows, macOS, or Linux

## Installation

### From Source

1. Clone the repository:
```bash
git clone https://github.com/Pyramid-Technical-Consultants/ic256-sampler.git
cd ic256-sampler
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install the package in development mode:
```bash
pip install -e .
```

## Usage

### Running the Application

After installation, you can run the application in several ways:

```bash
# Using the entry point (recommended after installation)
ic256-sampler

# As a Python module (standard way)
python -m ic256_sampler

# Direct script execution (development)
python run.py

# Using the main module directly
python -m ic256_sampler.main
```

### Configuration

The application uses a `config.json` file in the project root for storing:
- Device IP addresses (IC256 and TX2)
- Default save path for data files
- Default sampling rate

The configuration file is automatically created on first run with default values.

### Data Collection

1. **Configure Devices**: Enter IP addresses for IC256 and TX2 devices in the Settings tab
2. **Set Sampling Rate**: Configure the desired sampling rate (1-6000 Hz)
3. **Select Save Path**: Choose where to save collected data files
4. **Start Collection**: Click "START" to begin data collection
5. **Monitor Progress**: View elapsed time and log messages in real-time
6. **Stop Collection**: Click "STOP" to end data collection and save the file

### Data Files

Collected data is saved as CSV files with the following naming convention:
- IC256: `IC256_42x35-YYYYMMDD-HHMMSS.csv`
- TX2: `TX2-YYYYMMDD-HHMMSS.csv`

## Project Structure

This project follows standard Python packaging conventions (PEP 518/621):

```
ic256-sampler/
├── ic256_sampler/          # Main Python package
│   ├── __init__.py         # Package initialization
│   ├── __main__.py         # Module entry point (python -m ic256_sampler)
│   ├── main.py             # Application entry point
│   ├── application.py      # Main application class
│   ├── config.py           # Configuration management
│   ├── utils.py            # Utility functions
│   ├── igx_client.py       # WebSocket client
│   ├── device_paths.py     # Device path configuration
│   └── assets/
│       └── images/         # Application images
├── tests/                  # Test suite (pytest)
│   ├── unit/               # Unit tests
│   └── integration/        # Integration tests
├── scripts/                # Build scripts and tools
│   ├── build_exe.ps1       # PyInstaller build script
│   └── build_for_ic256.iss # Inno Setup installer script
├── data/                   # Data output directory (gitignored)
├── pyproject.toml          # Modern Python project config (PEP 518/621)
├── requirements.txt        # Python dependencies
├── MANIFEST.in             # Package data inclusion rules
├── README.md               # This file
└── LICENSE                 # License file
```

## Development

### Setting Up Development Environment

1. Clone the repository and create a virtual environment:
```bash
git clone https://github.com/Pyramid-Technical-Consultants/ic256-sampler.git
cd ic256-sampler
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install the package in editable mode with development dependencies:
```bash
pip install -e ".[dev]"
```

### Development Tools

- **Testing**: Run tests with `pytest`
  ```bash
  pytest                    # All tests
  pytest tests/unit/       # Unit tests only
  pytest -m "not integration"  # Skip integration tests
  ```

- **Linting**: Check code quality with `ruff check .`

- **Formatting**: Format code with `ruff format .`

- **Type Checking**: Optional type checking with `mypy ic256_sampler`

## Building Executables

### Local Build

Build standalone Windows executables using the provided scripts:

- **Executable**: Use `scripts/build_exe.ps1` (PowerShell script)
  - Creates versioned executable: `dist/ic256-sampler-{version}.exe`
  - GUI window title includes version: "IC256 Sampler v{version}"
- **Installer**: Use `scripts/build_for_ic256.iss` for Windows installer

See `scripts/README.md` for detailed build instructions.

### GitHub Actions Build

The project includes a GitHub Actions workflow for building Windows executables on-demand:

1. Go to the **Actions** tab in GitHub
2. Select **Build Windows Executable** from the workflow list
3. Click **Run workflow** and select the branch (usually `main`)
4. Wait for the workflow to complete
5. Download the artifact named `ic256-sampler-windows-{version}`

**Note**: The workflow is manual-only to save compute resources for infrequent releases.

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Security

For security vulnerabilities, please see [SECURITY.md](SECURITY.md) for our security policy and reporting process.

**Important**: This software is NOT intended for use in safety-critical applications.

## Support

For issues and questions, please open an issue on [GitHub](https://github.com/Pyramid-Technical-Consultants/ic256-sampler/issues).
