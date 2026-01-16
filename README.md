# IC256 Sampler

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
git clone https://github.com/yourusername/ic256-sampler.git
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

```bash
ic256-sampler
```

Or run directly:
```bash
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

```
ic256-sampler/
├── ic256_sampler/          # Main package
│   ├── __init__.py
│   ├── main.py            # Application entry point
│   ├── gui.py             # GUI implementation
│   ├── data_collection.py  # Data collection logic
│   ├── config.py          # Configuration management
│   ├── utils.py           # Utility functions
│   ├── igx_client.py      # WebSocket client
│   ├── device_paths.py    # Device path configuration
│   └── assets/
│       └── images/        # Application images
├── tests/                 # Test suite
├── data/                  # Data output directory
├── config.json           # Application configuration
├── pyproject.toml        # Project configuration
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Development

### Setting Up Development Environment

1. Install development dependencies:
```bash
pip install -e ".[dev]"
```

2. Run tests:
```bash
pytest
```

3. Run linting:
```bash
ruff check .
```

4. Format code:
```bash
ruff format .
```

### Code Style

This project uses:
- **Ruff** for linting and formatting
- **mypy** for type checking (optional)
- **pytest** for testing

## Building Executables

The project includes setup scripts for building standalone executables:

- **Windows**: Use `setup/build-exe.ps1` (PowerShell script)
- **Inno Setup**: Use `setup/BuildForIC256.iss` for Windows installer

See `setup/README.md` for detailed build instructions.

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.
