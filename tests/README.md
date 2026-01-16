# Test Suite

This directory contains the unit test suite for IC256 Sampler.

## Test Files

- **`test_utils.py`** - Tests for utility functions (IP validation, device validation with mocks)
- **`test_device_paths.py`** - Tests for device path configuration and helper functions
- **`test_ic256_model.py`** - Tests for IC256Model conversion functions and column definitions
- **`test_config.py`** - Tests for configuration management (loading, saving, validation)
- **`test_integration.py`** - Integration tests that require live device connections (optional)
- **`conftest.py`** - Pytest configuration and shared fixtures

## Running Tests

### Install Dependencies

```bash
pip install -e ".[dev]"
```

### Run All Tests

```bash
pytest
```

### Run with Coverage

```bash
pytest --cov=ic256_sampler --cov-report=html
```

### Run Specific Test File

```bash
pytest tests/test_utils.py
```

### Run Specific Test

```bash
pytest tests/test_utils.py::TestIsValidIPv4::test_valid_ipv4_addresses
```

### Run Integration Tests (Requires Live Devices)

```bash
# Run integration tests with real devices from config.json
pytest tests/test_integration.py -v

# Skip integration tests
pytest -m "not integration"
```

### Verbose Output

```bash
pytest -v
```

## Test Coverage

Current test coverage includes:

### Utils Module (`test_utils.py`)
- ✅ IPv4 address validation (valid, invalid, edge cases)
- ✅ Device validation with mocked HTTP requests
- ✅ Error handling (connection errors, timeouts, HTTP errors)
- ✅ Case-insensitive device matching

### Device Paths Module (`test_device_paths.py`)
- ✅ Path structure validation
- ✅ Helper functions (get_ic256_45_path, get_tx2_path, get_admin_path)
- ✅ HTTP URL building
- ✅ Error handling for invalid categories/keys

### IC256 Model Module (`test_ic256_model.py`)
- ✅ Mean value conversion (X/Y axis, invalid values)
- ✅ Sigma value conversion (X/Y axis, invalid values)
- ✅ IC256Model converter methods
- ✅ Column definition creation
- ✅ Gaussian value processing
- ✅ CSV header generation (IC256, TX2, unknown devices)
- ✅ Time binning function
- ✅ Sorted buffer cache (cache hits/misses, sorting)

### Config Module (`test_config.py`)
- ✅ Config file initialization (existing, missing, invalid JSON)
- ✅ Config file updates (valid, invalid IPs, empty values)
- ✅ Error handling (missing files, invalid JSON)

## Test Structure

Tests are organized using pytest's class-based structure:

```python
class TestFunctionName:
    """Test suite for function_name."""
    
    def test_specific_scenario(self):
        """Test description."""
        # Test implementation
        assert condition
```

## Mocking

Tests use `unittest.mock` for:
- HTTP requests (device validation)
- File I/O (configuration management)
- Tkinter widgets (GUI components)

## Integration Tests

Integration tests in `test_integration.py` use real device IPs from `config.json`:
- **IC256 Device**: Uses `ic256_45` IP from config.json
- **TX2 Device**: Uses `tx2` IP from config.json

These tests:
- Will **skip** if devices are unreachable (not a failure)
- Require network connectivity to test devices
- Can be excluded with `pytest -m "not integration"`

### Running Integration Tests

```bash
# Run only integration tests
pytest tests/test_integration.py -v

# Run all tests including integration
pytest -v

# Skip integration tests
pytest -m "not integration" -v
```

## Future Test Additions

Areas that could benefit from additional tests:
- GUI components (requires GUI testing framework)
- WebSocket client (requires mocking websocket connections)
- Main application flow (integration tests)
- Data collection end-to-end (requires device mocking or live devices)

## Continuous Integration

Tests are configured to run with coverage reporting. The `pyproject.toml` includes pytest configuration:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = "-v --cov=ic256_sampler --cov-report=term-missing"
```
