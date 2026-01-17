# Test Suite

This directory contains the unit and integration test suite for IC256 Sampler.

## Test Organization

Tests are now organized into a clear structure separating unit tests from integration tests:

```
tests/
├── unit/                    # Pure unit tests (no live devices, fast)
│   ├── core/               # Core utility modules
│   ├── database/           # Database modules
│   ├── manager/            # Device manager
│   ├── collector/          # Data collectors
│   ├── writer/             # CSV writer
│   └── application/         # Application class
│
├── integration/             # Integration tests (may require live devices)
│   ├── devices/            # Device connection tests
│   ├── application/        # Application integration tests
│   ├── manager/            # Device manager integration
│   ├── database/           # Database integration
│   ├── websocket/         # WebSocket tests
│   ├── collector/         # Collector integration
│   └── writer/            # CSV writer integration
│
├── benchmarks/             # Performance benchmarks
│
└── conftest.py            # Shared fixtures
```

## Running Tests

### Install Dependencies

```bash
pip install -e ".[dev]"
```

### Run All Tests

```bash
pytest
```

### Run Unit Tests Only (Fast)

```bash
pytest tests/unit/ -v
```

### Run Integration Tests Only

```bash
pytest tests/integration/ -v
```

### Run Specific Category

```bash
# Core utilities
pytest tests/unit/core/ -v

# Database tests
pytest tests/unit/database/ -v

# Device integration tests
pytest tests/integration/devices/ -v

# Application integration tests
pytest tests/integration/application/ -v
```

### Skip Integration Tests

```bash
# Skip all integration tests (run only unit tests)
pytest -m "not integration" -v

# Or use the directory structure
pytest tests/unit/ -v
```

### Run with Coverage

```bash
pytest --cov=ic256_sampler --cov-report=html
```

### Run Specific Test File

```bash
pytest tests/unit/core/test_utils.py -v
```

### Run Specific Test

```bash
pytest tests/unit/core/test_utils.py::TestIsValidIPv4::test_valid_ipv4_addresses -v
```

## Test Categories

### Unit Tests (`tests/unit/`)

Fast tests that don't require live devices or external dependencies:

- **core/**: Utility functions, device paths, models, config
- **database/**: IODatabase and VirtualDatabase unit tests
- **manager/**: DeviceManager unit tests (mocked)
- **collector/**: Model collector unit tests
- **writer/**: CSV writer unit tests
- **application/**: Application class unit tests

### Integration Tests (`tests/integration/`)

Tests that may require live devices or test full workflows:

- **devices/**: Device connection and data collection rate tests
- **application/**: End-to-end application workflows
- **manager/**: DeviceManager with real devices
- **database/**: Database integration with real data
- **websocket/**: WebSocket persistence and connection status
- **collector/**: Collector integration tests
- **writer/**: CSV writer with real device data

Integration tests will **skip** if devices are unreachable (not a failure).

## Test Coverage

### Core Module (`unit/core/`)
- ✅ IPv4 address validation
- ✅ Device validation with mocked HTTP requests
- ✅ Device path configuration and helper functions
- ✅ IC256Model conversion functions
- ✅ Configuration management
- ✅ MessagePack serialization

### Database Module (`unit/database/`)
- ✅ IODatabase data structure
- ✅ VirtualDatabase row generation
- ✅ Channel policies (SYNCHRONIZED, INTERPOLATED, ASYNCHRONOUS)
- ✅ Edge cases and performance

### Device Manager (`unit/manager/`)
- ✅ DeviceManager initialization
- ✅ Device connection creation (mocked)
- ✅ Data collection thread coordination
- ✅ Thread lifecycle management

### Application (`unit/application/`)
- ✅ Application initialization
- ✅ GUI value handling
- ✅ Sampling rate validation
- ✅ Callback functions

### Integration Tests
- ✅ Real device connections
- ✅ Data collection at expected rates
- ✅ End-to-end workflows
- ✅ CSV file generation with real data
- ✅ WebSocket persistence

## Key Test Files

### Critical Regression Tests

1. **`unit/manager/test_device_manager.py::test_collect_from_device_calls_update_subscribed_fields`**
   - Ensures data collection thread calls `updateSubscribedFields()`
   - Prevents regression where collection thread doesn't process websocket messages

2. **`integration/devices/test_data_collection_rate.py::test_ic256_sampling_rate_3000hz`**
   - End-to-end verification of data collection at expected rates
   - Catches issues in the full data collection pipeline

## Test Structure

Tests use pytest's class-based structure:

```python
class TestFunctionName:
    """Test suite for function_name."""
    
    def test_specific_scenario(self):
        """Test description."""
        # Test implementation
        assert condition
```

## Mocking

Unit tests use `unittest.mock` for:
- HTTP requests (device validation)
- File I/O (configuration management)
- Tkinter widgets (GUI components)
- WebSocket clients (device manager tests)

## Integration Test Requirements

Integration tests in `tests/integration/` use real device IPs from `config.json`:
- **IC256 Device**: Uses `ic256_45` IP from config.json
- **TX2 Device**: Uses `tx2` IP from config.json

These tests:
- Will **skip** if devices are unreachable (not a failure)
- Require network connectivity to test devices
- Can be excluded with `pytest -m "not integration"` or `pytest tests/unit/`

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

## Notes

- All test files import from `ic256_sampler.*` - imports remain unchanged
- `conftest.py` fixtures are automatically available to all tests
- Integration tests are marked with `@pytest.mark.integration`
- See `REORGANIZATION_PLAN.md` for details on the reorganization
