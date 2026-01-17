"""Pytest configuration and fixtures."""

import pytest
import sys
import json
from pathlib import Path
from unittest.mock import Mock, patch

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def temp_config_file(tmp_path):
    """Fixture to create a temporary config file."""
    config_file = tmp_path / "config.json"
    return config_file


@pytest.fixture
def config_file_path():
    """Fixture to get the path to the project config.json file."""
    return project_root / "config.json"


@pytest.fixture
def device_config(config_file_path):
    """Fixture to load device configuration from config.json.
    
    Returns a dictionary with device IPs from config.json.
    Falls back to defaults if config.json doesn't exist.
    """
    default_config = {
        "ic256_45": "10.11.25.67",
        "tx2": "10.11.25.202",
        "save_path": str(project_root / "data"),
        "sampling_rate": 3000,
    }
    
    if config_file_path.exists():
        try:
            with open(config_file_path) as f:
                config = json.load(f)
                # Merge with defaults to ensure all keys exist
                default_config.update(config)
        except (json.JSONDecodeError, IOError):
            pass
    
    return default_config


@pytest.fixture
def ic256_ip(device_config):
    """Fixture to get IC256 device IP from config.json."""
    return device_config.get("ic256_45", "10.11.25.67")


@pytest.fixture
def tx2_ip(device_config):
    """Fixture to get TX2 device IP from config.json."""
    return device_config.get("tx2", "10.11.25.202")


@pytest.fixture
def require_ic256_device(ic256_ip):
    """Fixture that skips test if IC256 device is not available."""
    from ic256_sampler.utils import is_valid_device, is_valid_ipv4
    
    if not is_valid_ipv4(ic256_ip):
        pytest.skip(f"Invalid IP address in config: {ic256_ip}")
    
    if not is_valid_device(ic256_ip, "IC256"):
        pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
    
    return ic256_ip


@pytest.fixture
def mock_gui_window(ic256_ip, tmp_path):
    """Fixture to create a mock GUI window with common defaults."""
    mock_window = Mock()
    mock_window.ix256_a_entry = Mock()
    mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
    mock_window.tx2_entry = Mock()
    mock_window.tx2_entry.get = Mock(return_value="")
    mock_window.note_entry = Mock()
    mock_window.note_entry.get = Mock(return_value="Test")
    mock_window.path_entry = Mock()
    mock_window.path_entry.get = Mock(return_value=str(tmp_path))
    mock_window.sampling_entry = Mock()
    mock_window.sampling_entry.get = Mock(return_value="500")
    mock_window.root = Mock()
    mock_window.root.after = Mock()
    mock_window.reset_elapse_time = Mock()
    mock_window.reset_statistics = Mock()
    mock_window.update_statistics = Mock()
    return mock_window


@pytest.fixture
def gui_patches():
    """Fixture that provides common GUI patches as a context manager."""
    return patch.multiple(
        'ic256_sampler.application',
        safe_gui_update=Mock(),
        set_button_state_safe=Mock(),
        show_message_safe=Mock(),
        log_message_safe=Mock()
    )


@pytest.fixture
def app_with_mock_gui(mock_gui_window, gui_patches):
    """Fixture to create an Application instance with mocked GUI.
    
    Ensures proper cleanup of threads and connections after test.
    """
    from ic256_sampler.application import Application
    import time
    
    app = Application()
    app.window = mock_gui_window
    
    with gui_patches:
        yield app
        
        # Cleanup: ensure all threads are stopped and connections closed
        try:
            app.stop_collection()
            time.sleep(0.5)  # Give threads time to stop
            
            if hasattr(app, 'device_manager') and app.device_manager:
                app.device_manager.stop()
                app.device_manager.close_all_connections()
        except Exception:
            pass  # Ignore cleanup errors


def verify_csv_file(csv_file, min_rows=10, expected_columns=None):
    """Helper function to verify CSV file structure and content.
    
    Args:
        csv_file: Path to CSV file
        min_rows: Minimum number of data rows expected
        expected_columns: List of column names that should exist in header
        
    Returns:
        tuple: (row_count, header)
    """
    import csv
    
    expected_columns = expected_columns or [
        "Timestamp (s)", "Dose", "Channel Sum", "Temperature", "Humidity"
    ]
    
    assert csv_file.exists(), f"CSV file should exist: {csv_file}"
    assert csv_file.stat().st_size > 0, "CSV file should not be empty"
    
    with open(csv_file, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)
        assert len(header) > 0, "CSV should have header row"
        
        # Verify expected columns exist
        header_str = ' '.join(header)
        for col in expected_columns:
            assert col in header_str, f"CSV header should contain '{col}'. Header: {header}"
        
        # Count data rows
        row_count = 0
        data_rows = []
        for row in reader:
            if row and len(row) == len(header):
                row_count += 1
                if row_count <= 5:
                    data_rows.append(row)
        
        assert row_count >= min_rows, \
            f"CSV should have at least {min_rows} rows. Got {row_count} rows."
        assert len(data_rows) > 0, "CSV should have at least one data row"
        
        # Verify time column is increasing
        if "Timestamp (s)" in header and len(data_rows) >= 2:
            time_col_idx = header.index("Timestamp (s)")
            try:
                time1 = float(data_rows[0][time_col_idx])
                time2 = float(data_rows[1][time_col_idx])
                assert time2 > time1, f"Time should be increasing. Row 1: {time1}, Row 2: {time2}"
            except (ValueError, IndexError):
                pass
        
        return row_count, header


def wait_for_condition(condition_func, timeout=10.0, interval=0.5, description="condition"):
    """Helper function to wait for a condition with timeout.
    
    Args:
        condition_func: Callable that returns True when condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
        description: Description for error message
        
    Returns:
        bool: True if condition was met, False if timeout
    """
    import time
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        time.sleep(interval)
    
    return False
