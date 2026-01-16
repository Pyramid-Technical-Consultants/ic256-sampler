"""Pytest configuration and fixtures."""

import pytest
import sys
import json
from pathlib import Path

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
