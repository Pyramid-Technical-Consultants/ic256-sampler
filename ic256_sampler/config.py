"""Configuration management module for IC256 data collection application.

This module handles loading and saving application configuration from/to JSON files.
"""
import tkinter as tk
import json
from typing import Optional
from .utils import is_valid_ipv4
import pathlib
import sys
import os

# Get the project root directory (parent of ic256_sampler package)
# Handle both installed package and development scenarios
_package_dir = pathlib.Path(__file__).parent.resolve()
_project_root = _package_dir.parent.resolve()

# Path to the Desktop directory
desktop_path = pathlib.Path.home() / "Desktop"

# File path - use project root for config
# Check if we're in development (pyproject.toml exists) or installed package
if (_project_root / "pyproject.toml").exists():
    # Development mode - use project root
    file_path = _project_root / "config.json"
else:
    # Installed package - use user home directory
    user_config_dir = pathlib.Path.home() / ".ic256-sampler"
    user_config_dir.mkdir(parents=True, exist_ok=True)
    file_path = user_config_dir / "config.json"

# Default data directory in project root
_default_data_dir = _project_root / "data"
# Fallback to package directory if project root doesn't exist
if not _default_data_dir.exists() and not (_project_root / "pyproject.toml").exists():
    _default_data_dir = _package_dir / "data"

data_json_init = {
    "ic256_45": "10.11.25.67",
    "tx2": "10.11.25.202",
    "save_path": str(_default_data_dir),
    "sampling_rate": 3000,
}


def _load_config() -> dict:
    """Load configuration from file, creating default if needed.
    
    Returns:
        Configuration dictionary
    """
    config = data_json_init.copy()
    
    try:
        if file_path.exists():
            with open(file_path, "r") as file:
                data = json.load(file)
                if data:  # Only update if data is not empty
                    config.update(data)
                else:
                    # Empty file, write defaults
                    _save_config(config)
        else:
            # File doesn't exist, create with defaults
            _save_config(config)
    except (json.JSONDecodeError, IOError, OSError):
        # Invalid JSON or I/O error, try to write defaults
        try:
            _save_config(config)
        except (IOError, OSError):
            pass  # If we can't write, continue with defaults
    
    return config


def _save_config(config: dict) -> None:
    """Save configuration to file.
    
    Args:
        config: Configuration dictionary to save
    """
    try:
        with open(file_path, "w") as file:
            json.dump(config, file, indent=4)
    except (IOError, OSError):
        pass  # Silently fail if we can't write


def init_ip(
    ix256_a_entry: tk.Entry,
    tx2_entry: tk.Entry,
    path_entry: tk.Entry,
    sampling_entry: tk.Entry,
) -> None:
    """Initialize IP addresses and settings from config file.
    
    Args:
        ix256_a_entry: IC256 device IP entry widget
        tx2_entry: TX2 device IP entry widget
        path_entry: Save path entry widget
        sampling_entry: Sampling rate entry widget
    """
    config = _load_config()
    
    # Initialize entry widgets
    ix256_a_entry.insert(0, str(config['ic256_45']))
    tx2_entry.insert(0, str(config['tx2']))
    
    # Init save path (readonly mode)
    path_entry.config(state="normal")
    path_entry.insert(0, str(config['save_path']))
    path_entry.config(state="readonly")
    
    sampling_entry.insert(0, str(config['sampling_rate']))


def update_file_json(
    ix256_a_entry: tk.Entry,
    tx2_entry: tk.Entry,
    path_entry: tk.Entry,
    sampling_entry: tk.Entry,
) -> None:
    """Update configuration file with current GUI values.
    
    Args:
        ix256_a_entry: IC256 device IP entry widget
        tx2_entry: TX2 device IP entry widget
        path_entry: Save path entry widget
        sampling_entry: Sampling rate entry widget
    """
    config = _load_config()
    
    # Update values if valid, otherwise keep existing
    ic256_value = ix256_a_entry.get()
    if ic256_value and is_valid_ipv4(ic256_value):
        config["ic256_45"] = ic256_value
    
    tx2_value = tx2_entry.get()
    if tx2_value and is_valid_ipv4(tx2_value):
        config["tx2"] = tx2_value
    
    path_value = path_entry.get()
    if path_value:
        config["save_path"] = path_value
    
    sampling_value = sampling_entry.get()
    if sampling_value:
        config["sampling_rate"] = sampling_value
    
    _save_config(config)
