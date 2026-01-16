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
    try:
        # Read file JSON
        with open(file_path, "r") as file:
            data = json.load(file)

        if data == {}:
            with open(file_path, "w") as file:
                json.dump(data_json_init, file, indent=4)

        data_json_init.update(data)
    except (FileNotFoundError, json.JSONDecodeError, IOError, OSError):
        # Config file doesn't exist or is invalid, create default
        try:
            with open(file_path, "w") as file:
                json.dump(data_json_init, file, indent=4)
        except (IOError, OSError):
            # If we can't write the config file, continue with defaults
            pass

    # Init value IC256-45 IP
    ix256_a_entry.insert(0, f"{data_json_init['ic256_45']}")

    # Init value TX2 IP
    tx2_entry.insert(0, f"{data_json_init['tx2']}")

    # Init save path (readonly mode)
    path_entry.config(state="normal")
    path_entry.insert(0, f"{data_json_init['save_path']}")
    path_entry.config(state="readonly")

    # Init sampling rate
    sampling_entry.insert(0, f"{data_json_init['sampling_rate']}")


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
    try:
        # Read file JSON
        with open(file_path, "r") as file:
            data = json.load(file)

        # Update data
        data["ic256_45"] = (
            ix256_a_entry.get() if is_valid_ipv4(ix256_a_entry.get()) else data["ic256_45"]
        )

        data["tx2"] = tx2_entry.get() if is_valid_ipv4(tx2_entry.get()) else data["tx2"]
        data["save_path"] = path_entry.get() if path_entry.get() else data["save_path"]

        data["sampling_rate"] = (
            sampling_entry.get() if sampling_entry.get() else data["sampling_rate"]
        )

        # Write to file JSON
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
    except (FileNotFoundError, json.JSONDecodeError, IOError, OSError) as e:
        # Log error but don't crash - user can retry
        print(f"Warning: Failed to update config file: {str(e)}")
