"""Tests for configuration management."""

import pytest
import json
import tempfile
import pathlib
from unittest.mock import Mock, patch, mock_open
from ic256_sampler.config import (
    init_ip,
    update_file_json,
    file_path,
    data_json_init,
)


class TestConfigFunctions:
    """Test configuration functions."""

    def test_init_ip_with_valid_config_file(self, tmp_path):
        """Test initializing IP addresses from valid config file."""
        # Create temporary config file
        config_file = tmp_path / "config.json"
        config_data = {
            "ic256_45": "192.168.1.100",
            "tx2": "192.168.1.200",
            "save_path": str(tmp_path / "data"),
            "sampling_rate": 5000,
        }
        config_file.write_text(json.dumps(config_data))

        # Mock tkinter Entry widgets
        ic256_entry = Mock()
        ic256_entry.insert = Mock()
        tx2_entry = Mock()
        tx2_entry.insert = Mock()
        path_entry = Mock()
        path_entry.insert = Mock()
        path_entry.config = Mock()
        sampling_entry = Mock()
        sampling_entry.insert = Mock()

        # Patch file_path to use temp file
        with patch("ic256_sampler.config.file_path", config_file):
            init_ip(ic256_entry, tx2_entry, path_entry, sampling_entry)

        # Verify entries were populated
        ic256_entry.insert.assert_called_once_with(0, "192.168.1.100")
        tx2_entry.insert.assert_called_once_with(0, "192.168.1.200")
        path_entry.insert.assert_called_once_with(0, str(tmp_path / "data"))
        sampling_entry.insert.assert_called_once_with(0, "5000")

    def test_init_ip_with_missing_config_file(self, tmp_path):
        """Test initializing IP addresses when config file doesn't exist."""
        # Use non-existent config file
        config_file = tmp_path / "nonexistent.json"

        # Mock tkinter Entry widgets
        ic256_entry = Mock()
        ic256_entry.insert = Mock()
        tx2_entry = Mock()
        tx2_entry.insert = Mock()
        path_entry = Mock()
        path_entry.insert = Mock()
        path_entry.config = Mock()
        sampling_entry = Mock()
        sampling_entry.insert = Mock()

        # Patch file_path to use temp file
        with patch("ic256_sampler.config.file_path", config_file):
            init_ip(ic256_entry, tx2_entry, path_entry, sampling_entry)

        # Verify default values were used
        ic256_entry.insert.assert_called_once()
        tx2_entry.insert.assert_called_once()
        path_entry.insert.assert_called_once()
        sampling_entry.insert.assert_called_once()

        # Verify config file was created
        assert config_file.exists()
        with open(config_file) as f:
            created_data = json.load(f)
            assert "ic256_45" in created_data
            assert "tx2" in created_data

    def test_init_ip_with_empty_config_file(self, tmp_path):
        """Test initializing IP addresses with empty config file."""
        config_file = tmp_path / "config.json"
        config_file.write_text("{}")

        # Mock tkinter Entry widgets
        ic256_entry = Mock()
        ic256_entry.insert = Mock()
        tx2_entry = Mock()
        tx2_entry.insert = Mock()
        path_entry = Mock()
        path_entry.insert = Mock()
        path_entry.config = Mock()
        sampling_entry = Mock()
        sampling_entry.insert = Mock()

        with patch("ic256_sampler.config.file_path", config_file):
            init_ip(ic256_entry, tx2_entry, path_entry, sampling_entry)

        # Verify entries were populated with defaults
        ic256_entry.insert.assert_called_once()
        tx2_entry.insert.assert_called_once()

    def test_init_ip_with_invalid_json(self, tmp_path):
        """Test initializing IP addresses with invalid JSON."""
        config_file = tmp_path / "config.json"
        config_file.write_text("{ invalid json }")

        # Mock tkinter Entry widgets
        ic256_entry = Mock()
        ic256_entry.insert = Mock()
        tx2_entry = Mock()
        tx2_entry.insert = Mock()
        path_entry = Mock()
        path_entry.insert = Mock()
        path_entry.config = Mock()
        sampling_entry = Mock()
        sampling_entry.insert = Mock()

        with patch("ic256_sampler.config.file_path", config_file):
            init_ip(ic256_entry, tx2_entry, path_entry, sampling_entry)

        # Should still populate with defaults
        ic256_entry.insert.assert_called_once()
        tx2_entry.insert.assert_called_once()

    def test_update_file_json_valid(self, tmp_path):
        """Test updating config file with valid data."""
        config_file = tmp_path / "config.json"
        initial_data = {
            "ic256_45": "10.11.25.67",
            "tx2": "10.11.25.202",
            "save_path": str(tmp_path / "data"),
            "sampling_rate": 3000,
        }
        config_file.write_text(json.dumps(initial_data))

        # Mock tkinter Entry widgets with new values
        ic256_entry = Mock()
        ic256_entry.get = Mock(return_value="192.168.1.100")
        tx2_entry = Mock()
        tx2_entry.get = Mock(return_value="192.168.1.200")
        path_entry = Mock()
        path_entry.get = Mock(return_value=str(tmp_path / "new_data"))
        sampling_entry = Mock()
        sampling_entry.get = Mock(return_value="5000")

        with patch("ic256_sampler.config.file_path", config_file):
            with patch("ic256_sampler.config.is_valid_ipv4", return_value=True):
                update_file_json(ic256_entry, tx2_entry, path_entry, sampling_entry)

        # Verify file was updated
        with open(config_file) as f:
            updated_data = json.load(f)
            assert updated_data["ic256_45"] == "192.168.1.100"
            assert updated_data["tx2"] == "192.168.1.200"
            assert updated_data["save_path"] == str(tmp_path / "new_data")
            assert updated_data["sampling_rate"] == "5000"

    def test_update_file_json_invalid_ip(self, tmp_path):
        """Test updating config file with invalid IP addresses."""
        config_file = tmp_path / "config.json"
        initial_data = {
            "ic256_45": "10.11.25.67",
            "tx2": "10.11.25.202",
            "save_path": str(tmp_path / "data"),
            "sampling_rate": 3000,
        }
        config_file.write_text(json.dumps(initial_data))

        # Mock tkinter Entry widgets with invalid IP
        ic256_entry = Mock()
        ic256_entry.get = Mock(return_value="invalid_ip")
        tx2_entry = Mock()
        tx2_entry.get = Mock(return_value="192.168.1.200")
        path_entry = Mock()
        path_entry.get = Mock(return_value=str(tmp_path / "data"))
        sampling_entry = Mock()
        sampling_entry.get = Mock(return_value="3000")

        with patch("ic256_sampler.config.file_path", config_file):
            with patch("ic256_sampler.config.is_valid_ipv4") as mock_valid:
                def side_effect(ip):
                    return ip == "192.168.1.200"
                mock_valid.side_effect = side_effect

                update_file_json(ic256_entry, tx2_entry, path_entry, sampling_entry)

        # Verify invalid IP was not updated
        with open(config_file) as f:
            updated_data = json.load(f)
            assert updated_data["ic256_45"] == "10.11.25.67"  # Original value preserved
            assert updated_data["tx2"] == "192.168.1.200"  # Valid IP updated

    def test_update_file_json_empty_values(self, tmp_path):
        """Test updating config file with empty values."""
        config_file = tmp_path / "config.json"
        initial_data = {
            "ic256_45": "10.11.25.67",
            "tx2": "10.11.25.202",
            "save_path": str(tmp_path / "data"),
            "sampling_rate": 3000,
        }
        config_file.write_text(json.dumps(initial_data))

        # Mock tkinter Entry widgets with empty values
        ic256_entry = Mock()
        ic256_entry.get = Mock(return_value="")
        tx2_entry = Mock()
        tx2_entry.get = Mock(return_value="")
        path_entry = Mock()
        path_entry.get = Mock(return_value="")
        sampling_entry = Mock()
        sampling_entry.get = Mock(return_value="")

        with patch("ic256_sampler.config.file_path", config_file):
            update_file_json(ic256_entry, tx2_entry, path_entry, sampling_entry)

        # Verify original values were preserved
        with open(config_file) as f:
            updated_data = json.load(f)
            assert updated_data["ic256_45"] == "10.11.25.67"
            assert updated_data["tx2"] == "10.11.25.202"

    def test_update_file_json_missing_file(self, tmp_path):
        """Test updating config file when file doesn't exist."""
        config_file = tmp_path / "nonexistent.json"

        # Mock tkinter Entry widgets
        ic256_entry = Mock()
        ic256_entry.get = Mock(return_value="192.168.1.100")
        tx2_entry = Mock()
        tx2_entry.get = Mock(return_value="192.168.1.200")
        path_entry = Mock()
        path_entry.get = Mock(return_value=str(tmp_path / "data"))
        sampling_entry = Mock()
        sampling_entry.get = Mock(return_value="3000")

        with patch("ic256_sampler.config.file_path", config_file):
            with patch("ic256_sampler.config.is_valid_ipv4", return_value=True):
                # Should not raise exception, just print warning
                update_file_json(ic256_entry, tx2_entry, path_entry, sampling_entry)
