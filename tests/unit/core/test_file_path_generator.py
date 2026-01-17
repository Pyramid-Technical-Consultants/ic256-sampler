"""Unit tests for file_path_generator module."""

import pytest
from unittest.mock import Mock, patch
from ic256_sampler.file_path_generator import (
    generate_file_path,
    get_file_path_for_primary_device,
)
from ic256_sampler.device_manager import IC256_CONFIG, TX2_CONFIG


class TestGenerateFilePath:
    """Tests for generate_file_path function."""
    
    def test_generate_file_path_ic256(self):
        """Test generate_file_path creates path for IC256 device."""
        path = generate_file_path(
            save_folder="/test/data",
            device_config=IC256_CONFIG,
            date="20240101",
            time_str="120000"
        )
        assert path == f"/test/data/{IC256_CONFIG.filename_prefix}-20240101-120000.csv"
    
    def test_generate_file_path_tx2(self):
        """Test generate_file_path creates path for TX2 device."""
        path = generate_file_path(
            save_folder="/test/data",
            device_config=TX2_CONFIG,
            date="20240101",
            time_str="120000"
        )
        assert path == f"/test/data/{TX2_CONFIG.filename_prefix}-20240101-120000.csv"
    
    def test_generate_file_path_with_trailing_slash(self):
        """Test generate_file_path handles trailing slash in save_folder."""
        path = generate_file_path(
            save_folder="/test/data/",
            device_config=IC256_CONFIG,
            date="20240101",
            time_str="120000"
        )
        # Should handle trailing slash correctly
        assert "20240101-120000.csv" in path
    
    def test_generate_file_path_various_dates(self):
        """Test generate_file_path with various date/time formats."""
        test_cases = [
            ("20240101", "120000"),
            ("20241231", "235959"),
            ("20240101", "000000"),
        ]
        
        for date, time_str in test_cases:
            path = generate_file_path(
                save_folder="/test",
                device_config=IC256_CONFIG,
                date=date,
                time_str=time_str
            )
            assert date in path
            assert time_str in path
            assert path.endswith(".csv")


class TestGetFilePathForPrimaryDevice:
    """Tests for get_file_path_for_primary_device function."""
    
    @patch('ic256_sampler.file_path_generator.get_timestamp_strings')
    def test_get_file_path_ic256_primary(self, mock_timestamp):
        """Test get_file_path_for_primary_device uses IC256 when available."""
        mock_timestamp.return_value = ("20240101", "120000")
        
        devices_added = [IC256_CONFIG.device_name]
        file_path, device_config = get_file_path_for_primary_device(
            "/test/data",
            devices_added
        )
        
        assert device_config == IC256_CONFIG
        assert IC256_CONFIG.filename_prefix in file_path
        assert file_path.endswith(".csv")
        mock_timestamp.assert_called_once()
    
    @patch('ic256_sampler.file_path_generator.get_timestamp_strings')
    def test_get_file_path_tx2_primary_when_no_ic256(self, mock_timestamp):
        """Test get_file_path_for_primary_device uses TX2 when IC256 not available."""
        mock_timestamp.return_value = ("20240101", "120000")
        
        devices_added = [TX2_CONFIG.device_name]
        file_path, device_config = get_file_path_for_primary_device(
            "/test/data",
            devices_added
        )
        
        assert device_config == TX2_CONFIG
        assert TX2_CONFIG.filename_prefix in file_path
        assert file_path.endswith(".csv")
    
    @patch('ic256_sampler.file_path_generator.get_timestamp_strings')
    def test_get_file_path_ic256_preferred_over_tx2(self, mock_timestamp):
        """Test get_file_path_for_primary_device prefers IC256 over TX2."""
        mock_timestamp.return_value = ("20240101", "120000")
        
        devices_added = [IC256_CONFIG.device_name, TX2_CONFIG.device_name]
        file_path, device_config = get_file_path_for_primary_device(
            "/test/data",
            devices_added
        )
        
        assert device_config == IC256_CONFIG
        assert IC256_CONFIG.filename_prefix in file_path
    
    @patch('ic256_sampler.file_path_generator.get_timestamp_strings')
    def test_get_file_path_includes_timestamp(self, mock_timestamp):
        """Test get_file_path_for_primary_device includes timestamp in path."""
        mock_timestamp.return_value = ("20240101", "120000")
        
        devices_added = [IC256_CONFIG.device_name]
        file_path, _ = get_file_path_for_primary_device(
            "/test/data",
            devices_added
        )
        
        assert "20240101" in file_path
        assert "120000" in file_path
    
    @patch('ic256_sampler.file_path_generator.get_timestamp_strings')
    def test_get_file_path_empty_devices_list(self, mock_timestamp):
        """Test get_file_path_for_primary_device handles empty devices list."""
        mock_timestamp.return_value = ("20240101", "120000")
        
        devices_added = []
        file_path, device_config = get_file_path_for_primary_device(
            "/test/data",
            devices_added
        )
        
        # Should default to TX2 when no devices
        assert device_config == TX2_CONFIG
        assert file_path.endswith(".csv")
