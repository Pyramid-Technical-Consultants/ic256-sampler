"""Tests for device path configuration."""

import pytest
from ic256_sampler.device_paths import (
    IC256_45_PATHS,
    TX2_PATHS,
    ADMIN_PATHS,
    get_ic256_45_path,
    get_tx2_path,
    get_admin_path,
    build_http_url,
    IC256_45_DEVICE_NAME,
    TX2_DEVICE_NAME,
)


class TestDevicePaths:
    """Test device path constants."""

    def test_ic256_45_paths_structure(self):
        """Test IC256-45 paths have correct structure."""
        assert "adc" in IC256_45_PATHS
        assert "single_dose_module" in IC256_45_PATHS
        assert "environmental_sensor" in IC256_45_PATHS
        assert "io" in IC256_45_PATHS

    def test_ic256_45_adc_paths(self):
        """Test IC256-45 ADC paths."""
        adc_paths = IC256_45_PATHS["adc"]
        assert "primary_dose" in adc_paths
        assert "channel_sum" in adc_paths
        assert "gaussian_fit_a_mean" in adc_paths
        assert "gaussian_fit_a_sigma" in adc_paths
        assert "gaussian_fit_b_mean" in adc_paths
        assert "gaussian_fit_b_sigma" in adc_paths
        assert "integration_frequency" in adc_paths
        assert "gate_signal" in adc_paths
        assert "sample_frequency" in adc_paths

    def test_ic256_45_paths_contain_device_name(self):
        """Test that IC256-45 paths contain device name."""
        for category, paths in IC256_45_PATHS.items():
            if isinstance(paths, dict):
                for path in paths.values():
                    assert IC256_45_DEVICE_NAME in path or "/io/" in path

    def test_tx2_paths_structure(self):
        """Test TX2 paths have correct structure."""
        assert "adc" in TX2_PATHS

    def test_tx2_adc_paths(self):
        """Test TX2 ADC paths."""
        adc_paths = TX2_PATHS["adc"]
        assert "channel_5" in adc_paths
        assert "channel_1" in adc_paths
        assert "fr2" in adc_paths
        assert "channel_5_units" in adc_paths
        assert "conversion_frequency" in adc_paths
        assert "sample_frequency" in adc_paths

    def test_tx2_paths_contain_device_name(self):
        """Test that TX2 paths contain device name."""
        for category, paths in TX2_PATHS.items():
            if isinstance(paths, dict):
                for path in paths.values():
                    assert TX2_DEVICE_NAME in path

    def test_admin_paths(self):
        """Test admin paths."""
        assert "device_type" in ADMIN_PATHS
        assert ADMIN_PATHS["device_type"] == "/io/admin/device_type/value.json"


class TestPathHelperFunctions:
    """Test path helper functions."""

    def test_get_ic256_45_path(self):
        """Test getting IC256-45 path by category and key."""
        path = get_ic256_45_path("adc", "primary_dose")
        assert isinstance(path, str)
        assert IC256_45_DEVICE_NAME in path
        assert "dose" in path or "channel" in path

    def test_get_ic256_45_path_invalid_category(self):
        """Test getting IC256-45 path with invalid category raises error."""
        with pytest.raises(KeyError):
            get_ic256_45_path("invalid_category", "key")

    def test_get_ic256_45_path_invalid_key(self):
        """Test getting IC256-45 path with invalid key raises error."""
        with pytest.raises(KeyError):
            get_ic256_45_path("adc", "invalid_key")

    def test_get_tx2_path(self):
        """Test getting TX2 path by category and key."""
        path = get_tx2_path("adc", "channel_5")
        assert isinstance(path, str)
        assert TX2_DEVICE_NAME in path
        assert "channel_5" in path

    def test_get_tx2_path_invalid_category(self):
        """Test getting TX2 path with invalid category raises error."""
        with pytest.raises(KeyError):
            get_tx2_path("invalid_category", "key")

    def test_get_tx2_path_invalid_key(self):
        """Test getting TX2 path with invalid key raises error."""
        with pytest.raises(KeyError):
            get_tx2_path("adc", "invalid_key")

    def test_get_admin_path(self):
        """Test getting admin path by key."""
        path = get_admin_path("device_type")
        assert isinstance(path, str)
        assert path == "/io/admin/device_type/value.json"

    def test_get_admin_path_invalid_key(self):
        """Test getting admin path with invalid key raises error."""
        with pytest.raises(KeyError):
            get_admin_path("invalid_key")

    def test_build_http_url(self):
        """Test building HTTP URL from IP and path."""
        ip = "10.11.25.67"
        path = "/io/admin/device_type/value.json"
        url = build_http_url(ip, path)
        assert url == f"http://{ip}{path}"
        assert url.startswith("http://")
        assert ip in url
        assert path in url

    def test_build_http_url_various_ips(self):
        """Test building HTTP URL with various IP addresses."""
        test_cases = [
            ("192.168.1.1", "/path/to/resource"),
            ("10.0.0.1", "/test"),
            ("127.0.0.1", "/local"),
        ]
        for ip, path in test_cases:
            url = build_http_url(ip, path)
            assert url == f"http://{ip}{path}"
