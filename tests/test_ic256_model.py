"""Tests for data collection functions.

Note: Many conversion functions have been moved to IC256Model.
These tests now test the IC256Model functions instead.
"""

import pytest
from ic256_sampler.ic256_model import (
    IC256Model,
    convert_mean_ic256,
    convert_sigma_ic256,
    MEAN_OFFSET,
    X_STRIP_OFFSET,
    Y_STRIP_OFFSET,
    ERROR_GAUSS,
)


class TestConvertMean:
    """Test mean value conversion using IC256Model."""

    def test_convert_mean_valid_x_axis(self):
        """Test converting valid mean value for X axis."""
        # Test with standard offset calculation
        value = 128.5  # Should result in 0.0
        result = convert_mean_ic256(value, x_axis=True)
        assert result == 0.0

        # Test with value above offset
        value = 129.5
        expected = (129.5 - MEAN_OFFSET) * X_STRIP_OFFSET
        result = convert_mean_ic256(value, x_axis=True)
        assert abs(result - expected) < 0.001

    def test_convert_mean_valid_y_axis(self):
        """Test converting valid mean value for Y axis."""
        value = 128.5  # Should result in 0.0
        result = convert_mean_ic256(value, x_axis=False)
        assert result == 0.0

        # Test with value above offset
        value = 129.5
        expected = (129.5 - MEAN_OFFSET) * Y_STRIP_OFFSET
        result = convert_mean_ic256(value, x_axis=False)
        assert abs(result - expected) < 0.001

    def test_convert_mean_invalid_values(self):
        """Test converting invalid mean values."""
        assert convert_mean_ic256(None) == ERROR_GAUSS
        assert convert_mean_ic256("") == ERROR_GAUSS
        assert convert_mean_ic256("invalid") == ERROR_GAUSS

    def test_convert_mean_numeric_types(self):
        """Test converting different numeric types."""
        # Test with int
        result_int = convert_mean_ic256(128, x_axis=True)
        result_float = convert_mean_ic256(128.0, x_axis=True)
        assert abs(result_int - result_float) < 0.001

        # Test with string number
        result_str = convert_mean_ic256("128.5", x_axis=True)
        assert result_str == 0.0


class TestConvertSigma:
    """Test sigma value conversion using IC256Model."""

    def test_convert_sigma_valid_x_axis(self):
        """Test converting valid sigma value for X axis."""
        value = 1.0
        expected = 1.0 * X_STRIP_OFFSET
        result = convert_sigma_ic256(value, x_axis=True)
        assert abs(result - expected) < 0.001

    def test_convert_sigma_valid_y_axis(self):
        """Test converting valid sigma value for Y axis."""
        value = 1.0
        expected = 1.0 * Y_STRIP_OFFSET
        result = convert_sigma_ic256(value, x_axis=False)
        assert abs(result - expected) < 0.001

    def test_convert_sigma_invalid_values(self):
        """Test converting invalid sigma values."""
        assert convert_sigma_ic256(None) == ERROR_GAUSS
        assert convert_sigma_ic256("") == ERROR_GAUSS
        assert convert_sigma_ic256("invalid") == ERROR_GAUSS

    def test_convert_sigma_zero(self):
        """Test converting zero sigma value."""
        result = convert_sigma_ic256(0.0, x_axis=True)
        assert result == 0.0


class TestIC256ModelConverters:
    """Test IC256Model converter methods."""

    def test_gaussian_x_mean_converter(self):
        """Test X mean converter from IC256Model."""
        converter = IC256Model.get_gaussian_x_mean_converter()
        value = 128.5
        result = converter(value)
        assert result == 0.0

    def test_gaussian_x_sigma_converter(self):
        """Test X sigma converter from IC256Model."""
        converter = IC256Model.get_gaussian_x_sigma_converter()
        value = 1.0
        result = converter(value)
        assert abs(result - X_STRIP_OFFSET) < 0.001

    def test_gaussian_y_mean_converter(self):
        """Test Y mean converter from IC256Model."""
        converter = IC256Model.get_gaussian_y_mean_converter()
        value = 128.5
        result = converter(value)
        assert result == 0.0

    def test_gaussian_y_sigma_converter(self):
        """Test Y sigma converter from IC256Model."""
        converter = IC256Model.get_gaussian_y_sigma_converter()
        value = 1.0
        result = converter(value)
        assert abs(result - Y_STRIP_OFFSET) < 0.001

    def test_create_columns(self):
        """Test creating IC256 column definitions."""
        from ic256_sampler.device_paths import IC256_45_PATHS
        
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        columns = IC256Model.create_columns(reference_channel)
        
        assert len(columns) > 0
        assert any(col.name == "X centroid (mm)" for col in columns)
        assert any(col.name == "Y centroid (mm)" for col in columns)
        assert any(col.name == "Timestamp (s)" for col in columns)
        assert any(col.name == "Note" for col in columns)
