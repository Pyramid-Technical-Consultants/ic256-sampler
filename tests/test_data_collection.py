"""Tests for data collection functions."""

import pytest
from collections import deque
from ic256_sampler.data_collection import (
    convert_mean,
    convert_sigma,
    process_gaussian_values,
    get_headers,
    _get_time_bin,
    _SortedBufferCache,
    ERROR_VALUE,
    ERROR_GAUSS,
    X_STRIP_OFFSET,
    Y_STRIP_OFFSET,
    MEAN_OFFSET,
    TIME_BIN_SIZE,
    DataPoint,
    ChannelBuffer,
)


class TestConvertMean:
    """Test mean value conversion."""

    def test_convert_mean_valid_x_axis(self):
        """Test converting valid mean value for X axis."""
        # Test with standard offset calculation
        value = 128.5  # Should result in 0.0
        result = convert_mean(value, x_axis=True)
        assert result == 0.0

        # Test with value above offset
        value = 129.5
        expected = (129.5 - MEAN_OFFSET) * X_STRIP_OFFSET
        result = convert_mean(value, x_axis=True)
        assert abs(result - expected) < 0.001

    def test_convert_mean_valid_y_axis(self):
        """Test converting valid mean value for Y axis."""
        value = 128.5  # Should result in 0.0
        result = convert_mean(value, x_axis=False)
        assert result == 0.0

        # Test with value above offset
        value = 129.5
        expected = (129.5 - MEAN_OFFSET) * Y_STRIP_OFFSET
        result = convert_mean(value, x_axis=False)
        assert abs(result - expected) < 0.001

    def test_convert_mean_invalid_values(self):
        """Test converting invalid mean values."""
        assert convert_mean(None) == ERROR_GAUSS
        assert convert_mean(ERROR_VALUE) == ERROR_GAUSS
        assert convert_mean("") == ERROR_GAUSS
        assert convert_mean("invalid") == ERROR_GAUSS

    def test_convert_mean_numeric_types(self):
        """Test converting different numeric types."""
        # Test with int
        result_int = convert_mean(128, x_axis=True)
        result_float = convert_mean(128.0, x_axis=True)
        assert abs(result_int - result_float) < 0.001

        # Test with string number
        result_str = convert_mean("128.5", x_axis=True)
        assert result_str == 0.0


class TestConvertSigma:
    """Test sigma value conversion."""

    def test_convert_sigma_valid_x_axis(self):
        """Test converting valid sigma value for X axis."""
        value = 1.0
        expected = 1.0 * X_STRIP_OFFSET
        result = convert_sigma(value, x_axis=True)
        assert abs(result - expected) < 0.001

    def test_convert_sigma_valid_y_axis(self):
        """Test converting valid sigma value for Y axis."""
        value = 1.0
        expected = 1.0 * Y_STRIP_OFFSET
        result = convert_sigma(value, x_axis=False)
        assert abs(result - expected) < 0.001

    def test_convert_sigma_invalid_values(self):
        """Test converting invalid sigma values."""
        assert convert_sigma(None) == ERROR_GAUSS
        assert convert_sigma(ERROR_VALUE) == ERROR_GAUSS
        assert convert_sigma("") == ERROR_GAUSS
        assert convert_sigma("invalid") == ERROR_GAUSS

    def test_convert_sigma_zero(self):
        """Test converting zero sigma value."""
        result = convert_sigma(0.0, x_axis=True)
        assert result == 0.0


class TestProcessGaussianValues:
    """Test gaussian value processing."""

    def test_process_gaussian_values_valid(self):
        """Test processing valid gaussian values."""
        x_mean, x_sigma, y_mean, y_sigma = process_gaussian_values(
            128.5, 1.0, 128.5, 1.0
        )
        assert x_mean == 0.0
        assert abs(x_sigma - X_STRIP_OFFSET) < 0.001
        assert y_mean == 0.0
        assert abs(y_sigma - Y_STRIP_OFFSET) < 0.001

    def test_process_gaussian_values_invalid(self):
        """Test processing invalid gaussian values."""
        x_mean, x_sigma, y_mean, y_sigma = process_gaussian_values(
            None, None, None, None
        )
        assert x_mean == ERROR_GAUSS
        assert x_sigma == ERROR_GAUSS
        assert y_mean == ERROR_GAUSS
        assert y_sigma == ERROR_GAUSS

    def test_process_gaussian_values_mixed(self):
        """Test processing mixed valid/invalid values."""
        x_mean, x_sigma, y_mean, y_sigma = process_gaussian_values(
            128.5, None, None, 1.0
        )
        assert x_mean == 0.0
        assert x_sigma == ERROR_GAUSS
        assert y_mean == ERROR_GAUSS
        assert abs(y_sigma - Y_STRIP_OFFSET) < 0.001


class TestGetHeaders:
    """Test CSV header generation."""

    def test_get_headers_ic256(self):
        """Test getting headers for IC256 device."""
        headers = get_headers("ic256_45", primary_units="Gy")
        assert "Timestamp (s)" in headers
        assert "X centroid (mm)" in headers
        assert "X sigma (mm)" in headers
        assert "Y centroid (mm)" in headers
        assert "Y sigma (mm)" in headers
        assert "Dose (Gy)" in headers
        assert "Channel Sum" in headers
        assert "External trigger" in headers
        assert "Temperature (℃)" in headers
        assert "Humidity (%rH)" in headers
        assert "Pressure (hPa)" in headers
        assert "Note" in headers

    def test_get_headers_ic256_case_insensitive(self):
        """Test that IC256 header generation is case insensitive."""
        headers1 = get_headers("IC256_45", primary_units="Gy")
        headers2 = get_headers("ic256_45", primary_units="Gy")
        assert headers1 == headers2

    def test_get_headers_tx2(self):
        """Test getting headers for TX2 device."""
        headers = get_headers("tx2", probe_units="V")
        assert "Timestamp (s)" in headers
        assert "Probe A (V)" in headers
        assert "Probe B (V)" in headers
        assert "FR2" in headers
        assert "Note" in headers

    def test_get_headers_unknown_device(self):
        """Test getting headers for unknown device."""
        headers = get_headers("unknown_device")
        assert headers == ["Timestamp (s)"]

    def test_get_headers_empty_units(self):
        """Test getting headers with empty units."""
        headers = get_headers("ic256_45", primary_units="")
        assert "Dose ()" in headers


class TestGetTimeBin:
    """Test time binning function."""

    def test_get_time_bin_rounds_correctly(self):
        """Test that time binning rounds to nearest bin."""
        # Test exact bin values
        assert _get_time_bin(0.0) == 0.0
        assert _get_time_bin(TIME_BIN_SIZE) == TIME_BIN_SIZE
        assert _get_time_bin(TIME_BIN_SIZE * 2) == TIME_BIN_SIZE * 2

    def test_get_time_bin_rounds_up(self):
        """Test that time binning rounds up correctly."""
        # Test values just above bin boundary
        result = _get_time_bin(TIME_BIN_SIZE / 2 + 0.001)
        assert result == TIME_BIN_SIZE

    def test_get_time_bin_rounds_down(self):
        """Test that time binning rounds down correctly."""
        # Test values just below bin boundary
        result = _get_time_bin(TIME_BIN_SIZE / 2 - 0.001)
        assert result == 0.0

    def test_get_time_bin_negative(self):
        """Test time binning with negative values."""
        # Small negative values round to 0.0 (nearest bin)
        result = _get_time_bin(-0.001)
        assert result == 0.0
        
        # Larger negative values should round to negative bins
        result = _get_time_bin(-TIME_BIN_SIZE - 0.001)
        assert result < 0


class TestSortedBufferCache:
    """Test sorted buffer cache."""

    def test_cache_initialization(self):
        """Test cache initialization."""
        cache = _SortedBufferCache()
        assert cache.sorted_data is None
        assert cache.last_size == 0
        assert cache.channel_name is None

    def test_cache_hit_same_channel_and_size(self):
        """Test cache hit when channel and size match."""
        cache = _SortedBufferCache()
        buffer: ChannelBuffer = deque([
            (1.0, 0.1, 1000000),
            (2.0, 0.2, 2000000),
            (3.0, 0.3, 3000000),
        ])

        # First call - should sort and cache
        result1 = cache.get_sorted("channel1", buffer)
        assert len(result1) == 3
        assert cache.sorted_data is not None
        assert cache.last_size == 3
        assert cache.channel_name == "channel1"

        # Second call with same channel and size - should use cache
        result2 = cache.get_sorted("channel1", buffer)
        assert result1 is result2  # Same object reference

    def test_cache_miss_different_channel(self):
        """Test cache miss when channel changes."""
        cache = _SortedBufferCache()
        buffer: ChannelBuffer = deque([
            (1.0, 0.1, 1000000),
            (2.0, 0.2, 2000000),
        ])

        cache.get_sorted("channel1", buffer)
        assert cache.channel_name == "channel1"

        # Different channel - should miss cache
        result = cache.get_sorted("channel2", buffer)
        assert cache.channel_name == "channel2"
        assert len(result) == 2

    def test_cache_miss_different_size(self):
        """Test cache miss when buffer size changes."""
        cache = _SortedBufferCache()
        buffer1: ChannelBuffer = deque([
            (1.0, 0.1, 1000000),
            (2.0, 0.2, 2000000),
        ])
        buffer2: ChannelBuffer = deque([
            (1.0, 0.1, 1000000),
            (2.0, 0.2, 2000000),
            (3.0, 0.3, 3000000),
        ])

        cache.get_sorted("channel1", buffer1)
        assert cache.last_size == 2

        # Different size - should miss cache
        result = cache.get_sorted("channel1", buffer2)
        assert cache.last_size == 3
        assert len(result) == 3

    def test_cache_sorts_by_elapsed_time(self):
        """Test that cache sorts data by elapsed time."""
        cache = _SortedBufferCache()
        buffer: ChannelBuffer = deque([
            (3.0, 0.3, 3000000),
            (1.0, 0.1, 1000000),
            (2.0, 0.2, 2000000),
        ])

        result = cache.get_sorted("channel1", buffer)
        assert result[0][1] == 0.1  # Sorted by elapsed time
        assert result[1][1] == 0.2
        assert result[2][1] == 0.3
