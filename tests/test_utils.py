"""Tests for utility functions."""

import pytest
from ic256_sampler.utils import is_valid_ipv4, is_valid_device


class TestUtils:
    """Test utility functions."""

    def test_is_valid_ipv4_valid(self):
        """Test valid IPv4 addresses."""
        assert is_valid_ipv4("192.168.1.1") is True
        assert is_valid_ipv4("10.0.0.1") is True
        assert is_valid_ipv4("255.255.255.255") is True

    def test_is_valid_ipv4_invalid(self):
        """Test invalid IPv4 addresses."""
        assert is_valid_ipv4("invalid") is False
        assert is_valid_ipv4("256.1.1.1") is False
        assert is_valid_ipv4("192.168.1") is False
        assert is_valid_ipv4("") is False

    # Note: is_valid_device requires network access and device availability
    # These tests would need mocking or actual device access
    # def test_is_valid_device(self):
    #     """Test device validation."""
    #     pass
