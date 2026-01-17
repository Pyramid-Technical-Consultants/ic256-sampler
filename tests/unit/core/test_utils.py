"""Tests for utility functions."""

import pytest
from unittest.mock import patch, Mock
from ic256_sampler.utils import is_valid_ipv4, is_valid_device


class TestIsValidIPv4:
    """Test IPv4 validation function."""

    def test_valid_ipv4_addresses(self):
        """Test valid IPv4 addresses."""
        assert is_valid_ipv4("192.168.1.1") is True
        assert is_valid_ipv4("10.0.0.1") is True
        assert is_valid_ipv4("255.255.255.255") is True
        assert is_valid_ipv4("0.0.0.0") is True
        assert is_valid_ipv4("127.0.0.1") is True
        assert is_valid_ipv4("172.16.0.1") is True

    def test_invalid_ipv4_addresses(self):
        """Test invalid IPv4 addresses."""
        assert is_valid_ipv4("invalid") is False
        assert is_valid_ipv4("256.1.1.1") is False
        assert is_valid_ipv4("192.168.1") is False
        assert is_valid_ipv4("") is False
        assert is_valid_ipv4("192.168.1.1.1") is False
        assert is_valid_ipv4("192.168.1.-1") is False
        assert is_valid_ipv4("192.168.1.256") is False
        assert is_valid_ipv4("::1") is False  # IPv6
        assert is_valid_ipv4("localhost") is False

    def test_edge_cases(self):
        """Test edge cases."""
        assert is_valid_ipv4("0.0.0.0") is True
        assert is_valid_ipv4("255.255.255.255") is True
        assert is_valid_ipv4("1.1.1.1") is True


class TestIsValidDevice:
    """Test device validation function."""

    def test_invalid_ip_returns_false(self):
        """Test that invalid IP addresses return False."""
        assert is_valid_device("invalid", "IC256") is False
        assert is_valid_device("", "IC256") is False
        assert is_valid_device("256.1.1.1", "IC256") is False

    @patch("ic256_sampler.utils.requests.get")
    def test_ic256_device_validation_success(self, mock_get):
        """Test successful IC256 device validation."""
        # Mock successful response with IC256 device
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '"IC256-45"'
        mock_get.return_value = mock_response

        assert is_valid_device("10.11.25.67", "IC256") is True
        assert is_valid_device("10.11.25.67", "IC256-45") is True
        # Function is called twice (once for each assertion)
        assert mock_get.call_count == 2

    @patch("ic256_sampler.utils.requests.get")
    def test_ic256_device_validation_failure(self, mock_get):
        """Test IC256 device validation failure."""
        # Mock response with wrong device
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '"TX2"'
        mock_get.return_value = mock_response

        assert is_valid_device("10.11.25.67", "IC256") is False

    @patch("ic256_sampler.utils.requests.get")
    def test_tx2_device_validation_success(self, mock_get):
        """Test successful TX2 device validation."""
        # Mock successful response with TX2 device
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '"TX2"'
        mock_get.return_value = mock_response

        assert is_valid_device("10.11.25.202", "TX2") is True

    @patch("ic256_sampler.utils.requests.get")
    def test_tx2_device_validation_failure(self, mock_get):
        """Test TX2 device validation failure."""
        # Mock response with wrong device
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '"IC256-45"'
        mock_get.return_value = mock_response

        assert is_valid_device("10.11.25.67", "TX2") is False

    @patch("ic256_sampler.utils.requests.get")
    def test_http_error_returns_false(self, mock_get):
        """Test that HTTP errors return False."""
        # Mock HTTP error
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        assert is_valid_device("10.11.25.67", "IC256") is False

    @patch("ic256_sampler.utils.requests.get")
    def test_connection_error_returns_false(self, mock_get):
        """Test that connection errors return False."""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError()

        assert is_valid_device("10.11.25.67", "IC256") is False

    @patch("ic256_sampler.utils.requests.get")
    def test_timeout_error_returns_false(self, mock_get):
        """Test that timeout errors return False."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout()

        assert is_valid_device("10.11.25.67", "IC256") is False

    @patch("ic256_sampler.utils.requests.get")
    def test_case_insensitive_device_matching(self, mock_get):
        """Test that device matching is case insensitive."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '"ic256-45"'  # Lowercase
        mock_get.return_value = mock_response

        assert is_valid_device("10.11.25.67", "IC256") is True
        assert is_valid_device("10.11.25.67", "ic256") is True
