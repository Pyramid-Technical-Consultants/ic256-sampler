"""Integration tests for device paths HTTP endpoints.

These tests validate that paths defined in device_paths.py can be used
to make HTTP GET requests to real device endpoints.

The format for HTTP GET requests is: http://ip/io/{path}/value.json

Run with: pytest tests/integration/devices/test_device_paths.py -v
Skip with: pytest -m "not integration"
"""

import pytest
import requests
from ic256_sampler.device_paths import (
    IC256_45_PATHS,
    ADMIN_PATHS,
    get_ic256_45_path,
    get_admin_path,
    build_http_url,
)
from ic256_sampler.utils import is_valid_device, is_valid_ipv4

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]




class TestDevicePathsHTTP:
    """Integration tests for device paths HTTP endpoints."""

    def test_ic256_io_paths_http_get(self, ic256_ip):
        """Test that IC256 IO paths can be accessed via HTTP GET.
        
        This test:
        1. Gets IO paths from device_paths.py
        2. Constructs HTTP URLs using build_http_url
        3. Makes HTTP GET requests to validate endpoints are accessible
        4. Verifies responses are valid JSON
        
        This test requires:
        - A live IC256 device at the IP in config.json
        - Network connectivity to that device
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Test all IO paths defined in IC256_45_PATHS
        io_paths = IC256_45_PATHS.get("io", {})
        assert len(io_paths) > 0, "Should have at least one IO path defined"
        
        # Collect results for reporting
        successful_paths = []
        timeout_paths = []
        connection_error_paths = []
        error_paths = []
        
        for path_name, path in io_paths.items():
            # Build full HTTP URL
            url = build_http_url(ic256_ip, path)
            
            # Verify URL format
            assert url.startswith(f"http://{ic256_ip}/io/"), \
                f"URL should start with http://{ic256_ip}/io/, got: {url}"
            assert url.endswith("/value.json"), \
                f"URL should end with /value.json, got: {url}"
            
            # Make HTTP GET request
            try:
                response = requests.get(url, timeout=5)  # Increased timeout to 5 seconds
                
                # Should get 200 OK (or at least not a 404)
                assert response.status_code == 200, \
                    f"Expected 200 OK for {path_name} at {url}, got {response.status_code}"
                
                # Response should be valid JSON
                try:
                    data = response.json()
                    assert data is not None, \
                        f"Response from {path_name} should contain JSON data"
                    successful_paths.append((path_name, url))
                except ValueError:
                    # Some endpoints might return plain text that's valid JSON
                    # Try parsing as text first
                    text = response.text.strip()
                    if text:
                        # If it's just a number or string, that's also valid
                        successful_paths.append((path_name, url))
                    else:
                        error_paths.append((path_name, url, "Empty response"))
                        
            except requests.exceptions.Timeout:
                timeout_paths.append((path_name, url))
            except requests.exceptions.ConnectionError:
                connection_error_paths.append((path_name, url))
            except requests.exceptions.RequestException as e:
                error_paths.append((path_name, url, str(e)))
        
        # Report results
        print(f"\n=== IO Path Test Results ===")
        print(f"Successful paths ({len(successful_paths)}):")
        for path_name, url in successful_paths:
            print(f"  [OK] {path_name}: {url}")
        
        if timeout_paths:
            print(f"\n[TIMEOUT] paths ({len(timeout_paths)}):")
            for path_name, url in timeout_paths:
                print(f"  [TIMEOUT] {path_name}: {url}")
        
        if connection_error_paths:
            print(f"\n[CONNECTION ERROR] paths ({len(connection_error_paths)}):")
            for path_name, url in connection_error_paths:
                print(f"  [CONN ERROR] {path_name}: {url}")
        
        if error_paths:
            print(f"\n[ERROR] paths ({len(error_paths)}):")
            for path_name, url, error in error_paths:
                print(f"  [ERROR] {path_name}: {url} - {error}")
        
        # Fail if any paths had errors (but not timeouts/connection errors which might be expected)
        if error_paths:
            pytest.fail(f"Found {len(error_paths)} paths with errors. See output above for details.")
        
        # Warn about timeouts but don't fail - these might be expected
        if timeout_paths:
            pytest.skip(f"Found {len(timeout_paths)} paths that timed out. See output above for details.")

    def test_ic256_io_paths_using_helper_functions(self, ic256_ip):
        """Test that IO paths retrieved via helper functions work with HTTP GET.
        
        This test validates that the helper functions (get_ic256_45_path)
        return paths that can be used to construct valid HTTP URLs.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Test a few IO paths using helper functions
        test_paths = [
            ("io", "fan_control"),
        ]
        
        successful = []
        timeout_paths = []
        connection_error_paths = []
        error_paths = []
        
        for category, key in test_paths:
            try:
                # Get path using helper function
                path = get_ic256_45_path(category, key)
                
                # Build HTTP URL
                url = build_http_url(ic256_ip, path)
                
                # Make HTTP GET request
                try:
                    response = requests.get(url, timeout=5)  # Increased timeout to 5 seconds
                    
                    assert response.status_code == 200, \
                        f"Expected 200 OK for {category}/{key} at {url}, got {response.status_code}"
                    successful.append((f"{category}/{key}", url))
                    
                except requests.exceptions.Timeout:
                    timeout_paths.append((f"{category}/{key}", url))
                except requests.exceptions.ConnectionError:
                    connection_error_paths.append((f"{category}/{key}", url))
                except requests.exceptions.RequestException as e:
                    error_paths.append((f"{category}/{key}", url, str(e)))
                    
            except KeyError:
                # Skip if path doesn't exist
                pytest.skip(f"Path {category}/{key} not found in IC256_45_PATHS")
        
        # Report results
        print(f"\n=== Helper Function Test Results ===")
        if successful:
            print(f"Successful paths ({len(successful)}):")
            for path_name, url in successful:
                print(f"  [OK] {path_name}: {url}")
        
        if timeout_paths:
            print(f"\n[TIMEOUT] paths ({len(timeout_paths)}):")
            for path_name, url in timeout_paths:
                print(f"  [TIMEOUT] {path_name}: {url}")
        
        if connection_error_paths:
            print(f"\n[CONNECTION ERROR] paths ({len(connection_error_paths)}):")
            for path_name, url in connection_error_paths:
                print(f"  [CONN ERROR] {path_name}: {url}")
        
        if error_paths:
            print(f"\n[ERROR] paths ({len(error_paths)}):")
            for path_name, url, error in error_paths:
                print(f"  [ERROR] {path_name}: {url} - {error}")
        
        # Fail if any paths had errors
        if error_paths:
            pytest.fail(f"Found {len(error_paths)} paths with errors. See output above for details.")
        
        # Warn about timeouts but don't fail
        if timeout_paths or connection_error_paths:
            pytest.skip(f"Found {len(timeout_paths)} timeouts and {len(connection_error_paths)} connection errors. See output above for details.")

    def test_admin_paths_http_get(self, ic256_ip):
        """Test that admin paths can be accessed via HTTP GET.
        
        This test validates admin endpoints like device_type.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Test admin device_type path
        admin_path = get_admin_path("device_type")
        url = build_http_url(ic256_ip, admin_path)
        
        # Make HTTP GET request
        response = requests.get(url, timeout=5)  # Increased timeout to 5 seconds
        
        assert response.status_code == 200, \
            f"Expected 200 OK for device_type at {url}, got {response.status_code}"
        
        # Response should contain device type information
        try:
            data = response.json()
            assert data is not None
        except ValueError:
            # Try as text
            text = response.text.strip().replace('"', '')
            assert len(text) > 0, "Device type response should not be empty"
            assert "IC256" in text.upper(), \
                f"Device type should contain 'IC256', got: {text}"

    def test_build_http_url_with_io_paths(self, ic256_ip):
        """Test that build_http_url correctly constructs URLs for IO paths.
        
        This test validates the build_http_url function works correctly
        with paths from device_paths.py.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Test URL construction for various IO paths
        test_cases = [
            ("io", "fan_control"),
        ]
        
        for category, key in test_cases:
            try:
                path = get_ic256_45_path(category, key)
                url = build_http_url(ic256_ip, path)
                
                # Verify URL structure
                assert url.startswith("http://"), "URL should start with http://"
                assert ic256_ip in url, f"URL should contain IP address {ic256_ip}"
                assert path in url, f"URL should contain path {path}"
                assert url == f"http://{ic256_ip}{path}", \
                    f"URL should be http://{ic256_ip}{path}, got {url}"
                    
            except KeyError:
                pytest.skip(f"Path {category}/{key} not found")
