"""Simple single-channel data capture for validation and testing.

This module provides a low-level interface for capturing data from a single
IO channel to validate the basic data collection mechanism before building
the full multi-channel system.
"""

import time
from typing import List, Tuple, Optional, Dict, Any
from collections import deque
from .igx_client import IGXWebsocketClient
from .io_database import IODatabase, ChannelData


def capture_single_channel(
    client: IGXWebsocketClient,
    channel_path: str,
    duration: float,
    stop_event: Optional[Any] = None,
) -> List[Tuple[Any, int]]:
    """Capture all data points from a single channel.
    
    This is a simple, low-level function that:
    1. Subscribes to a single channel with buffered data
    2. Continuously calls updateSubscribedFields() to get all data
    3. Processes ALL entries from the array of arrays returned
    4. Returns all captured data points with their timestamps
    
    Args:
        client: IGXWebsocketClient instance connected to device
        channel_path: Path to the channel (e.g., "/t1/adc/channel_sum")
        duration: How long to collect data (seconds)
        stop_event: Optional threading.Event to signal stop
        
    Returns:
        List of (value, timestamp_ns) tuples for all captured data points
        Timestamps are in nanoseconds since 1970
    """
    # Create channel field
    channel = client.field(channel_path)
    
    # Subscribe with buffered data (True = buffered)
    client.sendSubscribeFields({channel: True})
    
    # Initialize first update to get connection established
    client.updateSubscribedFields()
    
    # Storage for all captured data points
    captured_data: List[Tuple[Any, int]] = []
    
    # Track first timestamp for elapsed time calculation
    first_timestamp: Optional[int] = None
    
    # Start time for duration tracking
    start_time = time.time()
    
    # Main capture loop
    while True:
        # Check stop conditions
        if stop_event and stop_event.is_set():
            break
        if time.time() - start_time >= duration:
            break
        
        # Get latest data from the channel
        # This may return partial updates - that's OK
        client.updateSubscribedFields()
        
        # Get the array of data points from this channel
        # Format: [[value, timestamp], [value, timestamp], ...]
        data = channel.getDatums()
        
        if not data:
            # No data in this update - continue to next update
            time.sleep(0.001)  # Small sleep to avoid tight loop
            continue
        
        # Process ALL entries in the array
        # CRITICAL: Each entry is a [value, timestamp] pair
        # We must process EVERY entry, not just the last one
        for data_point in data:
            # Validate data point format
            if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                continue
            
            value = data_point[0]
            ts_raw = data_point[1]
            
            # Convert timestamp to nanoseconds (device sends ns since 1970)
            try:
                if isinstance(ts_raw, float):
                    # Check magnitude to determine if seconds or nanoseconds
                    if ts_raw < 1e12:  # Likely seconds (Unix timestamp)
                        ts_ns = int(ts_raw * 1e9)
                    else:  # Already in nanoseconds (as float)
                        ts_ns = int(ts_raw)
                elif isinstance(ts_raw, int):
                    ts_ns = ts_raw  # Already in nanoseconds
                else:
                    continue  # Skip invalid timestamp format
            except (ValueError, TypeError, OverflowError):
                continue  # Skip entries with invalid timestamps
            
            # Track first timestamp
            if first_timestamp is None:
                first_timestamp = ts_ns
            
            # Store the data point
            captured_data.append((value, ts_ns))
        
        # Clear datums after processing to avoid re-processing the same data
        channel.clearDatums()
        
        # Small sleep to avoid tight loop if data is coming fast
        time.sleep(0.001)
    
    return captured_data


def capture_to_database(
    client: IGXWebsocketClient,
    channel_paths: List[str],
    duration: float,
    database: Optional[IODatabase] = None,
    stop_event: Optional[Any] = None,
) -> IODatabase:
    """Capture data from multiple channels and store in IO database.
    
    Args:
        client: IGXWebsocketClient instance connected to device
        channel_paths: List of channel paths to capture
        duration: How long to collect data (seconds)
        database: Optional IODatabase to use (creates new one if None)
        stop_event: Optional threading.Event to signal stop
        
    Returns:
        IODatabase containing all captured data
    """
    # Create or use provided database
    if database is None:
        database = IODatabase()
    
    # Create channel fields
    channels = {}
    for path in channel_paths:
        database.add_channel(path)
        channels[path] = client.field(path)
    
    # Subscribe all channels with buffered data
    client.sendSubscribeFields({ch: True for ch in channels.values()})
    
    # Initialize first update
    client.updateSubscribedFields()
    
    # Start time for duration tracking
    start_time = time.time()
    
    # Main capture loop
    while True:
        # Check stop conditions
        if stop_event and stop_event.is_set():
            break
        if time.time() - start_time >= duration:
            break
        
        # Get latest data from all channels
        client.updateSubscribedFields()
        
        # Process data from each channel
        for channel_path, channel_field in channels.items():
            # Get the array of data points from this channel
            data = channel_field.getDatums()
            
            if not data:
                continue
            
            # Process ALL entries in the array
            for data_point in data:
                # Validate data point format
                if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                    continue
                
                value = data_point[0]
                ts_raw = data_point[1]
                
                # Convert timestamp to nanoseconds
                try:
                    if isinstance(ts_raw, float):
                        if ts_raw < 1e12:  # Likely seconds
                            ts_ns = int(ts_raw * 1e9)
                        else:  # Already in nanoseconds
                            ts_ns = int(ts_raw)
                    elif isinstance(ts_raw, int):
                        ts_ns = ts_raw
                    else:
                        continue
                except (ValueError, TypeError, OverflowError):
                    continue
                
                # Add to database
                database.add_data_point(channel_path, value, ts_ns)
            
            # Clear datums after processing to avoid re-processing the same data
            channel_field.clearDatums()
        
        # Small sleep to avoid tight loop
        time.sleep(0.001)
    
    return database


def capture_single_channel_with_stats(
    client: IGXWebsocketClient,
    channel_path: str,
    duration: float,
    stop_event: Optional[Any] = None,
) -> Dict[str, Any]:
    """Capture data from a single channel and return statistics.
    
    Args:
        client: IGXWebsocketClient instance connected to device
        channel_path: Path to the channel
        duration: How long to collect data (seconds)
        stop_event: Optional threading.Event to signal stop
        
    Returns:
        Dictionary with:
            - 'data': List of (value, timestamp_ns) tuples
            - 'count': Number of data points captured
            - 'duration': Actual collection duration (seconds)
            - 'rate': Average data rate (points per second)
            - 'first_timestamp': First timestamp in nanoseconds
            - 'last_timestamp': Last timestamp in nanoseconds
    """
    start_time = time.time()
    data = capture_single_channel(client, channel_path, duration, stop_event)
    end_time = time.time()
    
    actual_duration = end_time - start_time
    count = len(data)
    rate = count / actual_duration if actual_duration > 0 else 0
    
    first_ts = data[0][1] if data else None
    last_ts = data[-1][1] if data else None
    
    return {
        'data': data,
        'count': count,
        'duration': actual_duration,
        'rate': rate,
        'first_timestamp': first_ts,
        'last_timestamp': last_ts,
    }
