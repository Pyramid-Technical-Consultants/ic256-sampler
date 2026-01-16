"""IO Database for storing all data received from IGX device.

This module provides a formal data structure for storing all data points
received from device channels. The database grows throughout the session
and can be queried for data reconstruction and validation.
"""

import time
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import deque
from dataclasses import dataclass, field


@dataclass
class DataPoint:
    """A single data point from a channel.
    
    Attributes:
        value: The data value (can be any type)
        timestamp_ns: Timestamp in nanoseconds since 1970
        elapsed_time: Elapsed time in seconds since first timestamp
    """
    value: Any
    timestamp_ns: int
    elapsed_time: float = 0.0


@dataclass
class ChannelData:
    """Data for a single channel.
    
    Attributes:
        channel_path: Path to the channel (e.g., "/t1/adc/channel_sum")
        data_points: Deque of DataPoint objects (ordered by timestamp)
        first_timestamp: First timestamp seen for this channel (nanoseconds)
        last_timestamp: Last timestamp seen for this channel (nanoseconds)
        count: Total number of data points
    """
    channel_path: str
    data_points: deque = field(default_factory=deque)
    first_timestamp: Optional[int] = None
    last_timestamp: Optional[int] = None
    count: int = 0
    
    def add_point(self, value: Any, timestamp_ns: int, reference_timestamp: Optional[int] = None) -> None:
        """Add a data point to this channel.
        
        Args:
            value: The data value
            timestamp_ns: Timestamp in nanoseconds since 1970
            reference_timestamp: Reference timestamp for elapsed time calculation (uses first_timestamp if None)
        """
        # Track first and last timestamps
        if self.first_timestamp is None:
            self.first_timestamp = timestamp_ns
            reference_timestamp = timestamp_ns
        elif reference_timestamp is None:
            reference_timestamp = self.first_timestamp
        
        self.last_timestamp = timestamp_ns
        
        # Calculate elapsed time
        elapsed_time = (timestamp_ns - reference_timestamp) / 1e9
        
        # Create and store data point
        point = DataPoint(value=value, timestamp_ns=timestamp_ns, elapsed_time=elapsed_time)
        self.data_points.append(point)
        self.count += 1
    
    def get_points_in_range(
        self, 
        start_elapsed: float, 
        end_elapsed: float
    ) -> List[DataPoint]:
        """Get all data points within an elapsed time range.
        
        Args:
            start_elapsed: Start elapsed time (seconds)
            end_elapsed: End elapsed time (seconds)
            
        Returns:
            List of DataPoint objects within the range
        """
        return [
            point for point in self.data_points
            if start_elapsed <= point.elapsed_time <= end_elapsed
        ]
    
    def get_point_at_time(
        self, 
        target_elapsed: float, 
        tolerance: float = 0.001
    ) -> Optional[DataPoint]:
        """Get the data point closest to a target elapsed time.
        
        Args:
            target_elapsed: Target elapsed time (seconds)
            tolerance: Maximum time difference (seconds)
            
        Returns:
            DataPoint closest to target, or None if none within tolerance
        """
        closest = None
        min_diff = float('inf')
        
        for point in self.data_points:
            diff = abs(point.elapsed_time - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = point
        
        return closest
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics for this channel.
        
        Returns:
            Dictionary with channel statistics
        """
        if self.count == 0:
            return {
                'channel_path': self.channel_path,
                'count': 0,
                'first_timestamp': None,
                'last_timestamp': None,
                'time_span': 0.0,
                'rate': 0.0,
            }
        
        time_span = (self.last_timestamp - self.first_timestamp) / 1e9 if self.first_timestamp else 0.0
        rate = self.count / time_span if time_span > 0 else 0.0
        
        return {
            'channel_path': self.channel_path,
            'count': self.count,
            'first_timestamp': self.first_timestamp,
            'last_timestamp': self.last_timestamp,
            'time_span': time_span,
            'rate': rate,
        }


class IODatabase:
    """Database for storing all data received from IGX device channels.
    
    This database grows throughout the session, storing all data points
    from all subscribed channels. It can be queried for data reconstruction
    and validation.
    
    Attributes:
        channels: Dictionary mapping channel paths to ChannelData objects
        global_first_timestamp: First timestamp seen across all channels (nanoseconds)
        session_start_time: Session start time (seconds since epoch)
    """
    
    def __init__(self):
        """Initialize an empty IO database."""
        self.channels: Dict[str, ChannelData] = {}
        self.global_first_timestamp: Optional[int] = None
        self.session_start_time: float = time.time()
    
    def add_channel(self, channel_path: str) -> ChannelData:
        """Add a new channel to the database.
        
        Args:
            channel_path: Path to the channel
            
        Returns:
            ChannelData object for the channel
        """
        if channel_path not in self.channels:
            self.channels[channel_path] = ChannelData(channel_path=channel_path)
        return self.channels[channel_path]
    
    def add_data_point(
        self, 
        channel_path: str, 
        value: Any, 
        timestamp_ns: int
    ) -> None:
        """Add a data point to a channel.
        
        Args:
            channel_path: Path to the channel
            value: The data value
            timestamp_ns: Timestamp in nanoseconds since 1970
        """
        # Ensure channel exists
        if channel_path not in self.channels:
            self.add_channel(channel_path)
        
        channel_data = self.channels[channel_path]
        
        # Track global first timestamp
        if self.global_first_timestamp is None:
            self.global_first_timestamp = timestamp_ns
        
        # Use global first timestamp as reference for elapsed time
        reference = self.global_first_timestamp
        
        # Add the data point
        channel_data.add_point(value, timestamp_ns, reference)
    
    def get_channel(self, channel_path: str) -> Optional[ChannelData]:
        """Get channel data for a specific channel.
        
        Args:
            channel_path: Path to the channel
            
        Returns:
            ChannelData object, or None if channel doesn't exist
        """
        return self.channels.get(channel_path)
    
    def get_all_channels(self) -> List[str]:
        """Get list of all channel paths in the database.
        
        Returns:
            List of channel paths
        """
        return list(self.channels.keys())
    
    def get_data_at_time(
        self, 
        target_elapsed: float, 
        tolerance: float = 0.001
    ) -> Dict[str, Optional[DataPoint]]:
        """Get data points from all channels at a specific elapsed time.
        
        Args:
            target_elapsed: Target elapsed time (seconds)
            tolerance: Maximum time difference (seconds)
            
        Returns:
            Dictionary mapping channel paths to DataPoint objects (None if no data within tolerance)
        """
        result = {}
        for channel_path, channel_data in self.channels.items():
            result[channel_path] = channel_data.get_point_at_time(target_elapsed, tolerance)
        return result
    
    def get_data_in_range(
        self, 
        start_elapsed: float, 
        end_elapsed: float
    ) -> Dict[str, List[DataPoint]]:
        """Get all data points from all channels within an elapsed time range.
        
        Args:
            start_elapsed: Start elapsed time (seconds)
            end_elapsed: End elapsed time (seconds)
            
        Returns:
            Dictionary mapping channel paths to lists of DataPoint objects
        """
        result = {}
        for channel_path, channel_data in self.channels.items():
            result[channel_path] = channel_data.get_points_in_range(start_elapsed, end_elapsed)
        return result
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics for the entire database.
        
        Returns:
            Dictionary with database statistics
        """
        channel_stats = {
            path: channel.get_statistics() 
            for path, channel in self.channels.items()
        }
        
        total_points = sum(channel.count for channel in self.channels.values())
        session_duration = time.time() - self.session_start_time
        
        return {
            'global_first_timestamp': self.global_first_timestamp,
            'session_start_time': self.session_start_time,
            'session_duration': session_duration,
            'total_channels': len(self.channels),
            'total_data_points': total_points,
            'channels': channel_stats,
        }
    
    def clear(self) -> None:
        """Clear all data from the database."""
        self.channels.clear()
        self.global_first_timestamp = None
        self.session_start_time = time.time()
    
    def get_channel_count(self, channel_path: str) -> int:
        """Get the number of data points for a specific channel.
        
        Args:
            channel_path: Path to the channel
            
        Returns:
            Number of data points, or 0 if channel doesn't exist
        """
        channel_data = self.channels.get(channel_path)
        return channel_data.count if channel_data else 0
    
    def get_total_count(self) -> int:
        """Get total number of data points across all channels.
        
        Returns:
            Total number of data points
        """
        return sum(channel.count for channel in self.channels.values())
