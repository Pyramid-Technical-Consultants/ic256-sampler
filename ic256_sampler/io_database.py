"""IO Database for storing all data received from IGX device.

This module provides a formal data structure for storing all data points
received from device channels. The database grows throughout the session
and can be queried for data reconstruction and validation.
"""

import time
import bisect
import sys
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import deque
from dataclasses import dataclass, field


# Use slots=True for Python 3.10+ for better performance
if sys.version_info >= (3, 10):
    @dataclass(slots=True)
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
else:
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
        
        # If timestamp_ns is invalid (0), always use channel's first_timestamp as reference
        # This prevents negative elapsed_time when invalid timestamps are mixed with valid global references
        if timestamp_ns <= 0:
            reference_timestamp = self.first_timestamp if self.first_timestamp is not None else timestamp_ns
        
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
        
        Uses binary search for efficient range queries on large datasets.
        
        Args:
            start_elapsed: Start elapsed time (seconds)
            end_elapsed: End elapsed time (seconds)
            
        Returns:
            List of DataPoint objects within the range
        """
        if not self.data_points:
            return []
        
        # For small datasets, linear search is faster due to cache locality
        if self.count < 100:
            return [
                point for point in self.data_points
                if start_elapsed <= point.elapsed_time <= end_elapsed
            ]
        
        # For larger datasets, use binary search to find bounds
        # Create snapshot only if needed (deque doesn't support direct indexing for bisect)
        snapshot = list(self.data_points)
        
        # Find start index using binary search
        elapsed_times = [p.elapsed_time for p in snapshot]
        start_idx = bisect.bisect_left(elapsed_times, start_elapsed)
        end_idx = bisect.bisect_right(elapsed_times, end_elapsed)
        
        # Return slice of points in range
        return snapshot[start_idx:end_idx]
    
    def get_point_at_time(
        self, 
        target_elapsed: float, 
        tolerance: float = 0.001
    ) -> Optional[DataPoint]:
        """Get the data point closest to a target elapsed time.
        
        Uses binary search for efficient lookups on large datasets.
        
        Args:
            target_elapsed: Target elapsed time (seconds)
            tolerance: Maximum time difference (seconds)
            
        Returns:
            DataPoint closest to target, or None if none within tolerance
        """
        if not self.data_points:
            return None
        
        # For small datasets, linear search is faster due to cache locality
        if self.count < 50:
            closest = None
            min_diff = float('inf')
            for point in self.data_points:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = point
            return closest
        
        # For larger datasets, use binary search
        snapshot = list(self.data_points)
        elapsed_times = [p.elapsed_time for p in snapshot]
        
        # Find the index where target_elapsed would be inserted
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Check the point at idx and idx-1 (the two closest points)
        candidates = []
        if idx < len(snapshot):
            candidates.append(snapshot[idx])
        if idx > 0:
            candidates.append(snapshot[idx - 1])
        
        # Find the closest candidate within tolerance
        closest = None
        min_diff = float('inf')
        for point in candidates:
            diff = abs(point.elapsed_time - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = point
        
        return closest
    
    def prune_old_points(self, min_elapsed_time: float) -> int:
        """Prune data points older than the specified elapsed time.
        
        Removes points from the beginning of the deque that have elapsed_time
        less than min_elapsed_time. This helps prevent unbounded memory growth.
        
        Args:
            min_elapsed_time: Minimum elapsed time to keep (seconds)
            
        Returns:
            Number of points pruned
        """
        if not self.data_points or self.count == 0:
            return 0
        
        # Use popleft() in a loop instead of iterating to avoid mutation during iteration
        # Points are ordered by elapsed_time, so we can safely remove from the front
        points_to_remove = 0
        
        # Remove points from the beginning until we find one that's >= min_elapsed_time
        while self.data_points:
            # Peek at the first point without removing it yet
            first_point = self.data_points[0]
            if first_point.elapsed_time < min_elapsed_time:
                # Safe to remove - it's too old
                self.data_points.popleft()
                points_to_remove += 1
            else:
                # First point is new enough, so all remaining points are too
                break
        
        if points_to_remove > 0:
            self.count -= points_to_remove
            
            # Update first_timestamp if we removed all points
            if self.count == 0:
                self.first_timestamp = None
                self.last_timestamp = None
            elif self.data_points:
                # Update first_timestamp to the new first point
                first_point = self.data_points[0]
                self.first_timestamp = first_point.timestamp_ns
        
        return points_to_remove
    
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
        """Add a data point to a channel."""
        # Get or create channel in one operation
        channel_data = self.channels.setdefault(channel_path, ChannelData(channel_path=channel_path))
        
        # Track global first timestamp (only for valid timestamps > 0)
        # Invalid timestamps (0) should not be used as the global reference
        if self.global_first_timestamp is None and timestamp_ns > 0:
            self.global_first_timestamp = timestamp_ns
        
        # Use global_first_timestamp as reference, but only if it's valid (> 0)
        # Otherwise, let ChannelData.add_point use the channel's first_timestamp
        reference_timestamp = self.global_first_timestamp if (self.global_first_timestamp is not None and self.global_first_timestamp > 0) else None
        
        # Add the data point
        channel_data.add_point(value, timestamp_ns, reference_timestamp)
    
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
    
    def prune_old_data(self, min_elapsed_time: float, max_points_per_channel: int = 100000) -> Dict[str, int]:
        """Prune old data points from all channels.
        
        This method:
        1. Prunes points older than min_elapsed_time from each channel
        2. If a channel still has more than max_points_per_channel points,
           keeps only the most recent max_points_per_channel points
           
        Args:
            min_elapsed_time: Minimum elapsed time to keep (seconds)
            max_points_per_channel: Maximum points to keep per channel (default: 100000)
            
        Returns:
            Dictionary mapping channel paths to number of points pruned
        """
        pruned_counts = {}
        
        for channel_path, channel_data in self.channels.items():
            total_pruned = 0
            
            # First, prune by elapsed time
            pruned_by_time = channel_data.prune_old_points(min_elapsed_time)
            total_pruned += pruned_by_time
            
            # Then, if still too many points, keep only the most recent ones
            if channel_data.count > max_points_per_channel:
                points_to_remove = channel_data.count - max_points_per_channel
                for _ in range(points_to_remove):
                    channel_data.data_points.popleft()
                channel_data.count -= points_to_remove
                total_pruned += points_to_remove
                
                # Update first_timestamp
                if channel_data.data_points:
                    first_point = channel_data.data_points[0]
                    channel_data.first_timestamp = first_point.timestamp_ns
                else:
                    channel_data.first_timestamp = None
                    channel_data.last_timestamp = None
            
            if total_pruned > 0:
                pruned_counts[channel_path] = total_pruned
        
        return pruned_counts
