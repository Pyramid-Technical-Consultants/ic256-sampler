"""IO Database for storing all data received from IGX device channels."""

import time
import bisect
import sys
from typing import Dict, List, Optional, Any
from collections import deque
from dataclasses import dataclass, field


# Use slots=True for Python 3.10+ for better performance
if sys.version_info >= (3, 10):
    @dataclass(slots=True)
    class DataPoint:
        """Single data point: value, timestamp_ns (nanoseconds since 1970), elapsed_time (seconds)."""
        value: Any
        timestamp_ns: int
        elapsed_time: float = 0.0
else:
    @dataclass
    class DataPoint:
        """Single data point: value, timestamp_ns (nanoseconds since 1970), elapsed_time (seconds)."""
        value: Any
        timestamp_ns: int
        elapsed_time: float = 0.0


@dataclass
class ChannelData:
    """Data for a single channel. Points stored in deque, ordered by timestamp."""
    channel_path: str
    data_points: deque = field(default_factory=deque)
    first_timestamp: Optional[int] = None
    last_timestamp: Optional[int] = None
    count: int = 0
    
    def add_point(self, value: Any, timestamp_ns: int, reference_timestamp: Optional[int] = None) -> None:
        """Add a data point. Invalid timestamps (<=0) use channel's first_timestamp as reference."""
        if self.first_timestamp is None:
            self.first_timestamp = timestamp_ns
            reference_timestamp = timestamp_ns
        elif reference_timestamp is None:
            reference_timestamp = self.first_timestamp
        
        self.last_timestamp = timestamp_ns
        
        # Invalid timestamps use channel's first_timestamp to prevent negative elapsed_time
        if timestamp_ns <= 0:
            reference_timestamp = self.first_timestamp or timestamp_ns
        
        elapsed_time = (timestamp_ns - reference_timestamp) / 1e9
        self.data_points.append(DataPoint(value, timestamp_ns, elapsed_time))
        self.count += 1
    
    def get_points_in_range(self, start_elapsed: float, end_elapsed: float) -> List[DataPoint]:
        """Get all points in elapsed time range. Uses binary search for large datasets (>100 points)."""
        if not self.data_points:
            return []
        
        # Linear search for small datasets (better cache locality)
        if self.count < 100:
            return [p for p in self.data_points if start_elapsed <= p.elapsed_time <= end_elapsed]
        
        # Binary search for large datasets
        snapshot = list(self.data_points)
        elapsed_times = [p.elapsed_time for p in snapshot]
        return snapshot[bisect.bisect_left(elapsed_times, start_elapsed):bisect.bisect_right(elapsed_times, end_elapsed)]
    
    def get_point_at_time(self, target_elapsed: float, tolerance: float = 0.001) -> Optional[DataPoint]:
        """Get point closest to target elapsed time. Uses binary search for large datasets (>50 points)."""
        if not self.data_points:
            return None
        
        # Linear search for small datasets
        if self.count < 50:
            closest = None
            min_diff = float('inf')
            for point in self.data_points:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = point
            return closest
        
        # Binary search for large datasets
        snapshot = list(self.data_points)
        elapsed_times = [p.elapsed_time for p in snapshot]
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Check two closest points (idx and idx-1)
        closest = None
        min_diff = float('inf')
        for candidate_idx in (idx, idx - 1):
            if 0 <= candidate_idx < len(snapshot):
                point = snapshot[candidate_idx]
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = point
        
        return closest
    
    def prune_old_points(self, min_elapsed_time: float) -> int:
        """Remove points with elapsed_time < min_elapsed_time from front of deque."""
        if not self.data_points:
            return 0
        
        points_to_remove = 0
        while self.data_points and self.data_points[0].elapsed_time < min_elapsed_time:
            self.data_points.popleft()
            points_to_remove += 1
        
        if points_to_remove > 0:
            self.count -= points_to_remove
            if self.count == 0:
                self.first_timestamp = self.last_timestamp = None
            elif self.data_points:
                self.first_timestamp = self.data_points[0].timestamp_ns
        
        return points_to_remove
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get channel statistics."""
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
        return {
            'channel_path': self.channel_path,
            'count': self.count,
            'first_timestamp': self.first_timestamp,
            'last_timestamp': self.last_timestamp,
            'time_span': time_span,
            'rate': self.count / time_span if time_span > 0 else 0.0,
        }


class IODatabase:
    """Database storing all data points from IGX device channels. Grows during session."""
    
    def __init__(self):
        self.channels: Dict[str, ChannelData] = {}
        self.global_first_timestamp: Optional[int] = None
        self.session_start_time: float = time.time()
    
    def add_channel(self, channel_path: str) -> ChannelData:
        """Add a new channel. Returns existing channel if already present."""
        if channel_path not in self.channels:
            self.channels[channel_path] = ChannelData(channel_path=channel_path)
        return self.channels[channel_path]
    
    def add_data_point(self, channel_path: str, value: Any, timestamp_ns: int) -> None:
        """Add a data point. Invalid timestamps (<=0) don't set global_first_timestamp."""
        channel_data = self.channels.setdefault(channel_path, ChannelData(channel_path=channel_path))
        
        if self.global_first_timestamp is None and timestamp_ns > 0:
            self.global_first_timestamp = timestamp_ns
        
        reference = self.global_first_timestamp if (self.global_first_timestamp and self.global_first_timestamp > 0) else None
        channel_data.add_point(value, timestamp_ns, reference)
    
    def get_channel(self, channel_path: str) -> Optional[ChannelData]:
        """Get channel data, or None if not found."""
        return self.channels.get(channel_path)
    
    def get_all_channels(self) -> List[str]:
        """Get list of all channel paths."""
        return list(self.channels.keys())
    
    def get_data_at_time(self, target_elapsed: float, tolerance: float = 0.001) -> Dict[str, Optional[DataPoint]]:
        """Get data points from all channels at target elapsed time."""
        return {path: channel.get_point_at_time(target_elapsed, tolerance) for path, channel in self.channels.items()}
    
    def get_data_in_range(self, start_elapsed: float, end_elapsed: float) -> Dict[str, List[DataPoint]]:
        """Get all data points from all channels within elapsed time range."""
        return {path: channel.get_points_in_range(start_elapsed, end_elapsed) for path, channel in self.channels.items()}
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        return {
            'global_first_timestamp': self.global_first_timestamp,
            'session_start_time': self.session_start_time,
            'session_duration': time.time() - self.session_start_time,
            'total_channels': len(self.channels),
            'total_data_points': sum(channel.count for channel in self.channels.values()),
            'channels': {path: channel.get_statistics() for path, channel in self.channels.items()},
        }
    
    def clear(self) -> None:
        """Clear all data."""
        self.channels.clear()
        self.global_first_timestamp = None
        self.session_start_time = time.time()
    
    def get_channel_count(self, channel_path: str) -> int:
        """Get number of data points for a channel."""
        channel_data = self.channels.get(channel_path)
        return channel_data.count if channel_data else 0
    
    def get_total_count(self) -> int:
        """Get total number of data points across all channels."""
        return sum(channel.count for channel in self.channels.values())
    
    def prune_old_data(self, min_elapsed_time: float, max_points_per_channel: int = 100000) -> Dict[str, int]:
        """Prune old points: remove points < min_elapsed_time, then limit to max_points_per_channel."""
        pruned_counts = {}
        
        for channel_path, channel_data in self.channels.items():
            total_pruned = channel_data.prune_old_points(min_elapsed_time)
            
            # Limit to max_points_per_channel if still too many
            if channel_data.count > max_points_per_channel:
                points_to_remove = channel_data.count - max_points_per_channel
                for _ in range(points_to_remove):
                    channel_data.data_points.popleft()
                channel_data.count -= points_to_remove
                total_pruned += points_to_remove
                
                if channel_data.data_points:
                    channel_data.first_timestamp = channel_data.data_points[0].timestamp_ns
                else:
                    channel_data.first_timestamp = channel_data.last_timestamp = None
            
            if total_pruned > 0:
                pruned_counts[channel_path] = total_pruned
        
        return pruned_counts
