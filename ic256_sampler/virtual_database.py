"""Virtual Database for creating synthetic table rows from IO database.

This module provides a VirtualDatabase class that uses an IODatabase to create
a synthetic table database with rows at regular intervals. The database defines
column policies (synchronized, interpolated, asynchronous) and header names.
It also supports converters to transform raw data values to desired units.
"""

from typing import Dict, List, Tuple, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from .io_database import IODatabase, DataPoint, ChannelData


class ChannelPolicy(Enum):
    """Policy for how a channel should be matched to reference timestamps."""
    SYNCHRONIZED = "synchronized"  # Must match exact timestamp (within tolerance)
    INTERPOLATED = "interpolated"  # Interpolate between surrounding points
    ASYNCHRONOUS = "asynchronous"  # Snap to nearest point (no interpolation)


# Converter function type - generic function that transforms a value
Converter = Callable[[Any], Any]


def identity_converter(value: Any) -> Any:
    """Identity converter - returns value as-is."""
    return value


@dataclass
class ColumnDefinition:
    """Definition for a column in the virtual database.
    
    Attributes:
        name: Column name (for CSV header)
        channel_path: Path to the IO channel (None for computed columns)
        policy: How to match this channel to reference timestamps
        converter: Function to convert raw value to desired units (default: identity)
    """
    name: str
    channel_path: Optional[str] = None
    policy: ChannelPolicy = ChannelPolicy.INTERPOLATED
    converter: Converter = field(default_factory=lambda: identity_converter)


@dataclass
class VirtualRow:
    """A single row in the virtual database.
    
    Attributes:
        timestamp: Elapsed time in seconds
        data: Dictionary mapping column names to values
    """
    timestamp: float
    data: Dict[str, Any]


class VirtualDatabase:
    """Virtual database that creates synthetic rows from IO database.
    
    This class takes an IODatabase and creates a synthetic table with rows
    at regular intervals. It uses a reference channel for timing and applies
    different policies (synchronized, interpolated, asynchronous) to match
    other channels to the reference timestamps.
    
    The columns in the virtual database match 1:1 with CSV columns, and this
    class defines the header names and collection policies. Converters can be
    applied to transform raw data values to desired units.
    
    Attributes:
        io_database: The underlying IO database
        reference_channel: Channel path to use as timing reference
        sampling_rate: Sampling rate in Hz (rows per second)
        columns: List of column definitions
        rows: List of VirtualRow objects
    """
    
    def __init__(
        self,
        io_database: IODatabase,
        reference_channel: str,
        sampling_rate: int,
        columns: List[ColumnDefinition],
    ):
        """Initialize virtual database.
        
        Args:
            io_database: IODatabase containing all collected data
            reference_channel: Channel path to use as timing reference
            sampling_rate: Sampling rate in Hz (determines row spacing)
            columns: List of column definitions (defines CSV structure)
        """
        self.io_database = io_database
        self.reference_channel = reference_channel
        self.sampling_rate = sampling_rate
        self.columns = columns
        self.rows: List[VirtualRow] = []
        self._built = False
    
    def get_headers(self) -> List[str]:
        """Get CSV header names for all columns.
        
        Returns:
            List of header strings in column order
        """
        return [col.name for col in self.columns]
    
    def build(self) -> None:
        """Build the virtual database by creating rows at regular intervals.
        
        Uses the reference channel to determine the time range, then creates
        rows at regular intervals (1/sampling_rate) for all channels.
        """
        if self._built:
            return
        
        # Get reference channel data
        ref_channel = self.io_database.get_channel(self.reference_channel)
        if not ref_channel or ref_channel.count == 0:
            return  # No data to build from
        
        # Determine time range from reference channel
        first_elapsed = ref_channel.data_points[0].elapsed_time
        last_elapsed = ref_channel.data_points[-1].elapsed_time
        
        # Calculate row interval
        row_interval = 1.0 / self.sampling_rate
        
        # Create rows at regular intervals
        current_time = first_elapsed
        while current_time <= last_elapsed:
            # Get data for all columns at this timestamp
            row_data = self._get_row_data_at_time(current_time, row_interval)
            
            # Create virtual row
            row = VirtualRow(timestamp=current_time, data=row_data)
            self.rows.append(row)
            
            # Advance to next row time
            current_time += row_interval
        
        self._built = True
    
    def _get_row_data_at_time(
        self,
        target_elapsed: float,
        row_interval: float,
    ) -> Dict[str, Any]:
        """Get data for all columns at a specific elapsed time.
        
        Applies the appropriate policy (synchronized, interpolated, asynchronous)
        for each column based on its definition, then applies the converter.
        
        Args:
            target_elapsed: Target elapsed time in seconds
            row_interval: Time interval between rows (for tolerance calculation)
            
        Returns:
            Dictionary mapping column names to converted values
        """
        result = {}
        
        # First, get reference channel data at this time
        ref_channel = self.io_database.get_channel(self.reference_channel)
        if not ref_channel:
            return result
        
        ref_point = self._find_point_at_time(ref_channel.data_points, target_elapsed, row_interval * 0.5)
        # For synchronized channels, we need the original timestamp_ns from the reference point
        # This allows us to match channels that arrived together
        ref_timestamp_ns = ref_point[2] if ref_point else None
        
        # Process each column according to its policy
        for col_def in self.columns:
            if col_def.channel_path is None:
                # Computed column (no IO channel) - skip for now
                result[col_def.name] = None
                continue
            
            channel_data = self.io_database.get_channel(col_def.channel_path)
            if not channel_data:
                result[col_def.name] = None
                continue
            
            # Apply policy-specific matching
            raw_value = None
            if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                # Must match exact timestamp (within small tolerance)
                # Use original timestamp_ns for matching
                raw_value = self._find_synchronized_value(
                    channel_data.data_points,
                    ref_timestamp_ns,
                    tolerance_ns=1000,  # 1 microsecond tolerance
                )
            elif col_def.policy == ChannelPolicy.INTERPOLATED:
                # Interpolate between surrounding points
                raw_value = self._interpolate_value(
                    channel_data.data_points,
                    target_elapsed,
                    tolerance=row_interval * 2.0,
                )
            elif col_def.policy == ChannelPolicy.ASYNCHRONOUS:
                # Snap to nearest point
                raw_value = self._find_nearest_value(
                    channel_data.data_points,
                    target_elapsed,
                    tolerance=row_interval * 2.0,
                )
            
            # Apply converter to transform raw value
            if raw_value is not None:
                try:
                    converted_value = col_def.converter(raw_value)
                    result[col_def.name] = converted_value
                except Exception as e:
                    # If conversion fails, use None
                    result[col_def.name] = None
            else:
                result[col_def.name] = None
        
        return result
    
    def _find_point_at_time(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Tuple[Any, float, int]]:
        """Find data point closest to target elapsed time.
        
        Returns:
            Tuple of (value, elapsed_time, timestamp_ns) or None
        """
        if not data_points:
            return None
        
        closest = None
        min_diff = float('inf')
        
        for point in data_points:
            diff = abs(point.elapsed_time - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = (point.value, point.elapsed_time, point.timestamp_ns)
        
        return closest
    
    def _find_synchronized_value(
        self,
        data_points: List[DataPoint],
        ref_timestamp_ns: Optional[int],
        tolerance_ns: int = 1000,
    ) -> Optional[Any]:
        """Find value with exact timestamp match (for synchronized channels).
        
        Args:
            data_points: List of data points to search
            ref_timestamp_ns: Reference timestamp in nanoseconds
            tolerance_ns: Tolerance in nanoseconds
            
        Returns:
            Value if found within tolerance, None otherwise
        """
        if ref_timestamp_ns is None:
            return None
        
        for point in data_points:
            if abs(point.timestamp_ns - ref_timestamp_ns) <= tolerance_ns:
                return point.value
        
        return None
    
    def _interpolate_value(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Any]:
        """Interpolate value at target time (for interpolated channels).
        
        Args:
            data_points: List of data points to search
            target_elapsed: Target elapsed time
            tolerance: Maximum time difference for interpolation
            
        Returns:
            Interpolated value or None
        """
        if not data_points:
            return None
        
        # Find points before and after target
        before_point = None
        after_point = None
        before_diff = float('inf')
        after_diff = float('inf')
        
        for point in data_points:
            diff = point.elapsed_time - target_elapsed
            if diff <= 0 and abs(diff) < before_diff:
                before_point = point
                before_diff = abs(diff)
            elif diff > 0 and diff < after_diff:
                after_point = point
                after_diff = diff
        
        # If we have points on both sides, interpolate
        if before_point and after_point:
            t1, v1 = before_point.elapsed_time, before_point.value
            t2, v2 = after_point.elapsed_time, after_point.value
            
            # Avoid division by zero
            if abs(t2 - t1) < 1e-9:
                return v1
            
            # Linear interpolation: v = v1 + (v2 - v1) * (t - t1) / (t2 - t1)
            try:
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    interpolated = v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                    return interpolated
                else:
                    # Non-numeric values - use closest
                    return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
            except (TypeError, ValueError):
                return v1
        
        # If we only have one side, use it
        if before_point:
            return before_point.value
        elif after_point:
            return after_point.value
        
        return None
    
    def _find_nearest_value(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Any]:
        """Find nearest value (for asynchronous channels).
        
        Args:
            data_points: List of data points to search
            target_elapsed: Target elapsed time
            tolerance: Maximum time difference
            
        Returns:
            Nearest value or None
        """
        if not data_points:
            return None
        
        closest = None
        min_diff = float('inf')
        
        for point in data_points:
            diff = abs(point.elapsed_time - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = point.value
        
        return closest
    
    def get_row_count(self) -> int:
        """Get the number of rows in the virtual database."""
        if not self._built:
            self.build()
        return len(self.rows)
    
    def get_rows(self) -> List[VirtualRow]:
        """Get all rows in the virtual database."""
        if not self._built:
            self.build()
        return self.rows
    
    def get_row_at_index(self, index: int) -> Optional[VirtualRow]:
        """Get a row by index."""
        if not self._built:
            self.build()
        if 0 <= index < len(self.rows):
            return self.rows[index]
        return None
    
    def get_row_at_time(
        self,
        target_elapsed: float,
        tolerance: float = 0.0001,
    ) -> Optional[VirtualRow]:
        """Get a row closest to a target elapsed time."""
        if not self._built:
            self.build()
        
        closest = None
        min_diff = float('inf')
        
        for row in self.rows:
            diff = abs(row.timestamp - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = row
        
        return closest
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the virtual database."""
        if not self._built:
            self.build()
        
        if not self.rows:
            return {
                'row_count': 0,
                'time_span': 0.0,
                'sampling_rate': self.sampling_rate,
                'expected_rows': 0,
                'actual_rows': 0,
            }
        
        first_time = self.rows[0].timestamp
        last_time = self.rows[-1].timestamp
        time_span = last_time - first_time
        expected_rows = int(time_span * self.sampling_rate)
        
        return {
            'row_count': len(self.rows),
            'time_span': time_span,
            'first_timestamp': first_time,
            'last_timestamp': last_time,
            'sampling_rate': self.sampling_rate,
            'expected_rows': expected_rows,
            'actual_rows': len(self.rows),
            'coverage': len(self.rows) / expected_rows if expected_rows > 0 else 0.0,
        }
    
    def clear(self) -> None:
        """Clear all rows from the virtual database."""
        self.rows.clear()
        self._built = False
    
    def rebuild(self) -> None:
        """Rebuild the virtual database from the IO database."""
        self.clear()
        self.build()
    
    def prune_rows(self, keep_last_n: int) -> int:
        """Prune old rows from the virtual database to save memory.
        
        This removes rows from the beginning of the list, keeping only
        the most recent N rows. This should only be called after those
        rows have been safely written to disk.
        
        Args:
            keep_last_n: Number of most recent rows to keep
            
        Returns:
            Number of rows pruned
        """
        if not self._built or len(self.rows) <= keep_last_n:
            return 0
        
        rows_to_remove = len(self.rows) - keep_last_n
        # Remove rows from the beginning of the list
        self.rows = self.rows[-keep_last_n:]
        
        return rows_to_remove




def create_tx2_columns(
    io_database: IODatabase,
    reference_channel: str,
) -> List[ColumnDefinition]:
    """Create column definitions for TX2 device.
    
    Args:
        io_database: IODatabase to get channel paths from
        reference_channel: Channel path to use as reference
        
    Returns:
        List of ColumnDefinition objects in CSV order
    """
    from .device_paths import TX2_PATHS
    
    columns = [
        ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ColumnDefinition(
            name="Probe A",
            channel_path=TX2_PATHS["adc"]["channel_5"],
            policy=ChannelPolicy.INTERPOLATED,
        ),
        ColumnDefinition(
            name="Probe B",
            channel_path=TX2_PATHS["adc"]["channel_1"],
            policy=ChannelPolicy.INTERPOLATED,
        ),
        ColumnDefinition(
            name="FR2",
            channel_path=TX2_PATHS["adc"]["fr2"],
            policy=ChannelPolicy.INTERPOLATED,
        ),
        ColumnDefinition(name="Note", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
    ]
    
    return columns
