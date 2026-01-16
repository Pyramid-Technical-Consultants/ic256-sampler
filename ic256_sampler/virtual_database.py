"""Virtual Database for creating synthetic table rows from IO database.

This module provides a VirtualDatabase class that uses an IODatabase to create
a synthetic table database with rows at regular intervals. The database defines
column policies (synchronized, interpolated, asynchronous) and header names.
It also supports converters to transform raw data values to desired units.
"""

from typing import Dict, List, Tuple, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import bisect
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
        self._last_built_time: Optional[float] = None  # Track last built timestamp for incremental builds
        
        # Performance optimization: cache search positions for each channel
        # Maps channel_path -> last search index to enable incremental search
        self._search_cache: Dict[str, int] = {}
        
        # Performance optimization: cache channel snapshots and elapsed times
        # Maps channel_path -> (snapshot, elapsed_times_list, timestamps_list)
        # Only valid during a single build/rebuild operation
        self._snapshot_cache: Dict[str, Tuple[List[DataPoint], List[float], List[int]]] = {}
        
        # Performance optimization: cache channel data and snapshots
        # Maps channel_path -> (ChannelData, snapshot, elapsed_times_list, timestamps_list)
        self._channel_cache: Dict[str, Tuple[Optional[ChannelData], Optional[List[DataPoint]], Optional[List[float]], Optional[List[int]]]] = {}
        self._ref_channel_cache: Optional[ChannelData] = None
    
    def get_headers(self) -> List[str]:
        """Get CSV header names for all columns.
        
        Returns:
            List of header strings in column order
        """
        return [col.name for col in self.columns]
    
    def _is_primed(self) -> bool:
        """Check if all channels have at least one data point (primed).
        
        Only checks channels that actually exist in the IODatabase.
        If a channel path is in column definitions but not in IODatabase
        (e.g., not subscribed/collected), it's skipped for priming.
        
        Returns:
            True if all channels with channel_path that exist in IODatabase
            have at least one data point, False otherwise.
        """
        # Get all unique channel paths from column definitions
        channel_paths = set()
        for col_def in self.columns:
            if col_def.channel_path is not None:
                channel_paths.add(col_def.channel_path)
        
        # Get all channels that actually exist in the IODatabase
        existing_channels = self.io_database.get_all_channels()
        
        # Check that each channel that exists in IODatabase has at least one data point
        for channel_path in channel_paths:
            # Only check channels that actually exist in IODatabase
            if channel_path not in existing_channels:
                continue  # Skip channels that don't exist (not subscribed/collected)
            
            channel_data = self.io_database.get_channel(channel_path)
            if not channel_data or channel_data.count == 0:
                return False
        
        return True
    
    def build(self) -> None:
        """Build the virtual database by creating rows at regular intervals.
        
        Uses the reference channel to determine the time range, then creates
        rows at regular intervals (1/sampling_rate) for all channels.
        
        Will not create rows until all channels have at least one data point (primed).
        """
        if self._built:
            return
        
        # Clear snapshot cache for fresh build
        self._snapshot_cache.clear()
        self._ref_channel_cache = None
        
        # Get reference channel data
        ref_channel = self.io_database.get_channel(self.reference_channel)
        if not ref_channel or ref_channel.count == 0:
            return  # No data to build from
        
        # Check if all channels are primed (have at least one data point)
        if not self._is_primed():
            return  # Not all channels have data yet, wait for priming
        
        # Cache reference channel
        self._ref_channel_cache = ref_channel
        
        # Get time range efficiently - deques support indexing for first/last
        if len(ref_channel.data_points) == 0:
            return
        first_elapsed = ref_channel.data_points[0].elapsed_time
        last_elapsed = ref_channel.data_points[-1].elapsed_time
        
        # Calculate row interval
        row_interval = 1.0 / self.sampling_rate
        
        # Pre-compute snapshots for all channels to avoid repeated conversions
        # This is a one-time cost that pays off during the build loop
        # Also pre-compute elapsed_times and timestamps lists for binary search efficiency
        channel_snapshots: Dict[str, List[DataPoint]] = {}
        channel_elapsed_times: Dict[str, List[float]] = {}
        channel_timestamps: Dict[str, List[int]] = {}  # For synchronized channels
        for col_def in self.columns:
            if col_def.channel_path and col_def.channel_path not in channel_snapshots:
                channel_data = self.io_database.get_channel(col_def.channel_path)
                if channel_data:
                    # Create snapshot once per channel
                    snapshot = list(channel_data.data_points)
                    channel_snapshots[col_def.channel_path] = snapshot
                    # Pre-compute elapsed_times for binary search (one-time cost)
                    channel_elapsed_times[col_def.channel_path] = [p.elapsed_time for p in snapshot]
                    # Pre-compute timestamps for synchronized channels (one-time cost)
                    if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                        channel_timestamps[col_def.channel_path] = [p.timestamp_ns for p in snapshot]
        
        # Also snapshot reference channel and pre-compute its elapsed_times
        ref_snapshot = list(ref_channel.data_points)
        ref_elapsed_times = [p.elapsed_time for p in ref_snapshot]
        
        # Pre-index column data to avoid dictionary lookups in hot loop
        # Create a list of tuples: (col_name, col_policy, converter, snapshot, elapsed_times, timestamps)
        # Cache all attributes to reduce attribute access overhead
        column_data = []
        for col_def in self.columns:
            col_name = col_def.name
            col_policy = col_def.policy
            converter = col_def.converter
            if col_def.channel_path is None:
                column_data.append((col_name, col_policy, converter, None, None, None))
            else:
                snapshot = channel_snapshots.get(col_def.channel_path)
                elapsed_times = channel_elapsed_times.get(col_def.channel_path) if snapshot else None
                timestamps = channel_timestamps.get(col_def.channel_path) if col_policy == ChannelPolicy.SYNCHRONIZED else None
                column_data.append((col_name, col_policy, converter, snapshot, elapsed_times, timestamps))
        
        # Estimate number of rows and pre-allocate
        estimated_rows = int((last_elapsed - first_elapsed) * self.sampling_rate) + 1
        self.rows = []
        if estimated_rows > 0:
            # Pre-allocate with None to avoid repeated reallocations
            self.rows = [None] * min(estimated_rows, 1000000)  # Cap at 1M to avoid huge allocations
        
        # Create rows at regular intervals
        current_time = first_elapsed
        last_row_time = None
        row_idx = 0
        
        # Use incremental search - start from beginning for first row
        ref_search_idx = 0
        
        # Pre-compute tolerance values
        ref_tolerance = row_interval * 0.5
        interp_tolerance = row_interval * 2.0
        
        # Track last known values for forward-fill (for INTERPOLATED channels)
        last_known_values: Dict[str, Any] = {}
        
        while current_time <= last_elapsed:
            # Find reference point using optimized incremental search
            # Use binary search with incremental start position for better performance
            ref_timestamp_ns = None
            if ref_snapshot and ref_elapsed_times:
                # Use binary search starting from last known position
                idx = bisect.bisect_left(ref_elapsed_times, current_time, lo=ref_search_idx)
                # Check both candidates (idx and idx-1) for closest match
                best_idx = None
                best_diff = ref_tolerance + 1.0
                
                if idx < len(ref_elapsed_times):
                    diff = abs(ref_elapsed_times[idx] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx
                
                if idx > 0:
                    diff = abs(ref_elapsed_times[idx - 1] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx - 1
                
                if best_idx is not None:
                    point = ref_snapshot[best_idx]
                    ref_timestamp_ns = point.timestamp_ns
                    ref_search_idx = best_idx
                else:
                    # No match found, advance search index to avoid rechecking
                    ref_search_idx = max(0, idx - 1)
            
            # Process columns (fully inlined for speed)
            # Pre-allocate dictionary with known size to avoid rehashing
            row_data = dict.fromkeys((col_name for col_name, _, _, _, _, _ in column_data), None)
            
            # Inline all policy-specific matching to avoid function call overhead
            for col_name, col_policy, converter, snapshot, elapsed_times, timestamps in column_data:
                if snapshot is None:
                    continue
                
                raw_value = None
                
                # Inline SYNCHRONIZED policy
                if col_policy == ChannelPolicy.SYNCHRONIZED:
                    if ref_timestamp_ns is not None and timestamps:
                        if len(snapshot) < 50:
                            # Linear search for small datasets
                            for i, point in enumerate(snapshot):
                                if abs(timestamps[i] - ref_timestamp_ns) <= 1000:
                                    raw_value = point.value
                                    break
                        else:
                            # Binary search for larger datasets
                            ts_idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
                            for i in [ts_idx, ts_idx - 1, ts_idx + 1]:
                                if 0 <= i < len(snapshot):
                                    if abs(timestamps[i] - ref_timestamp_ns) <= 1000:
                                        raw_value = snapshot[i].value
                                        break
                
                # Inline INTERPOLATED policy
                elif col_policy == ChannelPolicy.INTERPOLATED:
                    if elapsed_times:
                        if len(snapshot) < 50:
                            # Linear search for small datasets
                            before_point = None
                            after_point = None
                            before_diff = float('inf')
                            after_diff = float('inf')
                            for i, point in enumerate(snapshot):
                                diff = elapsed_times[i] - current_time
                                if diff <= 0 and abs(diff) < before_diff:
                                    before_point = point
                                    before_diff = abs(diff)
                                elif diff > 0 and diff < after_diff:
                                    after_point = point
                                    after_diff = diff
                            
                            if before_point and after_point:
                                t1, v1 = before_point.elapsed_time, before_point.value
                                t2, v2 = after_point.elapsed_time, after_point.value
                                if abs(t2 - t1) >= 1e-9:
                                    try:
                                        if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                                            raw_value = v1 + (v2 - v1) * (current_time - t1) / (t2 - t1)
                                        else:
                                            raw_value = v1 if abs(current_time - t1) < abs(current_time - t2) else v2
                                    except (TypeError, ValueError):
                                        raw_value = v1
                                else:
                                    raw_value = v1
                            elif before_point:
                                raw_value = before_point.value
                            elif after_point:
                                raw_value = after_point.value
                        else:
                            # Binary search for larger datasets
                            idx = bisect.bisect_left(elapsed_times, current_time)
                            before_idx = idx - 1
                            after_idx = idx
                            before_valid = before_idx >= 0 and abs(elapsed_times[before_idx] - current_time) <= interp_tolerance
                            after_valid = after_idx < len(snapshot) and abs(elapsed_times[after_idx] - current_time) <= interp_tolerance
                            
                            if before_valid and after_valid:
                                before_point = snapshot[before_idx]
                                after_point = snapshot[after_idx]
                                t1, v1 = before_point.elapsed_time, before_point.value
                                t2, v2 = after_point.elapsed_time, after_point.value
                                if abs(t2 - t1) >= 1e-9:
                                    try:
                                        if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                                            raw_value = v1 + (v2 - v1) * (current_time - t1) / (t2 - t1)
                                        else:
                                            raw_value = v1 if abs(current_time - t1) < abs(current_time - t2) else v2
                                    except (TypeError, ValueError):
                                        raw_value = v1
                                else:
                                    raw_value = v1
                            elif before_valid:
                                raw_value = snapshot[before_idx].value
                            elif after_valid:
                                raw_value = snapshot[after_idx].value
                
                # Inline ASYNCHRONOUS policy
                elif col_policy == ChannelPolicy.ASYNCHRONOUS:
                    if elapsed_times:
                        if len(snapshot) < 50:
                            # Linear search for small datasets
                            closest = None
                            min_diff = float('inf')
                            for i, point in enumerate(snapshot):
                                diff = abs(elapsed_times[i] - current_time)
                                if diff < min_diff and diff <= interp_tolerance:
                                    min_diff = diff
                                    closest = point.value
                            raw_value = closest
                        else:
                            # Binary search for larger datasets
                            idx = bisect.bisect_left(elapsed_times, current_time)
                            closest = None
                            min_diff = float('inf')
                            
                            if idx < len(snapshot):
                                diff = abs(elapsed_times[idx] - current_time)
                                if diff < min_diff and diff <= interp_tolerance:
                                    min_diff = diff
                                    closest = snapshot[idx].value
                            
                            if idx > 0:
                                diff = abs(elapsed_times[idx - 1] - current_time)
                                if diff < min_diff and diff <= interp_tolerance:
                                    closest = snapshot[idx - 1].value
                            
                            raw_value = closest
                
                # Apply converter
                if raw_value is not None:
                    try:
                        converted_value = converter(raw_value)
                        row_data[col_name] = converted_value
                        # Update last known value for forward-fill
                        if col_policy == ChannelPolicy.INTERPOLATED:
                            last_known_values[col_name] = converted_value
                    except Exception:
                        pass
                elif col_policy == ChannelPolicy.INTERPOLATED and col_name in last_known_values:
                    # Forward-fill: use last known value if interpolation failed
                    row_data[col_name] = last_known_values[col_name]
            
            # Create virtual row
            if row_idx < len(self.rows):
                self.rows[row_idx] = VirtualRow(timestamp=current_time, data=row_data)
            else:
                self.rows.append(VirtualRow(timestamp=current_time, data=row_data))
            row_idx += 1
            last_row_time = current_time
            
            # Advance to next row time
            current_time += row_interval
        
        # Trim to actual size
        if row_idx < len(self.rows):
            self.rows = self.rows[:row_idx]
        
        self._built = True
        # Set to last row's timestamp, not last data point's elapsed time
        # This ensures incremental rebuild starts from the correct position
        self._last_built_time = last_row_time if last_row_time is not None else last_elapsed
        
        # Clear snapshot cache after build
        self._snapshot_cache.clear()
    
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
        
        # Cache reference channel lookup (set during build/rebuild)
        ref_channel = self._ref_channel_cache
        if not ref_channel:
            return result
        
        ref_point = self._find_point_at_time(ref_channel.data_points, target_elapsed, row_interval * 0.5)
        # For synchronized channels, we need the original timestamp_ns from the reference point
        # This allows us to match channels that arrived together
        ref_timestamp_ns = ref_point[2] if ref_point else None
        
        # Cache channel lookups to avoid redundant dictionary access
        # Many columns may share the same channel_path
        channel_cache: Dict[str, Optional[ChannelData]] = {}
        
        # Process each column according to its policy
        for col_def in self.columns:
            if col_def.channel_path is None:
                # Computed column (no IO channel) - skip for now
                result[col_def.name] = None
                continue
            
            # Use cached channel lookup if available
            channel_data = channel_cache.get(col_def.channel_path)
            if channel_data is None:
                channel_data = self.io_database.get_channel(col_def.channel_path)
                channel_cache[col_def.channel_path] = channel_data
            
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
    
    def _get_row_data_at_time_optimized(
        self,
        target_elapsed: float,
        row_interval: float,
        ref_snapshot: List[DataPoint],
        ref_elapsed_times: List[float],
        channel_snapshots: Dict[str, List[DataPoint]],
        channel_elapsed_times: Dict[str, List[float]],
        channel_timestamps: Dict[str, List[int]],
        start_search_idx: int = 0,
    ) -> Dict[str, Any]:
        """Optimized version that uses pre-computed snapshots and incremental binary search.
        
        This is used during build() to avoid repeated snapshot creation and elapsed_times list creation.
        """
        result = {}
        
        # Find reference point using incremental binary search (much faster for large arrays)
        ref_point = None
        ref_idx = start_search_idx
        if ref_snapshot and ref_elapsed_times:
            # Use binary search starting from last position for incremental search
            # First, narrow down the search range using the start index
            if ref_idx < len(ref_elapsed_times) and ref_elapsed_times[ref_idx] <= target_elapsed:
                # Start binary search from last known position
                search_start = ref_idx
            else:
                search_start = 0
            
            # Use binary search on pre-computed elapsed_times list
            idx = bisect.bisect_left(ref_elapsed_times, target_elapsed, lo=search_start)
            
            # Check the point at idx and idx-1 (the two closest points)
            candidates = []
            if idx < len(ref_snapshot):
                candidates.append((idx, ref_snapshot[idx]))
            if idx > 0:
                candidates.append((idx - 1, ref_snapshot[idx - 1]))
            
            # Find the closest candidate within tolerance
            closest_idx = None
            min_diff = float('inf')
            tolerance = row_interval * 0.5
            for candidate_idx, point in candidates:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest_idx = candidate_idx
                    ref_point = (point.value, point.elapsed_time, point.timestamp_ns)
            
            if closest_idx is not None:
                ref_idx = closest_idx
        
        ref_timestamp_ns = ref_point[2] if ref_point else None
        result['_ref_idx'] = ref_idx  # Store for next iteration
        
        # Process each column using cached snapshots
        for col_def in self.columns:
            if col_def.channel_path is None:
                result[col_def.name] = None
                continue
            
            snapshot = channel_snapshots.get(col_def.channel_path)
            if not snapshot:
                result[col_def.name] = None
                continue
            
            # Apply policy-specific matching using snapshot and pre-computed elapsed_times
            elapsed_times = channel_elapsed_times.get(col_def.channel_path)
            raw_value = None
            if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                # Use pre-computed timestamps list for synchronized search
                timestamps = channel_timestamps.get(col_def.channel_path)
                raw_value = self._find_synchronized_value_with_timestamps(
                    snapshot, timestamps, ref_timestamp_ns, tolerance_ns=1000
                )
            elif col_def.policy == ChannelPolicy.INTERPOLATED:
                raw_value = self._interpolate_value_in_snapshot_with_elapsed(
                    snapshot, elapsed_times, target_elapsed, tolerance=row_interval * 2.0
                )
            elif col_def.policy == ChannelPolicy.ASYNCHRONOUS:
                raw_value = self._find_nearest_value_in_snapshot_with_elapsed(
                    snapshot, elapsed_times, target_elapsed, tolerance=row_interval * 2.0
                )
            
            # Apply converter
            if raw_value is not None:
                try:
                    converted_value = col_def.converter(raw_value)
                    result[col_def.name] = converted_value
                except Exception:
                    result[col_def.name] = None
            else:
                result[col_def.name] = None
        
        return result
    
    def _find_synchronized_value_in_snapshot(
        self, snapshot: List[DataPoint], ref_timestamp_ns: Optional[int], tolerance_ns: int
    ) -> Optional[Any]:
        """Find synchronized value in pre-computed snapshot (fallback method)."""
        return self._find_synchronized_value_in_snapshot_optimized(snapshot, ref_timestamp_ns, tolerance_ns)
    
    def _find_synchronized_value_in_snapshot_optimized(
        self, snapshot: List[DataPoint], ref_timestamp_ns: Optional[int], tolerance_ns: int
    ) -> Optional[Any]:
        """Find synchronized value in pre-computed snapshot (fallback - creates timestamps)."""
        if not snapshot or ref_timestamp_ns is None:
            return None
        timestamps = [p.timestamp_ns for p in snapshot]
        return self._find_synchronized_value_with_timestamps(snapshot, timestamps, ref_timestamp_ns, tolerance_ns)
    
    def _find_synchronized_value_with_timestamps(
        self, snapshot: List[DataPoint], timestamps: Optional[List[int]], ref_timestamp_ns: Optional[int], tolerance_ns: int
    ) -> Optional[Any]:
        """Find synchronized value using pre-computed timestamps list."""
        return self._find_synchronized_fast(snapshot, timestamps, ref_timestamp_ns)
    
    def _find_synchronized_fast(
        self, snapshot: List[DataPoint], timestamps: Optional[List[int]], ref_timestamp_ns: Optional[int], tolerance_ns: int = 1000
    ) -> Optional[Any]:
        """Fast synchronized value finder (inlined for hot loop)."""
        if not snapshot or ref_timestamp_ns is None:
            return None
        
        # If timestamps not provided, create them (fallback)
        if timestamps is None:
            timestamps = [p.timestamp_ns for p in snapshot]
        
        # For small datasets, linear search is faster
        if len(snapshot) < 50:
            for i, point in enumerate(snapshot):
                if abs(timestamps[i] - ref_timestamp_ns) <= tolerance_ns:
                    return point.value
            return None
        
        # For larger datasets, use binary search on pre-computed timestamps
        idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
        
        # Check points around the insertion point (within tolerance)
        for i in [idx, idx - 1, idx + 1]:
            if 0 <= i < len(snapshot):
                if abs(timestamps[i] - ref_timestamp_ns) <= tolerance_ns:
                    return snapshot[i].value
        return None
    
    def _interpolate_fast(
        self, snapshot: List[DataPoint], elapsed_times: Optional[List[float]], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Fast interpolate (inlined for hot loop)."""
        if not snapshot or not elapsed_times:
            return None
        
        if len(snapshot) < 50:
            # Linear search for small datasets
            before_point = None
            after_point = None
            before_diff = float('inf')
            after_diff = float('inf')
            
            for i, point in enumerate(snapshot):
                diff = elapsed_times[i] - target_elapsed
                if diff <= 0 and abs(diff) < before_diff:
                    before_point = point
                    before_diff = abs(diff)
                elif diff > 0 and diff < after_diff:
                    after_point = point
                    after_diff = diff
            
            if before_point and after_point:
                t1, v1 = before_point.elapsed_time, before_point.value
                t2, v2 = after_point.elapsed_time, after_point.value
                if abs(t2 - t1) < 1e-9:
                    return v1
                try:
                    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                        return v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                    else:
                        return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
                except (TypeError, ValueError):
                    return v1
            elif before_point:
                return before_point.value
            elif after_point:
                return after_point.value
            return None
        
        # Binary search using pre-computed elapsed_times
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Optimize: check tolerance before accessing points
        before_idx = idx - 1
        after_idx = idx
        before_valid = before_idx >= 0 and abs(elapsed_times[before_idx] - target_elapsed) <= tolerance
        after_valid = after_idx < len(snapshot) and abs(elapsed_times[after_idx] - target_elapsed) <= tolerance
        
        if before_valid and after_valid:
            before_point = snapshot[before_idx]
            after_point = snapshot[after_idx]
            t1, v1 = before_point.elapsed_time, before_point.value
            t2, v2 = after_point.elapsed_time, after_point.value
            if abs(t2 - t1) < 1e-9:
                return v1
            try:
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    return v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                else:
                    return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
            except (TypeError, ValueError):
                return v1
        elif before_valid:
            return snapshot[before_idx].value
        elif after_valid:
            return snapshot[after_idx].value
        return None
    
    def _find_nearest_fast(
        self, snapshot: List[DataPoint], elapsed_times: Optional[List[float]], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Fast nearest finder (inlined for hot loop)."""
        if not snapshot or not elapsed_times:
            return None
        
        if len(snapshot) < 50:
            # Linear search for small datasets
            closest = None
            min_diff = float('inf')
            for i, point in enumerate(snapshot):
                diff = abs(elapsed_times[i] - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = point.value
            return closest
        
        # Binary search using pre-computed elapsed_times
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Optimize: check both candidates and pick closest
        closest = None
        min_diff = float('inf')
        
        if idx < len(snapshot):
            diff = abs(elapsed_times[idx] - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = snapshot[idx].value
        
        if idx > 0:
            diff = abs(elapsed_times[idx - 1] - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                closest = snapshot[idx - 1].value
        
        return closest
    
    def _interpolate_value_in_snapshot(
        self, snapshot: List[DataPoint], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Interpolate value in pre-computed snapshot (fallback method)."""
        if not snapshot:
            return None
        elapsed_times = [p.elapsed_time for p in snapshot]
        return self._interpolate_value_in_snapshot_with_elapsed(snapshot, elapsed_times, target_elapsed, tolerance)
    
    def _interpolate_value_in_snapshot_with_elapsed(
        self, snapshot: List[DataPoint], elapsed_times: List[float], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Interpolate value using pre-computed snapshot and elapsed_times list."""
        if not snapshot or not elapsed_times:
            return None
        
        if len(snapshot) < 50:
            # Linear search for small datasets
            before_point = None
            after_point = None
            before_diff = float('inf')
            after_diff = float('inf')
            
            for i, point in enumerate(snapshot):
                diff = elapsed_times[i] - target_elapsed
                if diff <= 0 and abs(diff) < before_diff:
                    before_point = point
                    before_diff = abs(diff)
                elif diff > 0 and diff < after_diff:
                    after_point = point
                    after_diff = diff
            
            if before_point and after_point:
                t1, v1 = before_point.elapsed_time, before_point.value
                t2, v2 = after_point.elapsed_time, after_point.value
                if abs(t2 - t1) < 1e-9:
                    return v1
                try:
                    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                        return v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                    else:
                        return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
                except (TypeError, ValueError):
                    return v1
            elif before_point:
                return before_point.value
            elif after_point:
                return after_point.value
            return None
        
        # Binary search using pre-computed elapsed_times
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        before_point = snapshot[idx - 1] if idx > 0 and abs(elapsed_times[idx - 1] - target_elapsed) <= tolerance else None
        after_point = snapshot[idx] if idx < len(snapshot) and abs(elapsed_times[idx] - target_elapsed) <= tolerance else None
        
        if before_point and after_point:
            t1, v1 = before_point.elapsed_time, before_point.value
            t2, v2 = after_point.elapsed_time, after_point.value
            if abs(t2 - t1) < 1e-9:
                return v1
            try:
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    return v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                else:
                    return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
            except (TypeError, ValueError):
                return v1
        elif before_point:
            return before_point.value
        elif after_point:
            return after_point.value
        return None
    
    def _find_nearest_value_in_snapshot(
        self, snapshot: List[DataPoint], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Find nearest value in pre-computed snapshot (fallback method)."""
        if not snapshot:
            return None
        elapsed_times = [p.elapsed_time for p in snapshot]
        return self._find_nearest_value_in_snapshot_with_elapsed(snapshot, elapsed_times, target_elapsed, tolerance)
    
    def _find_nearest_value_in_snapshot_with_elapsed(
        self, snapshot: List[DataPoint], elapsed_times: List[float], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Find nearest value using pre-computed snapshot and elapsed_times list."""
        if not snapshot or not elapsed_times:
            return None
        
        if len(snapshot) < 50:
            # Linear search for small datasets
            closest = None
            min_diff = float('inf')
            for i, point in enumerate(snapshot):
                diff = abs(elapsed_times[i] - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = point.value
            return closest
        
        # Binary search using pre-computed elapsed_times
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        candidates = []
        if idx < len(snapshot):
            candidates.append((elapsed_times[idx], snapshot[idx]))
        if idx > 0:
            candidates.append((elapsed_times[idx - 1], snapshot[idx - 1]))
        
        closest = None
        min_diff = float('inf')
        for elapsed, point in candidates:
            diff = abs(elapsed - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = point.value
        
        return closest
    
    def _find_point_at_time(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Tuple[Any, float, int]]:
        """Find data point closest to target elapsed time using binary search.
        
        Returns:
            Tuple of (value, elapsed_time, timestamp_ns) or None
        """
        if not data_points:
            return None
        
        # For small datasets, linear search is faster due to cache locality
        count = len(data_points) if hasattr(data_points, '__len__') else None
        if count is not None and count < 50:
            # Linear search for very small datasets
            closest = None
            min_diff = float('inf')
            for point in data_points:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = (point.value, point.elapsed_time, point.timestamp_ns)
            return closest
        
        # For larger datasets, create snapshot and use binary search
        # Optimize: only create snapshot if needed (deque doesn't support indexing)
        if hasattr(data_points, '__getitem__'):
            # It's already a list-like structure that supports indexing
            snapshot = data_points
        else:
            # It's a deque, need to create snapshot
            # For very large datasets, filter to time window first
            if count is not None and count > 10000:
                time_window = max(tolerance * 10, 0.01)
                min_time = target_elapsed - time_window
                max_time = target_elapsed + time_window
                snapshot = [p for p in data_points if min_time <= p.elapsed_time <= max_time]
            else:
                snapshot = list(data_points)
        
        if not snapshot:
            return None
        
        # Binary search using bisect
        # Create elapsed_times list only once for this snapshot
        # For very small snapshots, just check all points
        if len(snapshot) < 10:
            closest = None
            min_diff = float('inf')
            for point in snapshot:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = (point.value, point.elapsed_time, point.timestamp_ns)
            return closest
        
        # Use bisect on elapsed_times list
        elapsed_times = [p.elapsed_time for p in snapshot]
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
                closest = (point.value, point.elapsed_time, point.timestamp_ns)
        
        return closest
    
    def _find_synchronized_value(
        self,
        data_points: List[DataPoint],
        ref_timestamp_ns: Optional[int],
        tolerance_ns: int = 1000,
    ) -> Optional[Any]:
        """Find value with exact timestamp match using binary search (for synchronized channels).
        
        Args:
            data_points: List or deque of data points to search
            ref_timestamp_ns: Reference timestamp in nanoseconds
            tolerance_ns: Tolerance in nanoseconds
            
        Returns:
            Value if found within tolerance, None otherwise
        """
        if ref_timestamp_ns is None:
            return None
        
        # For small datasets, linear search is faster
        count = len(data_points) if hasattr(data_points, '__len__') else None
        if count is not None and count < 50:
            for point in data_points:
                if abs(point.timestamp_ns - ref_timestamp_ns) <= tolerance_ns:
                    return point.value
            return None
        
        # For larger datasets, create snapshot and use binary search
        if hasattr(data_points, '__getitem__'):
            snapshot = data_points
        else:
            # It's a deque, need to create snapshot
            if count is not None and count > 10000:
                window_ns = max(tolerance_ns * 10, 10000)
                min_timestamp_ns = ref_timestamp_ns - window_ns
                max_timestamp_ns = ref_timestamp_ns + window_ns
                snapshot = [p for p in data_points if min_timestamp_ns <= p.timestamp_ns <= max_timestamp_ns]
            else:
                snapshot = list(data_points)
        
        if not snapshot:
            return None
        
        # Binary search on timestamps
        timestamps = [p.timestamp_ns for p in snapshot]
        idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
        
        # Check points around the insertion point (within tolerance)
        candidates = []
        if idx < len(snapshot):
            candidates.append(snapshot[idx])
        if idx > 0:
            candidates.append(snapshot[idx - 1])
        if idx + 1 < len(snapshot):
            candidates.append(snapshot[idx + 1])
        
        # Find the first candidate within tolerance
        for point in candidates:
            if abs(point.timestamp_ns - ref_timestamp_ns) <= tolerance_ns:
                return point.value
        
        return None
    
    def _interpolate_value(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Any]:
        """Interpolate value at target time using binary search (for interpolated channels).
        
        Args:
            data_points: List or deque of data points to search
            target_elapsed: Target elapsed time
            tolerance: Maximum time difference for interpolation
            
        Returns:
            Interpolated value or None
        """
        if not data_points:
            return None
        
        # For small datasets, linear search is faster
        count = len(data_points) if hasattr(data_points, '__len__') else None
        if count is not None and count < 50:
            # Linear search for very small datasets
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
            
            # Interpolate if we have both points
            if before_point and after_point:
                t1, v1 = before_point.elapsed_time, before_point.value
                t2, v2 = after_point.elapsed_time, after_point.value
                if abs(t2 - t1) < 1e-9:
                    return v1
                try:
                    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                        interpolated = v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                        return interpolated
                    else:
                        return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
                except (TypeError, ValueError):
                    return v1
            elif before_point:
                return before_point.value
            elif after_point:
                return after_point.value
            return None
        
        # For larger datasets, create snapshot and use binary search
        if hasattr(data_points, '__getitem__'):
            snapshot = data_points
        else:
            # It's a deque, need to create snapshot
            if count is not None and count > 10000:
                time_window = max(tolerance * 10, 0.01)
                min_time = target_elapsed - time_window
                max_time = target_elapsed + time_window
                snapshot = [p for p in data_points if min_time <= p.elapsed_time <= max_time]
            else:
                snapshot = list(data_points)
        
        if not snapshot:
            return None
        
        # Binary search for large datasets
        elapsed_times = [p.elapsed_time for p in snapshot]
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Find points before and after target
        before_point = None
        after_point = None
        
        # Point before (or at) target
        if idx > 0:
            before_point = snapshot[idx - 1]
            if abs(before_point.elapsed_time - target_elapsed) > tolerance:
                before_point = None
        
        # Point after target
        if idx < len(snapshot):
            after_point = snapshot[idx]
            if abs(after_point.elapsed_time - target_elapsed) > tolerance:
                after_point = None
        
        # Also check point at idx-2 and idx+1 for better interpolation
        if idx > 1 and before_point is None:
            candidate = snapshot[idx - 2]
            if abs(candidate.elapsed_time - target_elapsed) <= tolerance:
                before_point = candidate
        
        if idx + 1 < len(snapshot) and after_point is None:
            candidate = snapshot[idx + 1]
            if abs(candidate.elapsed_time - target_elapsed) <= tolerance:
                after_point = candidate
        
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
            data_points: List or deque of data points to search
            target_elapsed: Target elapsed time
            tolerance: Maximum time difference
            
        Returns:
            Nearest value or None
        """
        if not data_points:
            return None
        
        # For small datasets, linear search is faster
        count = len(data_points) if hasattr(data_points, '__len__') else None
        if count is not None and count < 50:
            # Linear search for very small datasets
            closest = None
            min_diff = float('inf')
            for point in data_points:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = point.value
            return closest
        
        # For larger datasets, create snapshot and use binary search
        if hasattr(data_points, '__getitem__'):
            snapshot = data_points
        else:
            # It's a deque, need to create snapshot
            if count is not None and count > 10000:
                time_window = max(tolerance * 10, 0.01)
                min_time = target_elapsed - time_window
                max_time = target_elapsed + time_window
                snapshot = [p for p in data_points if min_time <= p.elapsed_time <= max_time]
            else:
                snapshot = list(data_points)
        
        if not snapshot:
            return None
        
        # Binary search for large datasets
        elapsed_times = [p.elapsed_time for p in snapshot]
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
        self._last_built_time = None
        self._snapshot_cache.clear()
        self._ref_channel_cache = None
    
    def rebuild(self) -> None:
        """Rebuild the virtual database from the IO database.
        
        This method is incremental - it only builds new rows since the last build,
        making it much more efficient than a full rebuild.
        
        Will not create rows until all channels have at least one data point (primed).
        """
        # Use cached reference channel if available, otherwise get it
        if self._ref_channel_cache is None:
            self._ref_channel_cache = self.io_database.get_channel(self.reference_channel)
        ref_channel = self._ref_channel_cache
        
        if not ref_channel or ref_channel.count == 0:
            return  # No data to build from
        
        # For incremental rebuild, we only need the last data point to check if there's new data
        # We can avoid creating a full snapshot if we just need to check the last point
        if not ref_channel.data_points:
            return
        
        # Get last data point without full snapshot (more efficient)
        # Access last element directly from deque (O(1) operation)
        last_data_point = ref_channel.data_points[-1]
        last_elapsed = last_data_point.elapsed_time
        
        # If not built yet, do a full build
        if not self._built or self._last_built_time is None:
            self.clear()
            self.build()  # build() will check for priming
            return
        
        # Check if all channels are primed (have at least one data point)
        # This is important for incremental rebuilds - we may have started before all channels had data
        if not self._is_primed():
            return  # Not all channels have data yet, wait for priming
        
        # If no new data, skip (with small tolerance for floating point)
        if last_elapsed <= self._last_built_time + 1e-9:
            return
        
        # Calculate row interval
        row_interval = 1.0 / self.sampling_rate
        
        # Start from the next row after the last built time
        # Since _last_built_time is the timestamp of the last row built,
        # we simply add row_interval to get the next row time
        start_time = self._last_built_time + row_interval
        
        # Pre-compute snapshots for incremental rebuild (similar to build)
        # This avoids repeated deque-to-list conversions during the rebuild loop
        # Also pre-compute elapsed_times and timestamps lists for binary search efficiency
        channel_snapshots: Dict[str, List[DataPoint]] = {}
        channel_elapsed_times: Dict[str, List[float]] = {}
        channel_timestamps: Dict[str, List[int]] = {}  # For synchronized channels
        for col_def in self.columns:
            if col_def.channel_path and col_def.channel_path not in channel_snapshots:
                channel_data = self.io_database.get_channel(col_def.channel_path)
                if channel_data:
                    snapshot = list(channel_data.data_points)
                    channel_snapshots[col_def.channel_path] = snapshot
                    # Pre-compute elapsed_times for binary search
                    channel_elapsed_times[col_def.channel_path] = [p.elapsed_time for p in snapshot]
                    # Pre-compute timestamps for synchronized channels
                    if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                        channel_timestamps[col_def.channel_path] = [p.timestamp_ns for p in snapshot]
        
        # Also snapshot reference channel and pre-compute its elapsed_times
        ref_snapshot = list(ref_channel.data_points)
        ref_elapsed_times = [p.elapsed_time for p in ref_snapshot]
        
        # Pre-index column data to avoid dictionary lookups in hot loop
        # Cache all attributes to reduce attribute access overhead
        column_data = []
        for col_def in self.columns:
            col_name = col_def.name
            col_policy = col_def.policy
            converter = col_def.converter
            if col_def.channel_path is None:
                column_data.append((col_name, col_policy, converter, None, None, None))
            else:
                snapshot = channel_snapshots.get(col_def.channel_path)
                elapsed_times = channel_elapsed_times.get(col_def.channel_path) if snapshot else None
                timestamps = channel_timestamps.get(col_def.channel_path) if col_policy == ChannelPolicy.SYNCHRONIZED else None
                column_data.append((col_name, col_policy, converter, snapshot, elapsed_times, timestamps))
        
        # Build only new rows incrementally
        # Limit rows per rebuild to avoid blocking (process in batches)
        # At 6 kHz, we might accumulate many rows between rebuilds
        # Increased limit for faster processing when catching up
        max_rows_per_rebuild = 10000  # Process up to 10k rows per rebuild call
        rows_built = 0
        
        # Use incremental search - start from a reasonable position using binary search
        ref_search_idx = 0
        if ref_elapsed_times and self._last_built_time is not None:
            # Use binary search to find starting position
            ref_search_idx = bisect.bisect_left(ref_elapsed_times, self._last_built_time)
            # Start a bit before to be safe
            ref_search_idx = max(0, ref_search_idx - 10)
        
        # Pre-compute tolerance values
        ref_tolerance = row_interval * 0.5
        interp_tolerance = row_interval * 2.0
        
        # Track last known values for forward-fill (for INTERPOLATED channels)
        # Initialize from existing rows if available
        last_known_values: Dict[str, Any] = {}
        if self.rows:
            # Get last known values from the last row
            last_row = self.rows[-1]
            for col_name, col_policy, _, _, _, _ in column_data:
                if col_policy == ChannelPolicy.INTERPOLATED and col_name in last_row.data:
                    value = last_row.data[col_name]
                    if value is not None:
                        last_known_values[col_name] = value
        
        current_time = start_time
        last_row_time = self._last_built_time
        while current_time <= last_elapsed and rows_built < max_rows_per_rebuild:
            # Find reference point using optimized incremental search
            # Use binary search with incremental start position for better performance
            ref_timestamp_ns = None
            if ref_snapshot and ref_elapsed_times:
                # Use binary search starting from last known position
                idx = bisect.bisect_left(ref_elapsed_times, current_time, lo=ref_search_idx)
                # Check both candidates (idx and idx-1) for closest match
                best_idx = None
                best_diff = ref_tolerance + 1.0
                
                if idx < len(ref_elapsed_times):
                    diff = abs(ref_elapsed_times[idx] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx
                
                if idx > 0:
                    diff = abs(ref_elapsed_times[idx - 1] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx - 1
                
                if best_idx is not None:
                    point = ref_snapshot[best_idx]
                    ref_timestamp_ns = point.timestamp_ns
                    ref_search_idx = best_idx
                else:
                    # No match found, advance search index to avoid rechecking
                    ref_search_idx = max(0, idx - 1)
            
            # Process columns (fully inlined for speed)
            # Pre-allocate dictionary with known size to avoid rehashing
            row_data = dict.fromkeys((col_name for col_name, _, _, _, _, _ in column_data), None)
            
            # Inline all policy-specific matching to avoid function call overhead
            for col_name, col_policy, converter, snapshot, elapsed_times, timestamps in column_data:
                if snapshot is None:
                    continue
                
                raw_value = None
                
                # Inline SYNCHRONIZED policy
                if col_policy == ChannelPolicy.SYNCHRONIZED:
                    if ref_timestamp_ns is not None and timestamps:
                        if len(snapshot) < 50:
                            # Linear search for small datasets
                            for i, point in enumerate(snapshot):
                                if abs(timestamps[i] - ref_timestamp_ns) <= 1000:
                                    raw_value = point.value
                                    break
                        else:
                            # Binary search for larger datasets
                            ts_idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
                            for i in [ts_idx, ts_idx - 1, ts_idx + 1]:
                                if 0 <= i < len(snapshot):
                                    if abs(timestamps[i] - ref_timestamp_ns) <= 1000:
                                        raw_value = snapshot[i].value
                                        break
                
                # Inline INTERPOLATED policy
                elif col_policy == ChannelPolicy.INTERPOLATED:
                    if elapsed_times:
                        if len(snapshot) < 50:
                            # Linear search for small datasets
                            before_point = None
                            after_point = None
                            before_diff = float('inf')
                            after_diff = float('inf')
                            for i, point in enumerate(snapshot):
                                diff = elapsed_times[i] - current_time
                                if diff <= 0 and abs(diff) < before_diff:
                                    before_point = point
                                    before_diff = abs(diff)
                                elif diff > 0 and diff < after_diff:
                                    after_point = point
                                    after_diff = diff
                            
                            if before_point and after_point:
                                t1, v1 = before_point.elapsed_time, before_point.value
                                t2, v2 = after_point.elapsed_time, after_point.value
                                if abs(t2 - t1) >= 1e-9:
                                    try:
                                        if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                                            raw_value = v1 + (v2 - v1) * (current_time - t1) / (t2 - t1)
                                        else:
                                            raw_value = v1 if abs(current_time - t1) < abs(current_time - t2) else v2
                                    except (TypeError, ValueError):
                                        raw_value = v1
                                else:
                                    raw_value = v1
                            elif before_point:
                                raw_value = before_point.value
                            elif after_point:
                                raw_value = after_point.value
                        else:
                            # Binary search for larger datasets
                            idx = bisect.bisect_left(elapsed_times, current_time)
                            before_idx = idx - 1
                            after_idx = idx
                            before_valid = before_idx >= 0 and abs(elapsed_times[before_idx] - current_time) <= interp_tolerance
                            after_valid = after_idx < len(snapshot) and abs(elapsed_times[after_idx] - current_time) <= interp_tolerance
                            
                            if before_valid and after_valid:
                                before_point = snapshot[before_idx]
                                after_point = snapshot[after_idx]
                                t1, v1 = before_point.elapsed_time, before_point.value
                                t2, v2 = after_point.elapsed_time, after_point.value
                                if abs(t2 - t1) >= 1e-9:
                                    try:
                                        if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                                            raw_value = v1 + (v2 - v1) * (current_time - t1) / (t2 - t1)
                                        else:
                                            raw_value = v1 if abs(current_time - t1) < abs(current_time - t2) else v2
                                    except (TypeError, ValueError):
                                        raw_value = v1
                                else:
                                    raw_value = v1
                            elif before_valid:
                                raw_value = snapshot[before_idx].value
                            elif after_valid:
                                raw_value = snapshot[after_idx].value
                
                # Inline ASYNCHRONOUS policy
                elif col_policy == ChannelPolicy.ASYNCHRONOUS:
                    if elapsed_times:
                        if len(snapshot) < 50:
                            # Linear search for small datasets
                            closest = None
                            min_diff = float('inf')
                            for i, point in enumerate(snapshot):
                                diff = abs(elapsed_times[i] - current_time)
                                if diff < min_diff and diff <= interp_tolerance:
                                    min_diff = diff
                                    closest = point.value
                            raw_value = closest
                        else:
                            # Binary search for larger datasets
                            idx = bisect.bisect_left(elapsed_times, current_time)
                            closest = None
                            min_diff = float('inf')
                            
                            if idx < len(snapshot):
                                diff = abs(elapsed_times[idx] - current_time)
                                if diff < min_diff and diff <= interp_tolerance:
                                    min_diff = diff
                                    closest = snapshot[idx].value
                            
                            if idx > 0:
                                diff = abs(elapsed_times[idx - 1] - current_time)
                                if diff < min_diff and diff <= interp_tolerance:
                                    closest = snapshot[idx - 1].value
                            
                            raw_value = closest
                
                # Apply converter
                if raw_value is not None:
                    try:
                        converted_value = converter(raw_value)
                        row_data[col_name] = converted_value
                        # Update last known value for forward-fill
                        if col_policy == ChannelPolicy.INTERPOLATED:
                            last_known_values[col_name] = converted_value
                    except Exception:
                        pass
                elif col_policy == ChannelPolicy.INTERPOLATED and col_name in last_known_values:
                    # Forward-fill: use last known value if interpolation failed
                    row_data[col_name] = last_known_values[col_name]
            
            # Create virtual row
            self.rows.append(VirtualRow(timestamp=current_time, data=row_data))
            last_row_time = current_time
            rows_built += 1
            
            # Advance to next row time
            current_time += row_interval
        
        # Update last built time to the timestamp of the last row built
        # This ensures we don't skip rows in the next incremental build
        if last_row_time is not None:
            self._last_built_time = last_row_time
    
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
