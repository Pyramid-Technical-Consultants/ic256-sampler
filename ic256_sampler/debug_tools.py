"""Debugging tools for diagnosing device data and performance issues."""

from typing import Dict, List, Any
from .io_database import IODatabase, ChannelData, DataPoint
from .virtual_database import VirtualDatabase


def diagnose_io_database(io_database: IODatabase) -> Dict[str, Any]:
    """Diagnose IODatabase state and identify potential issues."""
    stats = io_database.get_statistics()
    all_channel_paths = io_database.get_all_channels()

    diagnosis = {
        'total_points': stats.get('total_data_points', 0),
        'total_channels': len(all_channel_paths),
        'channels': {},
        'issues': [],
        'warnings': [],
    }

    for channel_path in all_channel_paths:
        channel_data = io_database.get_channel(channel_path)
        if not channel_data:
            diagnosis['issues'].append(f"Channel {channel_path} is None")
            continue

        channel_info = {
            'count': channel_data.count,
            'first_timestamp': channel_data.first_timestamp,
            'last_timestamp': channel_data.last_timestamp,
            'data_points_type': type(channel_data.data_points).__name__,
            'data_points_len': len(channel_data.data_points) if hasattr(channel_data.data_points, '__len__') else 'unknown',
        }

        if channel_data.count != len(channel_data.data_points):
            diagnosis['warnings'].append(
                f"Channel {channel_path}: count ({channel_data.count}) != "
                f"data_points length ({len(channel_data.data_points)})"
            )

        if channel_data.count > 100000:
            diagnosis['warnings'].append(
                f"Channel {channel_path} has {channel_data.count} points - "
                f"may cause performance issues"
            )

        if channel_data.first_timestamp and channel_data.last_timestamp:
            if channel_data.last_timestamp < channel_data.first_timestamp:
                diagnosis['issues'].append(
                    f"Channel {channel_path}: last_timestamp < first_timestamp"
                )

        if channel_data.data_points:
            sample_points = []
            for i, point in enumerate(list(channel_data.data_points)[:5]):
                sample_points.append({
                    'value': str(point.value)[:50],
                    'timestamp_ns': point.timestamp_ns,
                    'elapsed_time': point.elapsed_time,
                })
            channel_info['sample_points'] = sample_points

        diagnosis['channels'][channel_path] = channel_info

    return diagnosis


def diagnose_virtual_database_build(
    virtual_db: VirtualDatabase,
    max_snapshot_size: int = 100000
) -> Dict[str, Any]:
    """Diagnose VirtualDatabase build state and identify bottlenecks."""
    diagnosis = {
        'reference_channel': virtual_db.reference_channel,
        'sampling_rate': virtual_db.sampling_rate,
        'columns_count': len(virtual_db.columns),
        'built': virtual_db._built,
        'row_count': len(virtual_db.rows) if virtual_db.rows else 0,
        'last_built_time': virtual_db._last_built_time,
        'snapshot_sizes': {},
        'issues': [],
        'warnings': [],
    }

    ref_channel = virtual_db.io_database.get_channel(virtual_db.reference_channel)
    if ref_channel:
        diagnosis['reference_channel_count'] = ref_channel.count
        if ref_channel.count > max_snapshot_size:
            diagnosis['warnings'].append(
                f"Reference channel has {ref_channel.count} points, "
                f"exceeds max_snapshot_size ({max_snapshot_size})"
            )

        if ref_channel.count > 0:
            first_elapsed = ref_channel.data_points[0].elapsed_time
            last_elapsed = ref_channel.data_points[-1].elapsed_time
            time_span = last_elapsed - first_elapsed
            estimated_rows = int(time_span * virtual_db.sampling_rate) + 1
            diagnosis['time_span'] = time_span
            diagnosis['estimated_rows'] = estimated_rows

            if estimated_rows > 100000:
                diagnosis['warnings'].append(
                    f"Estimated rows ({estimated_rows}) is very large - "
                    f"build may be slow"
                )
    else:
        diagnosis['issues'].append(
            f"Reference channel '{virtual_db.reference_channel}' not found"
        )

    for col_def in virtual_db.columns:
        if col_def.channel_path:
            channel = virtual_db.io_database.get_channel(col_def.channel_path)
            if channel:
                diagnosis['snapshot_sizes'][col_def.channel_path] = channel.count
                if channel.count > max_snapshot_size:
                    diagnosis['warnings'].append(
                        f"Channel {col_def.channel_path} has {channel.count} points, "
                        f"exceeds max_snapshot_size ({max_snapshot_size})"
                    )
            else:
                diagnosis['warnings'].append(
                    f"Channel {col_def.channel_path} not found in IO database"
                )

    return diagnosis


def validate_data_point(point: DataPoint, channel_path: str) -> List[str]:
    """Validate a single data point and return any issues found."""
    issues = []

    if point.timestamp_ns <= 0:
        issues.append(f"Invalid timestamp_ns: {point.timestamp_ns}")

    if point.timestamp_ns < 1e15:
        issues.append(
            f"Timestamp {point.timestamp_ns} seems too small for nanoseconds since 1970"
        )

    if point.elapsed_time < 0:
        issues.append(f"Negative elapsed_time: {point.elapsed_time}")

    if abs(point.elapsed_time) > 86400 * 365:
        issues.append(
            f"Elapsed time {point.elapsed_time} seems unreasonably large"
        )

    if point.value is None:
        issues.append("Data point value is None")

    return issues


def print_diagnosis(diagnosis: Dict[str, Any]) -> None:
    """Print diagnosis in a readable format."""
    print("\n" + "="*60)
    print("DIAGNOSIS REPORT")
    print("="*60)

    if 'total_points' in diagnosis:
        print(f"\nTotal Data Points: {diagnosis['total_points']:,}")
        print(f"Total Channels: {diagnosis['total_channels']}")

    if 'row_count' in diagnosis:
        print(f"\nVirtual Database Rows: {diagnosis['row_count']:,}")
        print(f"Built: {diagnosis.get('built', False)}")

    if diagnosis.get('issues'):
        print(f"\n[ISSUES] ({len(diagnosis['issues'])}):")
        for issue in diagnosis['issues']:
            print(f"  - {issue}")

    if diagnosis.get('warnings'):
        print(f"\n[WARNINGS] ({len(diagnosis['warnings'])}):")
        for warning in diagnosis['warnings']:
            print(f"  - {warning}")

    if diagnosis.get('channels'):
        print(f"\n[CHANNELS] ({len(diagnosis['channels'])}):")
        for channel_path, info in list(diagnosis['channels'].items())[:10]:
            print(f"  {channel_path}:")
            print(f"    Points: {info['count']:,}")
            if info.get('first_timestamp') and info.get('last_timestamp'):
                span = (info['last_timestamp'] - info['first_timestamp']) / 1e9
                print(f"    Time span: {span:.3f}s")

    if 'snapshot_sizes' in diagnosis:
        print(f"\n[SNAPSHOT SIZES]:")
        for channel_path, size in diagnosis['snapshot_sizes'].items():
            print(f"  {channel_path}: {size:,} points")

    print("\n" + "="*60)
