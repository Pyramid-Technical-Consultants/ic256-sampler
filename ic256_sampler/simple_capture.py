"""Simple single-channel data capture for validation and testing."""

import time
from typing import List, Tuple, Dict, Any
from .igx_client import IGXWebsocketClient
from .io_database import IODatabase, ChannelData


def capture_single_channel(
    client: IGXWebsocketClient,
    channel_path: str,
    duration: float,
    stop_event: Any = None,
) -> List[Tuple[Any, int]]:
    """Capture all data points from a single channel."""
    channel = client.field(channel_path)
    client.sendSubscribeFields({channel: True})
    client.updateSubscribedFields()

    captured_data: List[Tuple[Any, int]] = []
    first_timestamp: int = None
    start_time = time.time()

    while True:
        if stop_event and stop_event.is_set():
            break
        if time.time() - start_time >= duration:
            break

        client.updateSubscribedFields()
        data = channel.getDatums()

        if not data:
            time.sleep(0.001)
            continue

        for data_point in data:
            if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                continue

            value = data_point[0]
            ts_raw = data_point[1]

            try:
                if isinstance(ts_raw, float):
                    if ts_raw < 1e12:
                        ts_ns = int(ts_raw * 1e9)
                    else:
                        ts_ns = int(ts_raw)
                elif isinstance(ts_raw, int):
                    ts_ns = ts_raw
                else:
                    continue
            except (ValueError, TypeError, OverflowError):
                continue

            if first_timestamp is None:
                first_timestamp = ts_ns

            captured_data.append((value, ts_ns))

        channel.clearDatums()
        time.sleep(0.001)

    return captured_data


def capture_to_database(
    client: IGXWebsocketClient,
    channel_paths: List[str],
    duration: float,
    database: IODatabase = None,
    stop_event: Any = None,
) -> IODatabase:
    """Capture data from multiple channels and store in IO database."""
    if database is None:
        database = IODatabase()

    channels = {}
    for path in channel_paths:
        database.add_channel(path)
        channels[path] = client.field(path)

    client.sendSubscribeFields({ch: True for ch in channels.values()})
    client.updateSubscribedFields()

    start_time = time.time()

    while True:
        if stop_event and stop_event.is_set():
            break
        if time.time() - start_time >= duration:
            break

        client.updateSubscribedFields()

        for channel_path, channel_field in channels.items():
            data = channel_field.getDatums()

            if not data:
                continue

            for data_point in data:
                if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                    continue

                value = data_point[0]
                ts_raw = data_point[1]

                try:
                    if isinstance(ts_raw, float):
                        if ts_raw < 1e12:
                            ts_ns = int(ts_raw * 1e9)
                        else:
                            ts_ns = int(ts_raw)
                    elif isinstance(ts_raw, int):
                        ts_ns = ts_raw
                    else:
                        continue
                except (ValueError, TypeError, OverflowError):
                    continue

                database.add_data_point(channel_path, value, ts_ns)

            channel_field.clearDatums()

        time.sleep(0.001)

    return database


def capture_single_channel_with_stats(
    client: IGXWebsocketClient,
    channel_path: str,
    duration: float,
    stop_event: Any = None,
) -> Dict[str, Any]:
    """Capture data from a single channel and return statistics."""
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
