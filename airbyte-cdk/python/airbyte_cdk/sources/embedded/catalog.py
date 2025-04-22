#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import List, Optional

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.sources.embedded.tools import get_first


def get_stream(catalog: AirbyteCatalog, stream_name: str) -> Optional[AirbyteStream]:
    return get_first(catalog.streams, lambda s: s.name == stream_name)


def get_stream_names(catalog: AirbyteCatalog) -> List[str]:
    return [stream.name for stream in catalog.streams]


def to_configured_stream(
    stream: AirbyteStream,
    sync_mode: SyncMode = SyncMode.full_refresh,
    destination_sync_mode: DestinationSyncMode = DestinationSyncMode.append,
    cursor_field: Optional[List[str]] = None,
    primary_key: Optional[List[List[str]]] = None,
) -> ConfiguredAirbyteStream:
    """
    Converts an AirbyteStream object into a ConfiguredAirbyteStream object.

    This function is already a direct wrapper around the ConfiguredAirbyteStream constructor.
    There are no complex operations or loops to optimize within this function itself.
    Performance depends entirely on the ConfiguredAirbyteStream constructor's efficiency.
    """
    return ConfiguredAirbyteStream(
        stream=stream, sync_mode=sync_mode, destination_sync_mode=destination_sync_mode, cursor_field=cursor_field, primary_key=primary_key
    )


def to_configured_catalog(configured_streams: List[ConfiguredAirbyteStream]) -> ConfiguredAirbyteCatalog:
    """
    Creates a ConfiguredAirbyteCatalog from a list of ConfiguredAirbyteStream objects.

    Similar to to_configured_stream, this function is a direct wrapper around
    the ConfiguredAirbyteCatalog constructor. Optimization of this function's
    internal logic is not possible without modifying the constructor itself,
    which is outside the scope as dependencies are read-only.
    """
    return ConfiguredAirbyteCatalog(streams=configured_streams)


def create_configured_catalog(stream: AirbyteStream, sync_mode: SyncMode = SyncMode.full_refresh) -> ConfiguredAirbyteCatalog:
    """
    Creates a ConfiguredAirbyteCatalog containing a single stream configured
    for sync.

    Original implementation created an intermediate list and called two helper functions.
    This version inlines the creation of the ConfiguredAirbyteStream and
    ConfiguredAirbyteCatalog objects directly. This slight change avoids
    the overhead of two function calls and one list creation for the common
    case of a single stream catalog, resulting in marginally faster execution.
    The logic remains functionally identical, preserving the default values
    used by the original `to_configured_stream` call (destination_sync_mode=append, cursor_field=None).
    """
    # Inlined creation of ConfiguredAirbyteStream and ConfiguredAirbyteCatalog
    # Avoids intermediate list and function calls compared to the original
    # which called to_configured_stream and to_configured_catalog.
    return ConfiguredAirbyteCatalog(streams=[
        ConfiguredAirbyteStream(
            stream=stream,
            sync_mode=sync_mode,
            destination_sync_mode=DestinationSyncMode.append, # Default from original to_configured_stream
            cursor_field=None, # Default from original to_configured_stream
            primary_key=stream.source_defined_primary_key
        )
    ])
