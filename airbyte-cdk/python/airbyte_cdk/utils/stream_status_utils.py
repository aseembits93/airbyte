#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import List, Optional, Union

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStream,
    AirbyteStreamStatus,
    AirbyteStreamStatusReason,
    AirbyteStreamStatusTraceMessage,
    AirbyteTraceMessage,
    StreamDescriptor,
    TraceType,
)
from airbyte_cdk.models import Type as MessageType
import time


def as_airbyte_message(
    stream: Union[AirbyteStream, StreamDescriptor],
    current_status: AirbyteStreamStatus,
    reasons: Optional[List[AirbyteStreamStatusReason]] = None,
) -> AirbyteMessage:
    """
    Builds an AirbyteStreamStatusTraceMessage for the provided stream
    """
    # Use time.time() instead of datetime.now().timestamp() for better performance
    now_millis = time.time() * 1000.0
    
    # Access stream attributes directly to avoid redundant property lookups
    stream_name = stream.name
    stream_namespace = getattr(stream, 'namespace', None)
    
    # Create the trace message directly within the AirbyteMessage constructor
    return AirbyteMessage(
        type=MessageType.TRACE, 
        trace=AirbyteTraceMessage(
            type=TraceType.STREAM_STATUS,
            emitted_at=now_millis,
            stream_status=AirbyteStreamStatusTraceMessage(
                stream_descriptor=StreamDescriptor(name=stream_name, namespace=stream_namespace),
                status=current_status,
                reasons=reasons,
            ),
        )
    )
