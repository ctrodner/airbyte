#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, AsyncGenerator, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.async_cdk.streams.core_async import AsyncStream
from airbyte_cdk.sources.streams.core import StreamData


async def get_first_stream_slice(stream: AsyncStream) -> Optional[Mapping[str, Any]]:
    """
    Gets the first stream_slice from a given stream's stream_slices.
    :param stream: stream
    :raises StopAsyncIteration: if there is no first slice to return (the stream_slices generator is empty)
    :return: first stream slice from 'stream_slices' generator (`None` is a valid stream slice)
    """
    first_slice = await anext(
        stream.stream_slices(
            cursor_field=[stream.cursor_field]
            if isinstance(stream.cursor_field, str)
            else stream.cursor_field,
            sync_mode=SyncMode.full_refresh,
        )
    )
    return first_slice


async def get_first_record_for_slice(
    stream: AsyncStream, stream_slice: Optional[Mapping[str, Any]]
) -> AsyncGenerator[StreamData, None]:
    """
    Gets the first record for a stream_slice of a stream.
    :param stream: stream
    :param stream_slice: stream_slice
    :raises StopAsyncIteration: if there is no first record to return (the read_records generator is empty)
    :return: StreamData containing the first record in the slice
    """
    # We wrap the return output of read_records() because some implementations return types that are iterable,
    # but not iterators such as lists or tuples
    async for record in stream.read_records(
        sync_mode=SyncMode.full_refresh, stream_slice=stream_slice
    ):
        yield record
