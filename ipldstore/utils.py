"""
Some utilities.
"""

from io import BufferedIOBase, BytesIO
from typing import List, Union

from multiformats import CID
from typing_validation import validate
from typing_extensions import TypeGuard

StreamLike = Union[BufferedIOBase, bytes]

def ensure_stream(stream_or_bytes: StreamLike) -> BufferedIOBase:
    validate(stream_or_bytes, StreamLike)
    if isinstance(stream_or_bytes, bytes):
        return BytesIO(stream_or_bytes)
    else:
        return stream_or_bytes


def is_cid_list(os: List[object]) -> TypeGuard[List[CID]]:
    return all(isinstance(o, CID) for o in os)
