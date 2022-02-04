"""
CAR handling functions.
"""

from io import BufferedIOBase
from typing import List, Optional, Tuple

import dag_cbor
from multiformats import CID, varint, multicodec, multihash

from .utils import is_cid_list, StreamLike, ensure_stream


def decode_car_header(stream: BufferedIOBase) -> List[CID]:
    """
    Decodes a CAR header and returns the list of contained roots.
    """
    header_size = varint.decode(stream)
    header = dag_cbor.decode(stream.read(header_size))
    if not isinstance(header, dict):
        raise ValueError("no valid CAR header found")
    if header["version"] != 1:
        raise ValueError("CAR is not version 1")
    roots = header["roots"]
    if not isinstance(roots, list):
        raise ValueError("CAR header doesn't contain roots")
    if not is_cid_list(roots):
        raise ValueError("CAR roots do not only contain CIDs")
    return roots


def decode_raw_car_block(stream: BufferedIOBase) -> Optional[Tuple[CID, bytes]]:
    try:
        block_size = varint.decode(stream)
    except ValueError:
        # stream has likely been consumed entirely
        return None

    data = stream.read(block_size)
    # as the size of the CID is variable but not explicitly given in
    # the CAR format, we need to partially decode each CID to determine
    # its size and the location of the payload data
    if data[0] == 0x12 and data[1] == 0x20:
        # this is CIDv0
        cid_version = 0
        default_base = "base58btc"
        cid_codec: Union[int, multicodec.Multicodec] = DagPbCodec
        hash_codec: Union[int, multihash.Multihash] = Sha256Hash
        cid_digest = data[2:34]
        data = data[34:]
    else:
        # this is CIDv1(+)
        cid_version, _, data = varint.decode_raw(data)
        if cid_version != 1:
            raise ValueError(f"CIDv{cid_version} is currently not supported")
        default_base = "base32"
        cid_codec, _, data = multicodec.unwrap_raw(data)
        hash_codec, _, data = varint.decode_raw(data)
        digest_size, _, data = varint.decode_raw(data)
        cid_digest = data[:digest_size]
        data = data[digest_size:]
    cid = CID(default_base, cid_version, cid_codec, (hash_codec, cid_digest))

    if not cid.hashfun.digest(data) == cid.digest:
        raise ValueError(f"CAR is corrupted. Entry '{cid}' could not be verified")

    return cid, bytes(data)


def read_car(stream_or_bytes: StreamLike):
    """
    Reads a CAR.

    Returns
    -------
    roots : List[CID]
        Roots as given by the CAR header
    blocks : Iterator[Tuple[cid, BytesLike]]
        Iterator over all blocks contained in the CAR
    """
    stream = ensure_stream(stream_or_bytes)
    roots = decode_car_header(stream)
    def blocks():
        while (next_block := decode_raw_car_block(stream)) is not None:
            yield next_block
    return roots, blocks()
