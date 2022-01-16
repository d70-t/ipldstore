from abc import ABC, abstractmethod
from typing import MutableMapping, Optional, Union, overload, Iterator, MutableSet
from io import BufferedIOBase, BytesIO

from multiformats import CID, multicodec, multibase, multihash, varint
import dag_cbor
from dag_cbor.encoding import EncodableType as DagCborEncodable
from typing_validation import validate


ValueType = Union[bytes, DagCborEncodable]

RawCodec = multicodec.get("raw")
DagCborCodec = multicodec.get("dag-cbor")
DagPbCodec = multicodec.get("dag-pb")
Sha256Codec = multicodec.get("sha2-256")


class ContentAddressableStore(ABC):
    @abstractmethod
    def get_raw(self, cid: CID) -> bytes:
        ...

    def get(self, cid: CID) -> ValueType:
        value = self.get_raw(cid)
        if cid.codec == RawCodec:
            return value
        elif cid.codec == DagCborCodec:
            return dag_cbor.decode(value)
        else:
            raise ValueError(f"can't decode CID's codec '{cid.codec.name}'")

    def __contains__(self, cid: CID) -> bool:
        try:
            self.get_raw(cid)
        except KeyError:
            return False
        else:
            return True

    @abstractmethod
    def put_raw(self,
                raw_value: bytes,
                codec: Union[str, int, multicodec.Multicodec]) -> CID:
        ...

    def put(self, value: ValueType) -> CID:
        validate(value, ValueType)
        if isinstance(value, bytes):
            return self.put_raw(value, RawCodec)
        else:
            return self.put_raw(dag_cbor.encode(value), DagCborCodec)

    @overload
    def to_car(self, root: CID, stream: BufferedIOBase) -> int:
        ...

    @overload
    def to_car(self, root: CID, stream: None = None) -> bytes:
        ...

    def to_car(self, root: CID, stream: Optional[BufferedIOBase] = None) -> Union[bytes, int]:
        validate(root, CID)
        validate(stream, Optional[BufferedIOBase])

        if stream is None:
            buffer = BytesIO()
            stream = buffer
            return_bytes = True
        else:
            return_bytes = False

        bytes_written = 0
        header = dag_cbor.encode({"version": 1, "roots": [root]})
        bytes_written += stream.write(varint.encode(len(header)))
        bytes_written += stream.write(header)
        bytes_written += self._to_car(root, stream, set())

        if return_bytes:
            return buffer.getvalue()
        else:
            return bytes_written

    def _to_car(self,
                root: CID,
                stream: BufferedIOBase,
                already_written: MutableSet[CID]) -> int:
        """
            makes a CAR without the header
        """
        bytes_written = 0

        if not root in already_written:
            data = self.get_raw(root)
            cid_bytes = bytes(root)
            bytes_written += stream.write(varint.encode(len(cid_bytes) + len(data)))
            bytes_written += stream.write(cid_bytes)
            bytes_written += stream.write(data)
            already_written.add(root)

            if root.codec == DagCborCodec:
                value = dag_cbor.decode(data)
                for child in iter_links(value):
                    bytes_written += self._to_car(child, stream, already_written)
        return bytes_written

    def import_car(self, stream_or_bytes: Union[BufferedIOBase, bytes]) -> None:
        validate(stream_or_bytes, Union[BufferedIOBase, bytes])
        if isinstance(stream_or_bytes, bytes):
            stream: BufferedIOBase = BytesIO(stream_or_bytes)
        else:
            stream = stream_or_bytes
        header_size = varint.decode(stream)
        header = dag_cbor.decode(stream.read(header_size))

        while True:
            try:
                block_size = varint.decode(stream)
            except ValueError:
                # stream has likely been consumed entirely
                break
            data = stream.read(block_size)
            if data[0] == 0x12 and data[1] == 0x20:
                # this is CIDv0
                cid_version = 0
                default_base = "base58btc"
                cid_codec = DagPbCodec
                hash_codec = Sha256Codec
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

            self.put_raw(bytes(data), cid.codec)
        


class MappingCAStore(ContentAddressableStore):
    def __init__(self,
                 mapping: Optional[MutableMapping] = None,
                 default_hash: Union[str, int, multicodec.Multicodec, multihash.Multihash] = "sha2-256",
                 default_base: Union[str, multibase.Multibase] = "base32",
                 ):
        validate(mapping, Optional[MutableMapping])
        validate(default_hash, Union[str, int, multicodec.Multicodec, multihash.Multihash])
        validate(default_base, Union[str, multibase.Multibase])

        if mapping is None:
            self._mapping : MutableMapping = {}
        else:
            self._mapping = mapping

        if isinstance(default_hash, multihash.Multihash):
            self._default_hash = default_hash
        else:
            self._default_hash = multihash.get(default_hash)

        if isinstance(default_base, multibase.Multibase):
            self._default_base = default_base
        else:
            self._default_base = multibase.get(default_base)

    def get_raw(self, cid: CID) -> bytes:
        validate(cid, CID)
        key = cid.set(base=self._default_base, version=1).digest
        return self._mapping[key]

    def put_raw(self,
                raw_value: bytes,
                codec: Union[str, int, multicodec.Multicodec]) -> CID:
        validate(raw_value, bytes)
        validate(codec, Union[str, int, multicodec.Multicodec])

        h = self._default_hash.digest(raw_value)
        cid = CID(self._default_base, 1, codec, h)
        key = cid.digest
        self._mapping[key] = raw_value
        return cid


def iter_links(o: DagCborEncodable) -> Iterator[CID]:
    if isinstance(o, dict):
        for v in o.values():
            yield from iter_links(v)
    elif isinstance(o, list):
        for v in o:
            yield from iter_links(v)
    elif isinstance(o, CID):
        yield o


__all__ = ["ContentAddressableStore", "MappingCAStore", "iter_links"]
