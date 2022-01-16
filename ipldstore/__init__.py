from collections.abc import MutableMapping
from typing import Optional
import json

from multiformats import varint, multihash, CID
import dag_cbor
from numcodecs.compat import ensure_bytes

import cbor2

from .contentstore import ContentAddressableStore, MappingCAStore

class InlineCodec:
    def __init__(self, decoder, encoder):
        self.decoder = decoder
        self.encoder = encoder

inline_objects = {
    ".zarray": InlineCodec(json.loads, json.dumps),
    ".zgroup": InlineCodec(json.loads, json.dumps),
    ".zmetadata": InlineCodec(json.loads, json.dumps),
    ".zattrs": InlineCodec(json.loads, json.dumps),
}


class IPLDStore(MutableMapping):
    def __init__(self, castore: Optional[ContentAddressableStore] = None, sep="/"):
        self._mapping = {}
        self._store = castore or MappingCAStore()
        self.sep = sep
        self.root_cid = None
    
    def __getitem__(self, key):
        key_parts = key.split(self.sep)
        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            return self._store.get(self._mapping[key])
        else:
            return inline_codec.encoder(self._mapping[key])

    def __setitem__(self, key, value):
        value = ensure_bytes(value)
        key_parts = key.split(self.sep)

        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            cid = self._store.put(value)
            self.root_cid = None
            self._mapping[key] = cid
        else:
            self.root_cid = None
            self._mapping[key] = inline_codec.decoder(value)

    def __delitem__(self, key):
        del self._mapping[key]

    def __iter__(self):
        return iter(self._mapping)

    def __len__(self):
        return len(self._mapping)

    def freeze(self):
        """
            Store current version and return the corresponding root cid.
        """
        if self.root_cid is None:
            self.root_cid = self._store.put(self._mapping)
        return self.root_cid

    def to_car(self, stream=None):
        return self._store.to_car(self.freeze(), stream)

    def import_car(self, stream):
        roots = self._store.import_car(stream)
        if len(roots) != 1:
            raise ValueError(f"CAR must have a single root, the given CAR has {len(roots)} roots!")
        self.root_cid = roots[0]
        self._mapping = self._store.get(self.root_cid)

    @classmethod
    def from_car(cls, stream):
        instance = cls()
        instance.import_car(stream)
        return instance
