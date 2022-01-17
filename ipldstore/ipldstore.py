from collections.abc import MutableMapping
from typing import Optional
import json

from multiformats import varint, multihash, CID
import dag_cbor
from numcodecs.compat import ensure_bytes

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
        get_value = get_recursive(self._mapping, key_parts)
        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            return self._store.get(get_value)
        else:
            return inline_codec.encoder(get_value)

    def __setitem__(self, key, value):
        value = ensure_bytes(value)
        key_parts = key.split(self.sep)

        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            cid = self._store.put(value)
            set_value = cid
        else:
            set_value = inline_codec.decoder(value)

        self.root_cid = None
        set_recursive(self._mapping, key_parts, set_value)

    def __delitem__(self, key):
        key_parts = key.split(self.sep)
        del_recursive(self._mapping, key_parts)

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

    def clear(self):
        self.root_cid = None
        self._mapping = {}

    def to_car(self, stream=None):
        return self._store.to_car(self.freeze(), stream)

    def import_car(self, stream):
        roots = self._store.import_car(stream)
        if len(roots) != 1:
            raise ValueError(f"CAR must have a single root, the given CAR has {len(roots)} roots!")
        self.set_root(roots[0])

    @classmethod
    def from_car(cls, stream):
        instance = cls()
        instance.import_car(stream)
        return instance

    def set_root(self, cid):
        if isinstance(cid, str):
            cid = CID.decode(cid)
        assert cid in self._store
        self.root_cid = cid
        self._mapping = self._store.get(cid)


def set_recursive(obj, path, value):
    assert len(path) >= 1
    if len(path) == 1:
        obj[path[0]] = value
    else:
        set_recursive(obj.setdefault(path[0], {}), path[1:], value)

def get_recursive(obj, path):
    assert len(path) >= 1
    if len(path) == 1:
        return obj[path[0]]
    else:
        return get_recursive(obj[path[0]], path[1:])

def del_recursive(obj, path):
    assert len(path) >= 1
    if len(path) == 1:
        del obj[path[0]]
    else:
        del_recursive(obj[path[0]], path[1:])
        if len(obj[path[0]]) == 0:
            del obj[path[0]]
