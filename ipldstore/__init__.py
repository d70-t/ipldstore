from collections.abc import MutableMapping
import json

from multiformats import varint, multihash, CID
import dag_cbor
from numcodecs.compat import ensure_bytes

import cbor2

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
    def __init__(self, default_hash="sha2-256", default_base="base32", sep="/"):
        self._mapping = {}
        self._objects = {}
        self.default_hash = default_hash
        self.default_base = default_base
        self.sep = sep
        self.root_cid = None
    
    def __getitem__(self, key):
        key_parts = key.split(self.sep)
        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            return self._objects[self._mapping[key]]
        else:
            return inline_codec.encoder(self._mapping[key])

    def __setitem__(self, key, value):
        value = ensure_bytes(value)
        key_parts = key.split(self.sep)
        mh = multihash.digest(value, self.default_hash)
        
        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            cid = CID(self.default_base, 1, "raw", mh)
            self._objects[cid] = value 
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
        if self.root_cid is None:
            root_data = dag_cbor.encode(self._mapping)
            mh = multihash.digest(root_data, self.default_hash)
            root_cid = CID(self.default_base, 1, "dag-cbor", mh)
            self._objects[root_cid] = root_data
            self.root_cid = root_cid
            return root_cid
        else:
            return self.root_cid


    def iter_objects(self):
        self.freeze()
        return self._objects.items()

    def iter_car(self):
        header = dag_cbor.encode({"version": 1, "roots": [self.freeze()]})
        print(cbor2.loads(header))
        yield varint.encode(len(header))
        yield header
        for cid, data in self._objects.items():
            cid_bytes = bytes(cid)
            yield varint.encode(len(cid_bytes) + len(data))
            yield cid_bytes
            yield data
    
    def to_car(self):
        return b''.join(self.iter_car())
