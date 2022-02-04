import json
from typing import Dict, Any, Iterator, Tuple

import dag_cbor
from multiformats import CID, multicodec

from .car import read_car, CARBlockLocation
from .ipldstore import inline_objects
from .utils import StreamLike

def collect_tree_objects(stream_or_bytes: StreamLike) -> Tuple[CID, Dict[CID, Any], Dict[CID, CARBlockLocation]]:
    DagCborCodec = multicodec.get("dag-cbor")

    roots, blocks = read_car(stream_or_bytes)
    if len(roots) != 1:
        raise ValueError("need single-rooted car")
    root = roots[0]

    object_locations = {}
    cbor_objects = {}
    for cid, data, location in blocks:
        object_locations[cid] = location
        if cid.codec == DagCborCodec:
            cbor_objects[cid] = data

    return root, cbor_objects, object_locations


def car2reference_fs_refs(stream_or_bytes: StreamLike, stream_name: str) -> Dict[str, Any]:
    root, cbor_objects, object_locations = collect_tree_objects(stream_or_bytes)

    tree = dag_cbor.decode(cbor_objects[root])
    assert isinstance(tree, dict)
    sep = "/"

    def iter_nested(prefix: str, mapping: Dict[str, Any]) -> Iterator[Tuple[str, Any]]:
        for key, value in mapping.items():
            key_parts = key.split(sep)
            if key_parts[-1] in inline_objects:
                yield prefix + key, value
            elif isinstance(value, dict):
                yield from iter_nested(prefix + key + sep, value)
            else:
                yield prefix + key, value

    refs: Dict[str, Any] = {}
    for key, content in iter_nested("", tree):
        if isinstance(content, CID):
            loc = object_locations[content]
            refs[key] = [stream_name, loc.payload_offset, loc.payload_size]
        else:
            refs[key] = json.dumps(content)

    return refs


def car2reference_fs(filename: str) -> Dict[str, Any]:
    with open(filename, "rb") as stream:
        refs = car2reference_fs_refs(stream, "{{a}}")
    return {"version": 1, "templates": {"a": filename}, "refs": refs}
