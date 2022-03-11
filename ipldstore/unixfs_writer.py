from multiformats import CID, multicodec
from typing import Tuple

from .contentstore import MappingCAStore
from .ipldstore import set_recursive
from . import unixfsv1

DagPbCodec = multicodec.get("dag-pb")

def dict_to_unixfs_car(d):
    store = MappingCAStore()
    leaf_cids = {k: (len(v), store.put(v)) for k, v in d.items()}

    tree = {}
    for k, v in leaf_cids.items():
        set_recursive(tree, k.split("/"), v)

    def store_tree(tree) -> Tuple[int, CID]:
        links = {k: v if isinstance(v, tuple) else store_tree(v)
                 for k, v in tree.items()}
        data = unixfsv1.Data(unixfsv1.DataType.Directory).dumps()
        pblinks = [unixfsv1.PBLink(Hash=bytes(cid), Name=name, Tsize=size)
                   for name, (size, cid) in sorted(links.items())]
        node = unixfsv1.PBNode(Links=pblinks, Data=data)
        node_bytes = node.dumps()
        return len(node_bytes) + sum(s for s, _ in links.values()), store.put_raw(node_bytes, DagPbCodec)

    size, root = store_tree(tree)
    return root, store.to_car(root)

def main():
    print(dict_to_unixfs_car({"a": b"test", "b/c": b"foo"}))

if __name__ == "__main__":
    main()
