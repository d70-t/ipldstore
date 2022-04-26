from multiformats import CID, multicodec, multihash, multibase
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


class FixedChunker:
    def __init__(self, chunksize: int = 262144):
        self.chunksize = chunksize

    def read_stream(self, stream):
        while chunk := stream.read(self.chunksize):
            yield chunk

class RawLeavesBuilder:
    _raw_codec = multicodec.get("raw")
    _hash = multihash.get("sha2-256")
    _cid_version = 1
    _base = multibase.get("base32")

    def build_leaf(self, chunk: bytes):
        h = self._hash.digest(chunk)
        cid = CID(self._base, self._cid_version, self._raw_codec, h)
        return cid, len(chunk), len(chunk), chunk

class BalancedLayouter:
    def __init__(self, max_width = 174):
        self.max_width = max_width

    def start_stream(self):
        return _BalancedLayouter(self.max_width)


class _BalancedLayouter:
    _pb_codec = multicodec.get("dag-pb")
    _hash = multihash.get("sha2-256")
    _cid_version = 1
    _base = multibase.get("base32")

    def __init__(self, max_width = 174):
        self.max_width = max_width
        self.children = [[]]

    def new_leaf(self, leaf):
        cid, blocksize, totalsize, data = leaf
        yield leaf
        yield from self.push_cid(cid, blocksize, totalsize)

    def push_cid(self, cid, blocksize, totalsize, level=0):
        if len(self.children) < level + 1:
            self.children.append([])
        self.children[level].append((cid, blocksize, totalsize))
        if len(self.children[level]) >= self.max_width:
            yield from self.flush_level(level)

    def flush_level(self, level):
        cid, blocksize, totalsize, data = self.build_node(self.children[level])
        yield cid, blocksize, totalsize, data
        self.children[level] = []
        yield from self.push_cid(cid, blocksize, totalsize, level+1)

    def build_node(self, children):
        cids, blocksizes, totalsizes = zip(*children)
        data = unixfsv1.Data(Type=unixfsv1.DataType.File,
                             filesize=sum(blocksizes),
                             blocksizes=blocksizes).dumps()
        pblinks = [unixfsv1.PBLink(Hash=bytes(cid), Name="", Tsize=totalsize)
                   for cid, blocksize, totalsize in children]
        node = unixfsv1.PBNode(Links=pblinks, Data=data).dumps()

        h = self._hash.digest(node)
        cid = CID(self._base, self._cid_version, self._pb_codec, h)
        return cid, sum(blocksizes), len(node) + sum(totalsizes), node

    def finish(self):
        for i in range(len(self.children)):
            if not (i == len(self.children) - 1 and len(self.children[i]) <= 1):
                yield from self.flush_level(i)


class UnixFSImporter:
    def __init__(self, chunker=None, leaf_builder=None, layouter=None, cid_version=1):
        self.chunker = chunker or FixedChunker()
        self.leaf_builder = leaf_builder or RawLeavesBuilder()
        self.layouter = layouter or BalancedLayouter()
        self.cid_version = cid_version

    def import_file(self, stream):
        stream_layouter = self.layouter.start_stream()
        for chunk in self.chunker.read_stream(stream):
            leaf = self.leaf_builder.build_leaf(chunk)
            yield from stream_layouter.new_leaf(leaf)
        yield from stream_layouter.finish()


def main():
    print(dict_to_unixfs_car({"a": b"test", "b/c": b"foo"}))

def main2():
    from io import BytesIO

    stream = BytesIO(b"hello world\n")
    importer = UnixFSImporter()
    for cid, bsize, tsize, data in importer.import_file(stream):
        print(cid, bsize, tsize)

if __name__ == "__main__":
    main2()
