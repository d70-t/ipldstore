from .ipldstore import IPLDStore
from .contentstore import ContentAddressableStore, MappingCAStore, IPFSStore

def get_ipfs_mapper(host="http://127.0.0.1:5001"):
    return IPLDStore(IPFSStore(host))
