from ipldstore import IPLDStore

import zarr
import numpy as np
from multiformats import CID

def test_create_array():
    store = IPLDStore()
    z = zarr.create(store=store, overwrite=True, shape=5, dtype='i1', compressor=None)
    z[:] = np.arange(5, dtype="i1")
    assert CID.decode("bafkreiaixnpf23vkyecj5xqispjq5ubcwgsntnnurw2bjby7khe4wnjihu") in store._objects  # b"\x00\x01\x02\x03\x04"

    for cid, data in store.iter_objects():
        print(cid, data)

    with open("test.car", "wb") as tc:
        tc.write(store.to_car())

    #assert False
