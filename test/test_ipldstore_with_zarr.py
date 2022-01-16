from io import BytesIO

from ipldstore import IPLDStore
from ipldstore.contentstore import MappingCAStore

import zarr
import numpy as np
from multiformats import CID

def test_create_array():
    castore = MappingCAStore()
    store = IPLDStore(castore)
    z = zarr.create(store=store, overwrite=True, shape=5, dtype='i1', compressor=None)
    z[:] = np.arange(5, dtype="i1")
    assert CID.decode("bafkreiaixnpf23vkyecj5xqispjq5ubcwgsntnnurw2bjby7khe4wnjihu") in castore  # b"\x00\x01\x02\x03\x04"

    with open("test.car", "wb") as tc:
        store.to_car(tc)

def test_move_array_between_stores_using_car():
    store1 = IPLDStore()
    z = zarr.create(store=store1, overwrite=True, shape=100, dtype='float', compressor=None)
    a = np.random.random(100)
    z[:] = a

    stream = BytesIO()
    store1.to_car(stream)

    stream.seek(0)

    store2 = IPLDStore.from_car(stream)
    z2 = zarr.open(store=store2)

    assert np.all(z[:] == a)
