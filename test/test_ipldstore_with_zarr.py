from io import BytesIO

from ipldstore import IPLDStore
from ipldstore.contentstore import MappingCAStore

import zarr
import numpy as np
from multiformats import CID

import pytest

def test_create_array():
    castore = MappingCAStore()
    store = IPLDStore(castore)
    z = zarr.create(store=store, overwrite=True, shape=5, dtype='i1', compressor=None)
    z[:] = np.arange(5, dtype="i1")
    assert CID.decode("bafkreiaixnpf23vkyecj5xqispjq5ubcwgsntnnurw2bjby7khe4wnjihu") in castore  # b"\x00\x01\x02\x03\x04"

    with open("test.car", "wb") as tc:
        store.to_car(tc)

@pytest.mark.parametrize("use_stream", [True, False])
def test_move_array_between_stores_using_car(use_stream):
    store1 = IPLDStore()
    z = zarr.create(store=store1, overwrite=True, shape=100, dtype='float', compressor=None)
    a = np.random.random(100)
    z[:] = a

    if use_stream:
        transport = BytesIO()
        store1.to_car(transport)
        transport.seek(0)
    else:
        transport = store1.to_car()

    store2 = IPLDStore.from_car(transport)
    z2 = zarr.open(store=store2)

    assert np.all(z2[:] == a)
