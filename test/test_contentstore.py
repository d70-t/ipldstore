from ipldstore.contentstore import MappingCAStore
from multiformats import CID

import pytest

test_cid = CID("base58btc", 1, "raw",
"12206e6ff7950a36187a801613426e858dce686cd7d7e3c0fc42ee0330072d245c95")

test_values = [b"hallo",
               "hallo",
               {"a": 1},
               [1, 2, 3],
               1,
               1.34,
               True,
               False,
               None,
               test_cid,
               [test_cid],
               {"foo": test_cid}]

@pytest.mark.parametrize("value", test_values)
def test_store_and_retrieve(value):
    s = MappingCAStore()
    cid = s.put(value)
    assert s.get(cid) == value


test_values = [["hallo", {"a": 5}],
               [1, True, [2,1,4], b"foo"],
              ]

@pytest.mark.parametrize("values", test_values)
def test_car_roundtrip(values):
    s = MappingCAStore()
    keyed_values = [(s.put(value), value) for value in values]
    all_cids = [cid for cid, _ in keyed_values]
    root = s.put(all_cids)
    car = s.to_car(root)

    s2 = MappingCAStore()
    s2.import_car(car)
    assert s2.get(root) == all_cids
    for cid, value in keyed_values:
        assert s2.get(cid) == value
