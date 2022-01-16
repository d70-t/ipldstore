from ipldstore import IPLDStore

import pytest

def test_basic_mapping_properties():
    s = IPLDStore()
    s["a"] = b"b"
    assert s["a"] == b"b"
    assert len(s) == 1
    del s["a"]
    assert len(s) == 0
    with pytest.raises(KeyError):
        s["a"]
