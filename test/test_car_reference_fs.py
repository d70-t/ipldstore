import os
import json
import tempfile

from ipldstore.car_reference_fs import car2reference_fs
from ipldstore import IPLDStore

import xarray as xr

import pytest

def test_car_reference_fs():
    basename = "test_car_reference_fs"
    m = IPLDStore()
    ds = xr.Dataset({"a": (("a",), [1, 2, 3]),
                     "b": (("a",), [5., 6., 8.])})
    ds.to_zarr(m)

    with tempfile.TemporaryDirectory() as folder:
        carfilename = os.path.join(folder, basename + ".car")
        indexfilename = os.path.join(folder, basename + ".json")
        with open(carfilename, "wb") as carfile:
            m.to_car(carfile)
        ref = car2reference_fs(carfilename)
        with open(indexfilename, "w") as reffile:
            json.dump(ref, reffile)

        ds2 = xr.open_zarr("reference::" + indexfilename)
        assert ds.identical(ds2)
