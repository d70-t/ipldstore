from io import BytesIO

import ipldstore.car as car

import pytest

def test_car_reject_v2():
    v2_start = bytes.fromhex("0aa16776657273696f6e02")
    stream = BytesIO(v2_start)
    with pytest.raises(ValueError):
        car.decode_car_header(stream)
