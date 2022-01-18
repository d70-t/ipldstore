# experimental IPLD based (writable) storage for zarr

This repository should be considered as experimental.

## examples

Store data on (local) IPFS node:
```python
>>> import ipldstore
>>> import xarray as xr
>>> ds = xr.Dataset({"a": ("a", [1, 2, 3])})
>>> m = ipldstore.get_ipfs_mapper()
>>> ds.to_zarr(m, encoding={"a": {"compressor": None}})   # doctest: +SKIP
<xarray.backends.zarr.ZarrStore object at 0x...>
>>> print(m.freeze())   # doctest: +SKIP
bafyreibf3yn5ovxhrcwwdg4sm5sinnkwoyfx3wswghbjchz4zvwt55bhte

```
(this example does only work if there's a local IPFS node running)

Instead of storing the data directly on IPFS, it is also possible to store the data
on a generic `MutableMapping`, which could be just a dictionary, but also some object store
or a file system. `MappingCAStore` does the necessary API conversions, so we wrap our
`backend` inside.

Let's try to store data in memory:

```python
>>> backend = {}  # can be any MutableMapping[str, bytes]
>>> m = ipldstore.IPLDStore(ipldstore.MappingCAStore(backend))
>>> ds.to_zarr(m, encoding={"a": {"compressor": None}})
<xarray.backends.zarr.ZarrStore object at 0x...>
>>> print(m.freeze())
bafyreibf3yn5ovxhrcwwdg4sm5sinnkwoyfx3wswghbjchz4zvwt55bhte

```

Now that we've got full control over our `backend`, we can also have a look at what's stored inside:

````python
>>> print(backend)
{'bafkreihc4ibtvz7btvualgou5mfbgwncwshmlovmoudgyml7x6crlhcu54': b'\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00', 'bafyreibf3yn5ovxhrcwwdg4sm5sinnkwoyfx3wswghbjchz4zvwt55bhte': b'\xa4aa\xa3a0\xd8*X%\x00\x01U\x12 \xe2\xe2\x03:\xe7\xe1\x9dh\x05\x99\xd4\xeb\n\x13Y\xa2\xb4\x8e\xc5\xba\xacu\x06l1\x7f\xbf\x85\x15\x9cT\xefg.zarray\xa8edtypec<i8eorderaCeshape\x81\x03fchunks\x81\x03gfilters\xf6jcompressor\xf6jfill_value\xf6kzarr_format\x02g.zattrs\xa1q_ARRAY_DIMENSIONS\x81aag.zattrs\xa0g.zgroup\xa1kzarr_format\x02j.zmetadata\xa2hmetadata\xa4g.zattrs\xa0g.zgroup\xa1kzarr_format\x02ia/.zarray\xa8edtypec<i8eorderaCeshape\x81\x03fchunks\x81\x03gfilters\xf6jcompressor\xf6jfill_value\xf6kzarr_format\x02ia/.zattrs\xa1q_ARRAY_DIMENSIONS\x81aax\x18zarr_consolidated_format\x01'}

```

The store contains two objects which are keyed by their content identifier (CID).
One are the raw bytes of our array data, the other is a combination of zarr metadata fields in DAG-CBOR encoding.
In order to understand these parts better, we'll decode the objects a bit further:

```python
>>> from multiformats import CID
>>> import dag_cbor
>>> from pprint import pprint

>>> def decode_block(k, v):
...     cid = CID.decode(k)
...     if cid.codec.name == "dag-cbor":
...         return cid, dag_cbor.decode(v)
...     else:
...         return cid, v

>>> for k, v in backend.items():
...     k, v = decode_block(k, v)
...     print(repr(k))
...     pprint(v, width=100)
...     print("---")
CID('base32', 1, 'raw', '1220e2e2033ae7e19d680599d4eb0a1359a2b48ec5baac75066c317fbf85159c54ef')
b'\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00'
---
CID('base32', 1, 'dag-cbor', '122025de1bd756e788ad619b92676486b556760b7dda5631c2911f3ccd6d3ef42799')
{'.zattrs': {},
 '.zgroup': {'zarr_format': 2},
 '.zmetadata': {'metadata': {'.zattrs': {},
                             '.zgroup': {'zarr_format': 2},
                             'a/.zarray': {'chunks': [3],
                                           'compressor': None,
                                           'dtype': '<i8',
                                           'fill_value': None,
                                           'filters': None,
                                           'order': 'C',
                                           'shape': [3],
                                           'zarr_format': 2},
                             'a/.zattrs': {'_ARRAY_DIMENSIONS': ['a']}},
                'zarr_consolidated_format': 1},
 'a': {'.zarray': {'chunks': [3],
                   'compressor': None,
                   'dtype': '<i8',
                   'fill_value': None,
                   'filters': None,
                   'order': 'C',
                   'shape': [3],
                   'zarr_format': 2},
       '.zattrs': {'_ARRAY_DIMENSIONS': ['a']},
       '0': CID('base58btc', 1, 'raw', '1220e2e2033ae7e19d680599d4eb0a1359a2b48ec5baac75066c317fbf85159c54ef')}}
---

```

So the first item are the raw bytes (we've disabled compression, so that we can observe the bytes `\x01`, `\x02` and `\x03` corresponding to the array data `[1, 2, 3]`.
The other object is a nested dag-cbor object which can be represented as a nested dictionary. It contains all the zarr metadata files inlined as well as a link to the CID of the array data. (The `base` of the CID differs, but as the `base` is purely representational and does not identify the content, it matches the CID from above).

It is also possible to transfer content via content archives (CAR):

```
>>> archive = m.to_car()
>>> type(archive), len(archive)
(<class 'bytes'>, 560)

```

The resulting archive is a valid [CARv1](https://ipld.io/specs/transport/car/carv1/) and can be imported to other IPLD speaking services (including IPFS).
It can also be imported into another `IPLDStore`:

```
>>> new_backend = {}  # can be any MutableMapping[str, bytes]
>>> new_m = ipldstore.IPLDStore(ipldstore.MappingCAStore(new_backend))
>>> new_backend
{}
>>> new_m.import_car(archive)
>>> print(new_m.freeze())
bafyreibf3yn5ovxhrcwwdg4sm5sinnkwoyfx3wswghbjchz4zvwt55bhte
>>> new_ds = xr.open_zarr(m)
>>> new_ds.a.values
array([1, 2, 3])

```

We've correctly transferred values from one dataset to a different backend store.
