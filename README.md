# experimental IPLD based (writable) storage for zarr

This repository should be considered as experimental.

## examples

### storing on IPFS

Store data on (local) IPFS node:
```python
>>> import ipldstore
>>> import xarray as xr
>>> ds = xr.Dataset({"a": ("a", [1, 2, 3])})
>>> m = ipldstore.get_ipfs_mapper()
>>> ds.to_zarr(m, encoding={"a": {"compressor": None}}, consolidated=False)   # doctest: +SKIP
<xarray.backends.zarr.ZarrStore object at 0x...>
>>> print(m.freeze())   # doctest: +SKIP
bafyreidn66mk3fktszrfwayonvpq6y3agtnb5e5o22ivof5tgikbxt7k6u

```
(this example does only work if there's a local IPFS node running)

#### Retrieving from IPFS

Since we generate a correctly formatted Zarr, we can read a Zarr on IPLD back into Xarray.
This is as simple as telling Xarray to open the IPLD mapper as the file URI. To correctly format this mapper,
we must decode the hash returned after `to_zarr` into the `base32` string representation (see below)),
then set our mapper's root to that.

Note we use a different hash below to show a more typical N-Dimensional dataset.

```python
>>> import xarray as xr
>>> from multiformats import CID
>>> ipld_mapper = ipldstore.get_ipfs_mapper()
>>> cid_obj = CID.decode("bafyreidjxhcilm5r227in4tvrjujawad4n7pydxk543ez53ttx6jieilc4")
>>> cid_obj
CID('base32', 1, 'dag-cbor', '12206df798ad955396625b030e6d5f0f636034da1e93aed6915717b332141bcfeaf5')
>>> ipld_mapper.set_root(cid_obj)
>>> z = xr.open_zarr(ipld_mapper, consolidated=False)
>>> z
<xarray.Dataset>
Dimensions:     (latitude: 721, longitude: 1440, valid_time: 8760)
Coordinates:
  * latitude    (latitude) float64 90.0 89.75 89.5 89.25 ... -89.5 -89.75 -90.0
  * longitude   (longitude) float64 0.0 0.25 0.5 0.75 ... 359.2 359.5 359.8
  * time        (time) datetime64[ns] 1990-01-01 .... 1992-02-06
Data variables:
    tmax        (time, longitude, latitude) float32 dask.array<chunksize=(2190, 1440, 4), meta=np.ndarray>

```

We're now able to query, analyze, and otherwise manipulate the Zarr just like a normal Xarray Dataset.

### storing on a MutableMapping

Instead of storing the data directly on IPFS, it is also possible to store the data
on a generic `MutableMapping`, which could be just a dictionary, but also some object store
or a file system. `MappingCAStore` does the necessary API conversions, so we wrap our
`backend` inside.

Let's try to store data in memory:

```python
>>> backend = {}  # can be any MutableMapping[str, bytes]
>>> m = ipldstore.IPLDStore(ipldstore.MappingCAStore(backend))
>>> ds.to_zarr(m, encoding={"a": {"compressor": None}}, consolidated=False)
<xarray.backends.zarr.ZarrStore object at 0x...>
>>> print(m.freeze())
bafyreidn66mk3fktszrfwayonvpq6y3agtnb5e5o22ivof5tgikbxt7k6u

```

#### A look inside

Now that we've got full control over our `backend`, we can also have a look at what's stored inside:

```python
>>> from pprint import pprint
>>> pprint(backend, width=120)
{'bafkreihc4ibtvz7btvualgou5mfbgwncwshmlovmoudgyml7x6crlhcu54': b'\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00'
                                                                b'\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00',
 'bafyreidn66mk3fktszrfwayonvpq6y3agtnb5e5o22ivof5tgikbxt7k6u': b'\xa3aa\xa3a0\xd8*X%\x00\x01U\x12 \xe2\xe2\x03:\xe7'
                                                                b'\xe1\x9dh\x05\x99\xd4\xeb\n\x13Y\xa2\xb4'
                                                                b'\x8e\xc5\xba\xacu\x06l1\x7f\xbf\x85\x15\x9cT\xefg.zar'
                                                                b'ray\xa8edtypec<i8eorderaCeshape\x81\x03fchunk'
                                                                b's\x81\x03gfilters\xf6jcompressor\xf6jfill_value\xf6'
                                                                b'kzarr_format\x02g.zattrs\xa1q_ARRAY_DIMENSIONS\x81aag'
                                                                b'.zattrs\xa0g.zgroup\xa1kzarr_format\x02'}

```

The store contains two objects which are keyed by their content identifier (CID).
The first one are the raw bytes of our array data, the second is a combination of zarr metadata fields in DAG-CBOR encoding.
Note that this is unconsolidated metadata, but the store is able to inline the metadata objects, which makes them
traversable using common IPLD mechanisms.

In order to understand the parts better, we'll decode the objects a bit further:

First, let's just have a look at the raw array data:
```python
>>> rawdata = backend['bafkreihc4ibtvz7btvualgou5mfbgwncwshmlovmoudgyml7x6crlhcu54']
>>> rawdata
b'\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00'
>>> import numpy as np
>>> np.frombuffer(rawdata, "int")
array([1, 2, 3])

```

Indeed, that's our original array-data.

For the second object, we'll use `cbor2` to decode the object and cover the `Link`-type (CBOR tag 42) manually afterwards.
Note that this object has the same content identifier as returned by `m.freeze()`, so we'll call it the `root_object`.

```python
>>> import cbor2
>>> root_object = cbor2.loads(backend['bafyreidn66mk3fktszrfwayonvpq6y3agtnb5e5o22ivof5tgikbxt7k6u'])
>>> pprint(root_object, width=100)
{'.zattrs': {},
 '.zgroup': {'zarr_format': 2},
 'a': {'.zarray': {'chunks': [3],
                   'compressor': None,
                   'dtype': '<i8',
                   'fill_value': None,
                   'filters': None,
                   'order': 'C',
                   'shape': [3],
                   'zarr_format': 2},
       '.zattrs': {'_ARRAY_DIMENSIONS': ['a']},
       '0': CBORTag(42, b'\x00\x01U\x12 \xe2\xe2\x03:\xe7\xe1\x9dh\x05\x99\xd4\xeb\n\x13Y\xa2\xb4\x8e\xc5\xba\xacu\x06l1\x7f\xbf\x85\x15\x9cT\xef')}}

```

This structure represents the entire hierarchy of objects generated by zarr. In particular, it contains:

* `.zattrs`
* `.zgroup`
* `a/.zarray`
* `a/.zattrs`
* `a/0`

where all but `a/0` have been inlined into the structure to make the contained metadata visible for IPLD.
So far, this is just plain CBOR. In order to understand the link, we need to decode it using [CID rules](https://ipld.io/specs/codecs/dag-cbor/spec/#links).
In particular, we want to transform the link form its raw binary encoding within CBOR into the `base32`
string representation which is used by our `backend`. To do so, we'll use the `multiformats` package, which
knows (among others) about baseN encodings, varints and CID encoding rules. As DAG-CBOR mandates a 0-byte as
prefix before any CID, we'll have to remove that when passing it to the CID module.

```python
>>> from multiformats import CID
>>> print(CID.decode(root_object["a"]["0"].value[1:]).set(base="base32"))
bafkreihc4ibtvz7btvualgou5mfbgwncwshmlovmoudgyml7x6crlhcu54

```

This is indeed the CID which links to our data block.


#### Transferring multiple blocks

It is also possible to transfer content via content archives (CAR):

```python
>>> archive = m.to_car()
>>> type(archive), len(archive)
(<class 'bytes'>, 356)

```

The resulting archive is a valid [CARv1](https://ipld.io/specs/transport/car/carv1/) and can be imported to other IPLD speaking services (including IPFS).
It can also be imported into another `IPLDStore`:

```python
>>> new_backend = {}  # can be any MutableMapping[str, bytes]
>>> new_m = ipldstore.IPLDStore(ipldstore.MappingCAStore(new_backend))
>>> new_backend
{}
>>> new_m.import_car(archive)
>>> print(new_m.freeze())
bafyreidn66mk3fktszrfwayonvpq6y3agtnb5e5o22ivof5tgikbxt7k6u
>>> new_ds = xr.open_zarr(m, consolidated=False)
>>> new_ds.a.values
array([1, 2, 3])

```

We've correctly transferred values from one dataset to a different backend store.
