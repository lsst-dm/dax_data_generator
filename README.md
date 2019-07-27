

Fake Catalog Generator
======================


Requires sphgeom, using the branch available at
https://github.com/lsst/sphgeom/tree/u/ctslater/getchunk. No other LSST packages are required.

Example usage:
```
python bin/datagen.py --chunk 3525 --visits 30 --objects 10000 example_spec.py
```


Internals
---------

The goal is to be able to generate individual chunks (spatial regions on the sky) independently.
This requires making some simplifications, since in a real survey visits will overlap different
chunks. This code creates a set of visit centers inside the chunk boundary, then for each visit
center and each object it creates a ForcedSource record if the object falls within a set radius of
the visit center. This means visits will appear to abruptly end at the edge of a chunk. Some
extra visits will be necessary to get the right number of ForceSource records.

An alternative would have been to generate the visit table in an initial phase, and then make chunks
in parallel using that.




