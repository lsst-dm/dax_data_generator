
import argparse
from lsst.dax.data_generator import DataGenerator

# start with
# original data generation:
#   python bin/datagen.py --chunk 3525 --visits 30 --objects 1000 example_spec.py
# edge first complete chunk:
#   python bin/datagen.py  --chunk 3525 --visits 30 --objects 1000 example_spec.py
# edge first only the edge:
#   python bin/datagen.py --edgeonly --chunk 3525 --visits 30 --objects 1000 example_spec.py
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", type=int, required=True)
    parser.add_argument("--visits", type=int, required=True)
    parser.add_argument("--edgeonly", action="store_true")
    parser.add_argument("specification", type=str)
    args = parser.parse_args()

    edge_only = args.edgeonly > 0

    with open(args.specification) as f:
        spec_globals = {}
        exec(f.read(), spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'edge_width' in spec_globals, "Specification file must define variable 'edge_width'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        spec = spec_globals['spec']
        edge_width = spec_globals['edge_width']
        chunker = spec_globals['chunker']

    seed = 1
    dataGen = DataGenerator(spec, chunker, seed=seed)
    chunk_id = args.chunk

    tables = dataGen.make_chunk(chunk_id, edge_width=edge_width, edge_only=edge_only)

    for table_name, table in tables.items():
        edge_type = "EO" if edge_only else "CT"
        table.to_csv("chunk{:d}_{:s}_{:s}.csv".format(chunk_id, edge_type, table_name),
                     header=False, index=False)

