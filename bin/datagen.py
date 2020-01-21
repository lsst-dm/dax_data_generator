
import argparse
from lsst.dax.data_generator import DataGenerator

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", type=int, required=True)
    parser.add_argument("--objects", type=int, required=True)
    parser.add_argument("--visits", type=int, required=True)
    parser.add_argument("specification", type=str)
    args = parser.parse_args()

    with open(args.specification) as f:
        spec_globals = {}
        exec(f.read(), spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        spec = spec_globals['spec']

    dataGen = DataGenerator(spec)
    chunk_id = args.chunk
    row_counts = {"CcdVisit": args.visits,
                  "Object": args.objects}

    # ForcedSource count is defined by visits and objects.
    if("ForcedSource" in spec):
        row_counts["ForcedSource"] = None

    tables = dataGen.make_chunk(chunk_id, num_rows=row_counts)

    for table_name, table in tables.items():
        table.to_parquet("chunk{:d}_{:s}.parquet".format(chunk_id, table_name))

