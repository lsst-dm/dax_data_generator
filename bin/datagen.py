#!/usr/bin/env python

# This file is part of dax_data_generator.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
from lsst.dax.data_generator import DataGenerator

# start with
# original data generation:
#   python bin/datagen.py --chunk 3525 example_spec.py
# edge first complete chunk:
#   python bin/datagen.py  --chunk 3525 example_spec.py
# edge first only the edge:
#   python bin/datagen.py --edgeonly --chunk 3525 example_spec.py
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", type=int, required=True)
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

