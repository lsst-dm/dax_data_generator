#!/usr/bin/env python3

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
from lsst.dax.data_generator import columns
from lsst.dax.data_generator import DataGenerator
from lsst.dax.data_generator import testing


def edgeOnlyContainedInComplete(chunk_id, object_count, visit_count, edge_width, spec, chunker):
    """ Check if all the edge only rows can be matched with identical rows
    in the complete table.
    """

    seed = 1
    dataGen = DataGenerator(spec, chunker, seed=seed)

    row_counts = {"CcdVisit": visit_count, "Object": object_count}
    # ForcedSource count is defined by visits and objects.
    if("ForcedSource" in spec):
        row_counts["ForcedSource"] = None
    tablesComplete = dataGen.make_chunk(chunk_id,
                                        edge_width=edge_width, edge_only=False,
                                        return_pregenerated=True)

    row_counts = {"CcdVisit": visit_count, "Object": object_count}
    if("ForcedSource" in spec):
        row_counts["ForcedSource"] = None
    tablesEdgeOnly = dataGen.make_chunk(chunk_id,
                                        edge_width=edge_width, edge_only=True,
                                        return_pregenerated=True)

    print("visits len:", len(tablesComplete["CcdVisit"]))
    print("forced len:", len(tablesComplete["ForcedSource"]))
    print("object len:", len(tablesComplete["Object"]))
    print("visitsEO len:", len(tablesEdgeOnly["CcdVisit"]))
    print("forcedEO len:", len(tablesEdgeOnly["ForcedSource"]))
    print("objectEO len:", len(tablesEdgeOnly["Object"]))

    # For every edge only table in every chunk, check that all of its
    # rows have an identical match in the equivalent complete table.
    for tblNameEO, tblEO in tablesEdgeOnly.items():
        # find matching complete DataFrame
        tblComplete = tablesComplete[tblNameEO]

        colCount = len(tblEO.columns)
        rowCountEO = len(tblEO)
        rowCountComp = len(tblComplete)
        rowsChecked = 0
        for j in range(rowCountEO):
            found = False
            rowEO = tblEO.iloc[j]
            for x in range(rowCountComp):
                rowComp = tblComplete.iloc[x]
                match = True
                rowsChecked += 1
                for y in range(colCount):
                    if rowEO[y] != rowComp[y]:
                        match = False
                        break
                if match:
                    found = True
                    break
            if not found:
                #$$$ print("Failed for table=", tblNameEO, " to find rowEO=", rowEO, " in complete table")
                print(f"Failed for table={tblNameEO} to find rowEO={rowEO} in complete table")
                return False
        #$$$print("rowsChecked=", rowsChecked)
        print(f"rowsChecked={rowsChecked}")
    return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--skip", action="count", default=0)
    parser.add_argument("--chunk", type=int, default=3525)
    parser.add_argument("--objects", type=int, default=1000)
    parser.add_argument("--visits", type=int, default=30)
    parser.add_argument("--spec", type=str, default="example_spec.py")
    args = parser.parse_args()

    success = True
    if args.skip:
        print("skipping low level tests")
    else:
        if not testing.tst_convertBlockToRows():
            success = False
        if not testing.tst_mergeBlocks():
            success = False
        if not testing.tst_RaDecGenerator():
            success = False

    if not success:
        print("Failed low level tests.")

    with open(args.spec) as f:
        spec_globals = {}
        exec(f.read(), spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'edge_width' in spec_globals, "Specification file must define a variable 'edge_width'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        spec = spec_globals['spec']
        edge_width = spec_globals['edge_width']
        chunker = spec_globals['chunker']

    if not edgeOnlyContainedInComplete(args.chunk, args.objects, args.visits,
                                       edge_width, spec, chunker):
        success = False
        print("Failed row comparisons")

    print("Success=", success)
