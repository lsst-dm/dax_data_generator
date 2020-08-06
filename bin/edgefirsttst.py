
import argparse
from lsst.dax.data_generator import columns
from lsst.dax.data_generator import DataGenerator


def edgeOnlyContainedInComplete(chunk_id, object_count, visit_count, edge_width, spec):
    """ Check if all the edge only rows can be matched with identical rows
    in the complete table.
    """

    dataGen = DataGenerator(spec)
    seed = 1

    row_counts = {"CcdVisit": visit_count, "Object": object_count}
    # ForcedSource count is defined by visits and objects.
    if("ForcedSource" in spec):
        row_counts["ForcedSource"] = None
    tablesComplete = dataGen.make_chunk(chunk_id, num_rows=row_counts, seed=seed,
                                        edge_width=edge_width, edge_only=False)

    row_counts = {"CcdVisit": visit_count, "Object": object_count}
    if("ForcedSource" in spec):
        row_counts["ForcedSource"] = None
    tablesEdgeOnly = dataGen.make_chunk(chunk_id, num_rows=row_counts, seed=seed,
                                        edge_width=edge_width, edge_only=True)

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
        if not columns.tst_convertBlockToRows():
            success = False
        if not columns.tst_mergeBlocks():
            success = False
        if not columns.tst_RaDecGenerator():
            success = False

    if not success:
        print("Failed low level tests.")

    with open(args.spec) as f:
        spec_globals = {}
        exec(f.read(), spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'edge_width' in spec_globals, "Specification file must define a variable 'edge_width'."
        spec = spec_globals['spec']
        edge_width = spec_globals['edge_width']

    if not edgeOnlyContainedInComplete(args.chunk, args.objects, args.visits, edge_width, spec):
        success = False
        print("Failed row comparisons")

    print("Success=", success)