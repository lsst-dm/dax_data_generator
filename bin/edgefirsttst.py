
import argparse
import pandas as pd
from lsst.dax.data_generator import columns
from lsst.dax.data_generator import DataGenerator


def edgeOnlyContainedInComplete(chunk_id, objectCount, visitCount, spec):
    """ Check if all the edge only rows can be matched with identical rows
    in the complete table table.
    """

    success = None
    dataGen = DataGenerator(spec)

    seed = 1
    edge_width = 0.017 # degrees

    row_counts = {"CcdVisit": visitCount, "Object": objectCount}
    # ForcedSource count is defined by visits and objects.
    if("ForcedSource" in spec):
        row_counts["ForcedSource"] = None
    tablesComplete = dataGen.make_chunk(chunk_id, num_rows=row_counts, seed=seed,
                                          edge_width=edge_width, edge_only=False)

    row_counts = {"CcdVisit": visitCount, "Object": objectCount}
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
                print("Failed for table=", tblNameEO, " to find rowEO=", rowEO, " in complete table")
                success = False
                return success
        print("rowsChecked=", rowsChecked)

    if success is None:
        success = True
    return success




if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--skip", action="count", default=0)
    parser.add_argument("--chunk", type=int, default=3525)
    parser.add_argument("--objects", type=int, default=1000)
    parser.add_argument("--visits", type=int, default=30)
    parser.add_argument("--spec", type=str, default="example_spec.py")
    args = parser.parse_args()

    success = None
    if args.skip:
        print("skipping low level tests")
    else:
        if not columns.tst_convertBlockToRows():
            success = False
        if not columns.tst_mergeBlocks():
            success = False
        if not columns.tst_RaDecGenerator():
            success = False


    with open(args.spec) as f:
        spec_globals = {}
        exec(f.read(), spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        spec = spec_globals['spec']

    if not edgeOnlyContainedInComplete(args.chunk, args.objects, args.visits, spec):
        success = False

    if success is None:
        success = True
    print("Success=", success)