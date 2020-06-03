
import argparse
from lsst.dax.data_generator import columns


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    #parser.add_argument("--chunk", type=int, required=True)
    #parser.add_argument("--objects", type=int, required=True)
    #parser.add_argument("--visits", type=int, required=True)
    #parser.add_argument("--edgefirst", action="count", default=0)
    #parser.add_argument("specification", type=str)
    args = parser.parse_args()

    success = None
    if not columns.tst_convertBlockToRows():
        success = False
    if not columns.tst_mergeBlocks():
        success = False
    #if not tst_CcdVisitGeneratorEF():
    #    success = False
    if not columns.tst_RaDecGeneratorEF():
        success = False
    if success is None:
        success = True
    print("Success=", success)
