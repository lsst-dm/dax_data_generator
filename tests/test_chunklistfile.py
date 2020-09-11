
import unittest

import lsst.dax.distribution.chunklistfile as chunklistfile


class TData:
    def __init__(self):
        self.good_raw = "3:15\n30\n77\n108\n55\n0\n999"
        # valid_ids are chunks that exist in the partitioning scheme
        self.valid_ids = [0,1,2,3,4,10,15,30,55,999,543,1000]
        self.merged_set = set([0,3,4,10,15,30,55,999])
        self.good_set = set([3,4,5,6,7,8,9,10,11,12,13,14,15,30,77,108,55,0,999])
        self.completed = [4,5,6,7,8,9]
        self.limbo = []
        self.assigned = [15,30]
        # result_expected = (good_set intersection valid_ids)
        #                 - (completed + limbo + assigned)
        self.result_expected = set([3,10,55,0,999])
        # Produced set for ok_raw should match good_set despite
        # extra elements
        self.ok_raw = "3:15\n30\n77\n108\n5\n6\n55\n0\n999"

        # for testing ChunkLogs, tests use self.valid_ids
        self.lists_raw = "9:100"
        # intersection of self.valid_ids and self.lists_raw
        self.lists_expected = [10,15,30,55]

        # These should fail
        self.bad_raw_a = "3s:15\n30\n77"
        self.bad_raw_b = "3:15\n3w0\n77"

class ChunkListFileTests(unittest.TestCase):

    def testParse(self):
        dummyf = "/tmp/dummy"
        tdata = TData()
        clf = chunklistfile.ChunkListFile(dummyf)

        clf.parse(tdata.good_raw)
        self.assertSetEqual(clf.chunk_set, tdata.good_set)

        clf.intersectWithValid(tdata.valid_ids)
        self.assertSetEqual(clf.chunk_set, tdata.merged_set)

        clf_ok = chunklistfile.ChunkListFile(dummyf)
        clf_ok.parse(tdata.ok_raw)
        self.assertSetEqual(clf_ok.chunk_set, tdata.good_set)

        clf_bad_a = chunklistfile.ChunkListFile(dummyf)
        threw = False
        try:
            clf_bad_a.parse(tdata.bad_raw_a)
        except ValueError:
            threw = True
        self.assertTrue(threw)

        clf_bad_b = chunklistfile.ChunkListFile(dummyf)
        threw = False
        try:
            clf_bad_b.parse(tdata.bad_raw_b)
        except ValueError:
            threw = True
        self.assertTrue(threw)

        clf_empty = chunklistfile.ChunkListFile(dummyf)
        clf_empty.parse('')
        self.assertTrue(not clf_empty.chunk_set)

    def testAdd(self):
        dummyf = "/tmp/dummy"
        tdata = TData()
        clf = chunklistfile.ChunkListFile(dummyf)

        clf.parse(tdata.good_raw)
        self.assertSetEqual(clf.chunk_set, tdata.good_set)

        to_add = set([341, 342, 343])
        clf.add(to_add)
        tdata.good_set.update(to_add)
        self.assertSetEqual(clf.chunk_set, tdata.good_set)

    def testWriteRead(self):
        dummyf = "/tmp/tmpchunktest"
        tdata = TData()

        clf = chunklistfile.ChunkListFile(dummyf)
        clf.parse(tdata.good_raw)
        clf.write()
        to_add = set([341, 342, 343])
        clf.add(to_add)

        clf_r = chunklistfile.ChunkListFile(dummyf)
        clf_r.read()
        # test that what was read matches what was written,
        # including 'add'.
        self.assertSetEqual(clf.chunk_set, clf_r.chunk_set)

    def testChunkFileListsRW(self):
        # Test reading and writing to disk
        dummyf = "/tmp/tmpchunktest"
        tdata = TData()

        clf_target = chunklistfile.ChunkListFile(dummyf)
        clf_target.parse(tdata.good_raw)
        clf_target.write()

        # base ChunkFilesList off of clf_target and write
        clogs = chunklistfile.ChunkLogs(dummyf)
        clogs.build(tdata.valid_ids)
        # create outputs
        clogs_out = clogs.createOutput("/tmp/")
        clogs_out.write()

        clogs_out.addCompleted(tdata.completed)
        clogs_out.addLimbo(tdata.limbo)
        clogs_out.addAssigned(tdata.assigned)

        tf, cf, af, lf = chunklistfile.ChunkLogs.createNames("/tmp")
        clogs_read = chunklistfile.ChunkLogs(tf, cf, af, lf)

        clogs_read.build(tdata.valid_ids)
        self.assertSetEqual(clogs_out._target.chunk_set, clogs_read._target.chunk_set)
        self.assertSetEqual(clogs_out._completed.chunk_set, clogs_read._completed.chunk_set)
        self.assertSetEqual(clogs_out._assigned.chunk_set, clogs_read._assigned.chunk_set)
        self.assertSetEqual(clogs_out._limbo.chunk_set, clogs_read._limbo.chunk_set)

        self.assertSetEqual(clogs_read.result_set, set(tdata.result_expected))

    def testChunkFileLists(self):
        tdata = TData()
        clogs = chunklistfile.ChunkLogs(None, raw = tdata.lists_raw)
        clogs.build(tdata.valid_ids)
        self.assertSetEqual(clogs.result_set, set(tdata.lists_expected))
        print(clogs.report())











