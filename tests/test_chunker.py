
import unittest

from lsst.dax.data_generator import Chunker


class ChunkerTests(unittest.TestCase):

    def testLocate(self):
        chunker = Chunker(0, 0, 0)
        result_chunk = chunker.locate((0.25, 0.25))
        self.assertEqual(result_chunk, 1)

        result_chunk = chunker.locate((3.25, 0.25))
        self.assertEqual(result_chunk, 0)

        result_chunk = chunker.locate((0.25, 3.25))
        self.assertEqual(result_chunk, 0)
