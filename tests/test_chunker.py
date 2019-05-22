
import unittest

from lsst.dax.data_generator import Chunker


class ChunkerTests(unittest.TestCase):

    def testLocate(self):
        chunker = Chunker(0, 50, 5)
        result_chunk = chunker.locate((0.25, 0.25))
        self.assertEqual(result_chunk, 2500)

        result_chunk = chunker.locate((30.0, 0.0))
        self.assertEqual(result_chunk, 2408)

        result_chunk = chunker.locate((0.0, -20.0))
        self.assertEqual(result_chunk, 1900)
