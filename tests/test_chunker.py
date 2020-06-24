
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

        # south pole
        result_chunks = chunker.getChunksAround(0, 0.018)
        self.assertEqual(result_chunks, [0, 100, 101, 102, 103, 104])

        # north pole
        result_chunks = chunker.getChunksAround(4900, 0.018)
        self.assertEqual(result_chunks, [4800, 4801, 4802, 4803, 4804, 4900])

        # first chunk of a stripe
        result_chunks = chunker.getChunksAround(2600, 0.018)
        self.assertEqual(result_chunks, [2500, 2501, 2598, 2600, 2601, 2698, 2700, 2797])

        # mid stripe chunk
        result_chunks = chunker.getChunksAround(1621, 0.018)
        self.assertEqual(result_chunks, [1519, 1520, 1620, 1621, 1622, 1721, 1722])


