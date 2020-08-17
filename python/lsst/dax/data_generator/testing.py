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

import numpy as np

from . import columns
from .chunker import Chunker

def equalBlocks(block_a, block_b):
    """Each block should be a tuple of np.array. Return True if
    both blocks contain the same arrays in the same order.
    """
    if len(block_a) != len(block_b):
        return False
    for j in range(len(block_a)):
        a = block_a[j]
        b = block_b[j]
        if not np.array_equal(a, b):
            return False
    return True

def tst_convertBlockToRows(log_msgs=True):
    """Test convertBlockToRows.

    Paramters
    ---------
    log_msgs : bool
        True if non-error messages should be printed.

    Return
    ------
    success : bool
        True when the test was successful.
    """
    vals1 = np.array([101, 102, 103])
    vals2 = np.array([309, 308, 307])
    vals3 = np.array([951, 952, 953])
    blockA = (vals1, vals2, vals3)
    rows_expected = []
    rows_expected.append(list([101, 309, 951]))
    rows_expected.append(list([102, 308, 952]))
    rows_expected.append(list([103, 307, 953]))
    rowsA = columns.convertBlockToRows(blockA)
    if log_msgs:
        print("rowsA=", rowsA)
        print("expected=", rows_expected)
    if rowsA != rows_expected:
        print("FAILED rows did not match a=", rowsA, " expected=", rows_expected)
        return False
    return True

def tst_mergeBlocks(log_msgs=True):
    """Test mergeBlocks

    Paramters
    ---------
    log_msgs : bool
        True if non-error messages should be printed.

    Return
    ------
    success : bool
        True when the test was successful.
    """
    vals1 = np.array([101, 102, 103, 104, 105])
    vals2 = np.array([309, 308, 307, 306, 305])
    vals3 = np.array([951, 952, 953, 954, 955])
    blockA = (vals1, vals2, vals3)
    vals4 = np.array([201, 202, 203, 204, 205, 207])
    vals5 = np.array([609, 608, 607, 606, 605, 604])
    vals6 = np.array([751, 752, 753, 754, 755, 756])
    blockB = (vals4, vals5, vals6)
    blockC = columns.mergeBlocks(blockA, blockB)
    if log_msgs:
        print("blockA=",blockA)
        print("blockB=",blockB)
        print("blockC=",blockC)
    expected1 = np.array([101, 102, 103, 104, 105, 201, 202, 203, 204, 205, 207])
    expected2 = np.array([309, 308, 307, 306, 305, 609, 608, 607, 606, 605, 604])
    expected3 = np.array([951, 952, 953, 954, 955, 751, 752, 753, 754, 755, 756])
    block_expected = (expected1, expected2, expected3)
    success = None
    if not equalBlocks(blockC, block_expected):
        print("FAILED blocks do not match expected=", block_expected, " C=", blockC)
        success = False
    bad3 = np.array([951, 952, 953, 954, 955, 751, 752, 753, 754, 375, 756])
    block_bad = (expected1, expected2, bad3)
    if equalBlocks(blockC, block_bad):
        print("FAILED to detect differences in blocks bad=", block_bad, " C=", blockC)
        success = False
    if not columns.containsBlock(blockA, blockC):
        print("FAILED blockA should have been found in blockC A=", blockA, " C=", blockC)
        success = False
    if columns.containsBlock(block_bad, block_expected):
        print("FAILED blockBad should not have been found in blockExpected bad=", block_bad,
              "ex=", block_expected)
        success = False
    # check for duplicate row
    blockBad2 = (np.array([101, 101]), np.array([309, 309]), np.array([951, 951]))
    if columns.containsBlock(blockBad2, blockC):
        print("FAILED blockBad2 should not have been found in blockC bad=", block_bad,
              "C=", blockC)
        success = False

    if success is None:
        success = True
    return success


def tst_RaDecGenerator(log_msgs=True, every_nth=75):
    """Test RaDecGenerator.

    Paramters
    ---------
    log_msgs : bool
        True if non-error messages should be printed.
    every_nth : int
        Starting from 0, every_nth valid chunk id number
        and the last valid chunk number will be used in
        the test. This gets both poles and a fair number
        of other chunks in between.

    Return
    ------
    success : bool
        True when the test was successful.
    """
    success = None
    # setup chunking information
    num_stripes = 200
    num_substripes = 5
    localChunker = Chunker(0, num_stripes, num_substripes)

    allChunks = localChunker.getAllChunks()
    if log_msgs: print("allChunks=", len(allChunks))
    many_chunks = allChunks[0::every_nth]
    # Get both north and south pole chunks.
    if many_chunks[-1] != allChunks[-1]:
        many_chunks.append(allChunks[-1])
    length = 10000
    # a bit more than an arcminute for edge_width.
    edge_width = 0.017
    col_gen = columns.RaDecGenerator(localChunker)
    seed = 1
    blocksA = {}
    for chunk_id in many_chunks:
        blocksA[chunk_id] = col_gen(chunk_id, length, seed, edge_width, edge_only=False)
        if log_msgs: print("blocksA[", chunk_id, "]=", blocksA[chunk_id])

    blocksB = {}
    for chunk_id in many_chunks:
        blocksB[chunk_id] = col_gen(chunk_id, length, seed, edge_width, edge_only=False)
        if log_msgs: print("blocksB[", chunk_id, "]=", blocksB[chunk_id])

    blocksC = {}
    for chunk_id in many_chunks:
        blocksC[chunk_id] = col_gen(chunk_id, length, seed, edge_width, edge_only=True)
        if log_msgs: print("blocksC[", chunk_id, "]=", blocksC[chunk_id])

    blocksD = {}
    for chunk_id in many_chunks:
        blocksD[chunk_id] = col_gen(chunk_id, length, seed, edge_width, edge_only=True)
        if log_msgs: print("blocksD[", chunk_id, "]=", blocksD[chunk_id])

    for chunk_id in many_chunks:
        blockA = blocksA[chunk_id]
        blockB = blocksB[chunk_id]
        if not equalBlocks(blockA, blockB):
            print("FAILED blocks not equal chunk_id=", chunk_id)
            success = False
        blockC = blocksC[chunk_id]
        blockD = blocksD[chunk_id]
        if not equalBlocks(blockC, blockD):
            print("FAILED edgeOnly blocks not equal chunk_id=", chunk_id)
            success = False
        if not columns.containsBlock(blockC, blockA):
            print("FAILED edgeOnly blocks not found in larger block chunk_id=", chunk_id)

    if success is None:
        success = True
    return success
