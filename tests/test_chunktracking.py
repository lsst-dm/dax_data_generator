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


import unittest
import tempfile

import lsst.dax.distribution.chunklogs as chunklogs
import lsst.dax.distribution.chunktracking as chunktracking

from lsst.dax.data_generator import Chunker


local_chunker = Chunker(0, 50, 5)

# Valid chunks for raw='1:1000' with Chunker(0, 50, 5)
valid_chunks = set([0, 100, 101, 102, 103, 104, 200, 201, 202, 203, 204, 205,
                    206, 207, 208, 209, 210, 211, 300, 301, 302, 303, 304, 305,
                    306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317,
                    400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411,
                    412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423,
                    500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511,
                    512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523,
                    524, 525, 526, 527, 528, 529, 600, 601, 602, 603, 604, 605,
                    606, 607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617,
                    618, 619, 620, 621, 622, 623, 624, 625, 626, 627, 628, 629,
                    630, 631, 632, 633, 634, 635, 700, 701, 702, 703, 704, 705,
                    706, 707, 708, 709, 710, 711, 712, 713, 714, 715, 716, 717,
                    718, 719, 720, 721, 722, 723, 724, 725, 726, 727, 728, 729,
                    730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741,
                    800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811,
                    812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823,
                    824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835,
                    836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847,
                    900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911,
                    912, 913, 914, 915, 916, 917, 918, 919, 920, 921, 922, 923,
                    924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 935,
                    936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 947,
                    948, 949, 950, 951, 952, 1000])


class ChunkTrackingTests(unittest.TestCase):

    def test_chunk_trakcing(self):
        with tempfile.TemporaryDirectory() as log_dir:
            clfs = chunklogs.ChunkLogs(None, raw='0:1000')
            db_name = 'junk_db'
            # ingest will not be contacted in unit tests
            skip_ingest = True
            skip_schema = True
            keep_csv = True
            ingest_dict = {'host': '127.0.0.1', 'port': 25080, 'auth': '',
                            'db': db_name, 'skip': skip_ingest, 'keep': keep_csv}
            c_t = chunktracking.ChunkTracking(local_chunker, clfs, 100, skip_ingest, skip_schema, log_dir,
                                            ingest_dict)
            self.assertSetEqual(c_t._chunks_to_send_set, valid_chunks)

            client_chunks, transaction_id = c_t.get_chunks_for_client(7, "some.pc.edu", 5)
            print(f"t_id={transaction_id} client_chunks={client_chunks}")
            print(f" {c_t._transaction}")

            self.assertTrue(c_t._transaction.id == transaction_id)
            self.assertTrue(c_t._transaction == c_t._transaction_dict[transaction_id])
            self.assertSetEqual(c_t._transaction.total_chunks, c_t._transaction.chunks.union(client_chunks))
            self.assertTrue(c_t._transaction.total_chunks != c_t._transaction.chunks)
            self.assertTrue(c_t._transaction.chunks.isdisjoint(client_chunks))

            # Pretend that the chunks were sent to the client and the client created all of them
            completed_chunks = client_chunks.copy()
            c_t.client_results(transaction_id, client_chunks, completed_chunks)
            self.assertFalse(c_t._transaction.closed)
            self.assertFalse(c_t._transaction.abort)

            # loop through until nothing left to send
            first = True
            cl_chunks = set()
            while cl_chunks or first:
                first = False
                print("loop start")
                cl_chunks, t_id = c_t.get_chunks_for_client(12, "some.pc.edu", 5)
                self.assertTrue(c_t._transaction.chunks.isdisjoint(cl_chunks))
                self.assertFalse(c_t._transaction.closed)
                self.assertFalse(c_t._transaction.abort)

                # Check that Transactions contain appropriate sets.
                chunks_in_all_transactions = set()
                for t_id, t_val in c_t._transaction_dict.items():
                    chunks_in_all_transactions = chunks_in_all_transactions | t_val.total_chunks
                self.assertTrue(c_t._chunks_to_send_set.isdisjoint(chunks_in_all_transactions))
                union_to_send_all_trans = c_t._chunks_to_send_set | chunks_in_all_transactions
                self.assertSetEqual(c_t._chunks_entire_set, union_to_send_all_trans)

                # Pretend that the chunks were sent to the client and the client created all of them
                completed_chunks = cl_chunks.copy()
                c_t.client_results(t_id, cl_chunks, completed_chunks)

            print(f"c_t._transaction={c_t._transaction}")

            self.assertTrue(c_t._transaction.closed)
            self.assertFalse(c_t._transaction.abort)
            self.assertTrue(len(c_t._chunks_to_send_set) == 0)
            self.assertSetEqual(c_t._chunks_entire_set, c_t._chunk_logs._completed.chunk_set)
        return
