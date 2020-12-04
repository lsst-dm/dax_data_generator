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

import itertools
import sys
import threading

from enum import Enum

from .DataIngest import DataIngest


class GenerationStage(Enum):
    """This class is used to indicate where a chunk is in the process
    of having synthetic data genrated.

    UNASSIGNED : The chunk has not been assigned to a worker to be generated.
    ASSIGNED : The chunk has been assigend to a worker.
    FINISHED : The assigned worker has finished generating the chunk.
    LIMBO : The chunk was assigned but never finished.
    """
    UNASSIGNED = 1
    TRANSACTION = 2
    ASSIGNED = 3
    FINISHED = 4
    LIMBO = 5


class ChunkInfo:
    """Information about a chunk including its status

    Parameters
    ----------
    chunk_id : int
        The chunk id number

    Members
    -------
    gen_stage : enum class
        This indactes how far along the chunk is in the generation process.
    client_id : int
        The id of the client program generating the chunk.
    client_addr : string
        The IP address of the client generating the chunk.
    """

    def __init__(self, chunk_id):
        self.chunk_id = chunk_id
        self.gen_stage = GenerationStage(GenerationStage.UNASSIGNED)
        self.client_id = '-1'
        self.client_addr = None  # str

    def __repr__(self):
        return ("ChunkInfo " + str(self.chunk_id) + ' ' + self.client_id +
                ' ' + str(self.client_addr) + ' ' + str(self.gen_stage))


class Transaction:
    """ Track the chunks in a transaction and the transaction id.

    chunks : set of int
        chunk ids of the chunks in the transaction.
    """

    def __init__(self, chunks):
        print(f"&&& chunks={chunks}")
        self.chunks = chunks  # destroyed as chunks moved to transactions.
        print(f"&&&a self.chunks={self.chunks}")
        self.total_chunks = chunks.copy()
        print(f"&&&a self.chunks={self.chunks}")
        self.completed_chunks = set()
        self.id = None  # id number given to the transaction
        self.abort = False
        self.closed = False

    def __repr__(self):
        s = f"total_chunks {len(self.total_chunks)} {self.total_chunks}\n"
        s += f"chunks {len(self.chunks)} {self.chunks}\n"
        s += f"completed {len(self.completed_chunks)} {self.completed_chunks}\n"
        s += f"transaction id {self.id}\n"
        s += f"abort {self.abort}\n"
        s += f"closed {self.closed}\n"
        return s


    def is_finished(self):
        print(f"&&& is_finished {self}")
        print(f"&&& is_finished {self.total_chunks == self.completed_chunks or self.abort}")
        return self.total_chunks == self.completed_chunks or self.abort


class ChunkTracking:
    """The set of chunks, with status, to send to the clients with subsets for
    individual transactions.
    """

    def __init__(self, chunker, chunk_logs_in, transaction_size, skip_ingest, skip_schema,
                 log_dir, ingest_dict):

        self._transaction_size = transaction_size
        self._skip_ingest = skip_ingest
        self._skip_schema = skip_schema
        # lock to protect internal lists
        self._list_lock = threading.Lock()
        # All the chunks generated so far
        self._total_generated_chunks = set()

        # Unique Transaction ids come from the ingest system.
        # When that system is bypassed, unique id's are needed.
        self._fake_transaction_id_sequence = -1

        all_chunks = chunker.getAllChunks()
        print("Finding valid chunk numbers...")
        # Use provided information to build the set of chunks to generate.
        chunk_logs_in.build(all_chunks)
        # Use the input information/files to create the output logs.
        print(f"&&&a log_dir {log_dir}")
        self._chunk_logs = chunk_logs_in.createOutput(log_dir)
        print(f"&&&a log_dir {log_dir}")
        if log_dir is not None:
            # Start logging
            self._chunk_logs.write()
        # Static set of all chunks to be generated.
        self._chunks_entire_set = self._chunk_logs.result_set.copy()
        # Set of chunks to send - this is destroyed as chunks are assigned.
        self._chunks_to_send_set = self._chunk_logs.result_set.copy()
        self._chunks_to_send_total = len(self._chunks_to_send_set)
        self._limbo_count = 0  # number of chunks that had problems being created.
        # Dictionary of information about chunks being sent.
        # self._chunks_to_send only includes information about this run.
        # self._chunk_logs may include information from previous runs.
        self._chunks_to_send = {}
        for chunk in self._chunks_to_send_set:
            chunk_info = ChunkInfo(chunk)
            self._chunks_to_send[chunk] = chunk_info
        print("_chunks_to_send_total=", self._chunks_to_send_total)

        # Ingest values
        ingd = ingest_dict
        self._ingest = DataIngest(ingd['host'], ingd['port'], ingd['auth'])
        self._skip_ingest = ingd['skip']
        self._db_name = ingd['db']

        self._transaction = None  # current transaction
        self._transaction_dict = {}  # dictionary of transactions by transaction id.
        self.INVALID_ID = -sys.maxsize - 1

    def remaining_chunk_count(self):
        with self._list_lock:
            return self._remaining_chunk_count()

    def _remaining_chunk_count(self):
        """Note: self._list_lock must be held when calling this function.
        """
        count = len(self._chunks_to_send_set)
        if self._transaction:
            count += len(self._transaction.chunks)
        return count

    def _build_next_transaction(self):
        """Take chunks from _chunks_to_send_set and put them in a new
        Transaction.

        Note: self._list_lock must be held when calling this function.
        """
        transaction_chunks = set()

        print(f"&&& self._transaction_size {self._transaction_size}")
        for chunk in itertools.islice(self._chunks_to_send_set, self._transaction_size):
            transaction_chunks.add(chunk)
            cInfo = self._chunks_to_send[chunk]
            cInfo.gen_stage = GenerationStage.TRANSACTION
        for chunk in transaction_chunks:
            self._chunks_to_send_set.discard(chunk)
        print(f"new transaction_chunks {transaction_chunks}")
        self._transaction = Transaction(transaction_chunks)

    def _start_transaction(self):
        """Start the latest transaction or raise a RuntimeException.
        Note: self._list_lock must be held when calling this function.
        It also asigns an id, puts it in the dictionary, and registers
        the transaction with the ingest system.
        """
        if not self._transaction.total_chunks:
            self._transaction.id = self.INVALID_ID
            self._transaction_dict[self._transaction.id] = self._transaction
            print("-----------------------------------------------")
            print(f"Not starting an empty transaction {self._transaction}")
            return
        if self._skip_ingest:
            # Return an invalid id
            print(f"skipping ingest transaction_id={self._fake_transaction_id_sequence}")
            self._transaction.id = self._fake_transaction_id_sequence
            self._fake_transaction_id_sequence -= 1
        else:
            success, id = self._ingest.startTransaction(self._db_name)
            if not success:
                print("ERROR Failed to start transaction ", self._db_name)
                raise RuntimeError("ERROR failed to start transaction ", self._db_name)
            self._transaction.id = id
        print(f"new transaction started {self._transaction.id}")
        self._transaction_dict[self._transaction.id] = self._transaction
        print("-----------------------------------------------")
        print("Transaction started ", self._db_name, "id=", self._transaction.id)
        return

    def abort_and_close(self, transaction_id):
        """&&&
        """
        print("Aborting transaction ", transaction_id)
        with self._list_lock:
            transaction = self._transaction_dict[transaction_id]
            transaction.abort = True
            self._close_transaction(transaction_id)

    def _close_transaction(self, transaction_id):
        """End the transaction, aborting if indicated.

        Parameters
        ----------
        transaction_id : int
            Transaction id.

        Return
        ------
        success : bool
            True if successfully closed the transaction.
        status : int
            Status value of the put action.
        content : json or None
            Information about the success or failure of the operation.

        Note: self._list_lock must be held when calling this function
        """
        print(f"&&& _transaction_dict {self._transaction_dict}")
        print(f"&&& transaction_id={transaction_id}")
        transaction = self._transaction_dict[transaction_id]
        if not transaction:
            print(f"No Transaction for {transaction_id}")
            return False, -1, None
        if transaction.closed:
            print(f"Transaction was already closed {transaction_id}")
            return False, -1, None
        abort = transaction.abort
        print("Transaction end abort=", abort)
        if transaction_id < 0:
            print("No real transaction to end, closing", transaction_id)
            transaction.closed = True
            return True, -1, None
        print("_close_transaction id={transaction_id} abort={abort}")
        success, status, content = self._ingest.endTransaction(transaction_id, abort)
        transaction.closed = True
        return success, status, content

    def get_chunks_for_client(self, client_name, client_addr, req_chunk_count):
        """Get a list of chunks for a client to generate.

        Parameters
        ----------
        req_chunk_count :int
            The maximum number of chunks the client wants to recieve.

        Returns
        -------
        chunks_for_client : set of int
            A set of chunks the client should generate
        transaction_id : int
            Id number of the current transaction.
        """
        with self._list_lock:
            ret_set = set()
            if (not self._transaction) or (not self._transaction.chunks) or self._transaction.abort:
                print("Creating a new transaction.")
                # create a new transaction_set
                self._build_next_transaction()
                # start the new transaction
                self._start_transaction()

            # Get chunks to send from self._transaction and remove them
            # from self._transaction.chunks
            for chunk in itertools.islice(self._transaction.chunks, req_chunk_count):
                ret_set.add(chunk)
                cInfo = self._chunks_to_send[chunk]
                cInfo.gen_stage = GenerationStage.ASSIGNED
                cInfo.client_id = client_name
                cInfo.client_addr = client_addr
            self._chunk_logs.addAssigned(ret_set)
            print(f"chunks_for client t_id={self._transaction.id} chunks to send={ret_set}")
            for chunk in ret_set:
                self._transaction.chunks.discard(chunk)
        return ret_set, self._transaction.id

    def client_results(self, transaction_id, expected_chunks, completed_chunks):
        """Remove completed_chunks from the transaction, abort if chunks missing.

        Parameters
        ----------
        transaction_id : int
            Transaction id.
        expected_chunks : set of int
            Chunks that the client should have generated.
        completed_chunks : list of int
            Chunk the client did generate.
        """
        # Check for INVALID_ID, if so there should be no chunks
        if transaction_id == self.INVALID_ID:
            print(f"transacation_id is INVALID_ID, indicating nothing left to send")
            self._close_transaction(transaction_id)
            if completed_chunks or expected_chunks:
                print(f"ERROR at least one not empty:")
                print(f"  completed_chunks={completed_chunks}")
                print(f"  expected_chunks={expected_chunks}")
                return
        for completed in completed_chunks:
            self._total_generated_chunks.add(completed)
            cInfo = self._chunks_to_send[completed]
            cInfo.gen_stage = GenerationStage.FINISHED
        completed_set = set(completed_chunks)
        diff = expected_chunks ^ completed_set
        print(f"&&& expected={expected_chunks}")
        print(f"&&& complete={completed_set}")
        print(f"&&& diff={diff}")
        with self._list_lock:
            # get the correct transaction
            transaction = self._transaction_dict[transaction_id]
            if len(diff) > 0:
                print(f"Error, missing chunks t_id={transaction_id} diff={diff}")
                # Mark missing chunks as being in limbo.
                self._chunk_logs.addLimbo(diff)
                for missing in diff:
                    cInfo = self._chunks_to_send[missing]
                    cInfo.gen_stage = GenerationStage.LIMBO
                    self._limbo_count += 1
                # Abort the transaction
                transaction.abort = True
                self._close_transaction(transaction_id)
                # &&& TODO: maybe something useful to do here
                return

            # Check if the transaction is completed and
            # close it if it is.
            print(f"&&& t_comp={transaction.completed_chunks}  c_c={completed_chunks}")
            transaction.completed_chunks = transaction.completed_chunks.union(completed_chunks)
            print(f"&&& t_comp={transaction.completed_chunks}")
            if transaction.is_finished():
                print(f"Transaction {transaction_id} finished #chunks={len(transaction.completed_chunks)}")
                self._close_transaction(transaction_id)
                self._chunk_logs.addCompleted(transaction.completed_chunks)

            total_to_send = self._chunks_to_send_total
            chunks_left = len(self._chunks_to_send_set)
            chunks_in_transactions = 0
            for t_id, t_val in self._transaction_dict.items():
                if not t_val.closed:
                    chunks_in_transactions += len(t_val.total_chunks)
            completed_count = len(self._total_generated_chunks)
            limbo_count = self._limbo_count

        print('Chunks total      =', total_to_send)
        print('  left            =', chunks_left)
        print('  in transactions =', chunks_in_transactions)
        print('  finished        =', completed_count)
        print('  in limbo        =', limbo_count)
        print('  processing      =', (total_to_send -
                                      (chunks_left + completed_count + limbo_count)))



