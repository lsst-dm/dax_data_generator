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

import getopt
import itertools
import os
import socket
import sys
import threading
import yaml

from enum import Enum

from DataGenConnection import DataGenConnection
from DataGenConnection import DataGenError
from DataIngest import DataIngest

class GenerationStage(Enum):
    """This class is used to indicate where a chunk is in the process
    of having synthetic data genrated.

    UNASSIGNED : The chunk has not been assigned to a worker to be generated.
    ASSIGNED : The chunk has been assigend to a worker.
    FINISHED : The assigned worker has finished generating the chunk.
    LIMBO : The chunk was assigned but never finished.
    """
    UNASSIGNED = 1
    ASSIGNED = 2
    FINISHED = 3
    LIMBO = 4


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
        self.client_addr = None

    def __repr__(self):
        return ("ChunkInfo " + str(self.chunk_id) + ' ' + self.client_id +
                ' ' + str(self.client_addr) + ' ' + str(self.gen_stage))


class DataGenServer:
    """This class is meant to provide clients with the information needed
    to generate chunks.

    Parameters
    ----------
    cfg_file_name : string
        The name of the server configuration file
    min_chunk_num : int
        The bottom end of the range of chunkIds to generate.
    max_chunk_num : int
        The top end of the range of chunks to generate.
        TODO: Both min_chunk_num and max_chunk_num should be replaced by a file
            containing chunkIds to generate with a format like:
            "50-99, 105, 110, 140-300", that accepts ranges and
            individual chunkIds. This progam can then generate a file in this
            format containing failed chunkIds, which can then be fed back to
            the program.
    skip_ingest : bool
        When true, do not try to pass generated files to the ingest system.
    skip_schema : bool
        When true, expect attempts to send schemas to ingest to fail.

    Notes
    -----
    This class is meant to provide clients with names, fake data configuration,
    and chunks that need to be generated. While also keeping track of what has
    been generated where. The replicator should be able to identify duplicate
    chunks and mismatching chunks, so this process will not be concerned
    with that.

    cfg_file_name contains our port number and the command line arguments
    to be sent to the fake data generating program. The contents of
    fake_cfg_file_name will be copied to the clients and passed
    to the fake data genrating program. Failures creating this object
    should terminate the program.
    """

    def __init__(self, cfg_file_name, min_chunk_num, max_chunk_num,
                 skip_ingest, skip_schema):
        self._cfgFileName = cfg_file_name
        # Set of all chunkIds to generate. sphgeom::Chunker is used to limit
        # the list to valid chunks.
        total_chunks = set(range(min_chunk_num, max_chunk_num))
        self._skip_ingest = skip_ingest
        self._skip_schema = skip_schema
        # Set to false to stop accepting and end the program
        self._loop = True
        # Sequence count, incremented to provide unique client names
        self._sequence = 1
        # lock to protect _sequence, _clients
        self._client_lock = threading.Lock()
        # lock to protect _total_generated_chunks, _chunksToSend,
        # _chunksToSendSet
        self._list_lock = threading.Lock()
        # All the chunks generated so far
        self._total_generated_chunks = set()

        # Read configuration to set other values.
        with open(self._cfgFileName, 'r') as cfgFile:
            self._cfg = yaml.load(cfgFile)
            print("cfg", self._cfg)
        # The port number the host will listen to.
        self._port = self._cfg['server']['port']
        # The arguments that will be passed from server to
        # clients to dax_data_generator/bin/datagen.py.
        self._cfg_fake_args = self._cfg['fakeDataGenerator']['arguments']
        print("port=", self._port, self._cfg_fake_args)
        # The name and contents of the configuration file that will be passed
        # from server to clients to dax_data_generator/bin/datagen.py.
        fake_cfg_file_name = self._cfg['fakeDataGenerator']['cfgFileName']
        print("fake_cfg_file_name", fake_cfg_file_name)
        with open(fake_cfg_file_name, 'r') as file:
            self._fakeCfgData = file.read()
        print("fake_cfg_data=", self._fakeCfgData)  #&&& rename self._fakeCfgData

        # Get the directory containing partioner configuration files.
        partioner_cfg_dir = os.path.abspath(self._cfg['partitioner']['cfgDir'])
        print("partioner_cfg_dir=", partioner_cfg_dir)
        # Read all the files in that directory and their contents.
        self._partioner_cfg_dict = self._readPartionerCfgDir(partioner_cfg_dir)

        # Get ingest sytem information
        self._db_name = self._cfg['ingest']['dbName']
        ingest_host = self._cfg['ingest']['host']
        ingest_port = self._cfg['ingest']['port']
        ingest_user = self._cfg['ingest']['user']
        ingest_auth = self._cfg['ingest']['authKey']
        self._ingest_dict = {'host':ingest_host, 'port':ingest_port, 'user':ingest_user, 'auth':ingest_auth,
                            'db':self._db_name, 'skip':self._skip_ingest}
        self._ingest_cfg_dir = os.path.abspath(self._cfg['ingest']['cfgDir'])
        print("ingest addr=", ingest_host, ":", ingest_port, " user=", ingest_user)
        print("ingest cfg dir=", self._ingest_cfg_dir)
        self._ingest = DataIngest(ingest_host, ingest_port, ingest_user, ingest_auth)

        # List of client connection threads
        self._client_threads = list()
        # Dictionary of clients by clientId
        self._clients = dict()

        # Build dictionary of info for chunks to send to workers.
        # Read the datagen configuration for chunker info.
        spec_globals = dict()
        exec(self._fakeCfgData, spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        chunker = spec_globals['chunker']
        all_chunks = chunker.getAllChunks()
        self._chunks_to_send = dict() # Dictionary of information on chunks to send
        # Set of chunks to send, desirable to have in order but not essential.
        self._chunks_to_send_set = set()
        print("Finding valid chunk numbers...")
        for chunk in total_chunks:
            if chunk in all_chunks:
                chunk_info = ChunkInfo(chunk)
                self._chunks_to_send[chunk] = chunk_info
                self._chunks_to_send_set.add(chunk)
        print("len(totalChunks)=", len(total_chunks),
              "len(self._chunksToSendSet)=", len(self._chunks_to_send_set))

        # Track all client connections so it is possible to
        # determine when the server's job is finished.
        self._active_client_count = 0
        self._active_client_mtx = threading.Lock()

    def _readPartionerCfgDir(self, partioner_cfg_dir):
        """Read in all the files ending with cfg in partioner_cfg_dir.

        Parameters
        ----------
        partioner_cfg_dir : string
            The directory containing partioner config files.

        Returns
        -------
        dictionary :
            Keys are sequential integers starting at 0
            Values are tuples of file name and file contents.

        Note
        ----
        All the files ending with '.csv' will be read in and entries
        for them will be put in a dictionary with integer keys, and
        values being a tuple of the file name and file contents. The keys
        must be sequential and start at 0, as the clients ask for them by
        by number starting at 0.
        """
        entries = os.listdir(partioner_cfg_dir)
        files = list()
        for e in entries:
            if os.path.isfile(os.path.join(partioner_cfg_dir, e)):
                ext = os.path.splitext(e)[1]
                if ext == '.cfg':
                    files.append(os.path.basename(e))
        print("partitionCfg files=", files, entries)
        file_dict = dict()
        index = 0
        for f in files:
            fName = os.path.join(partioner_cfg_dir, f)
            with open(fName, 'r') as file:
                file_data = file.read()
                file_dict[index] = (f, file_data)
                index += 1
        print("file_dict", file_dict)
        return file_dict

    def _servAccept(self):
        """Accept connections from clients, spinning up a new thread
        to handle each one. This ends when there are no more chunk ids
        to send and all threads have joined.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', self._port))
            s.listen()
            while self._loop:
                conn, addr = s.accept()
                print('Connected by', addr)
                if self._loop:
                    # start new thread
                    self._client_lock.acquire()
                    clientName = 'client' + str(self._sequence)
                    self._sequence += 1
                    self._client_lock.release()
                    print("starting thread", clientName, conn, addr)
                    thrd = threading.Thread(target=self._servToClient, args=(clientName, conn, addr))
                    self._client_threads.append(thrd)
                    with self._active_client_mtx:
                        self._active_client_count += 1
                    thrd.start()
        print("Accept loop shutting down")
        for j, thrd in enumerate(self._client_threads):
            print("joining thread", j)
            thrd.join()
        print("All threads joined.")

    def _servToClient(self, name, conn, addr):
        """Handle the requests of a single client.

        Parameters
        ----------
        name : string
            The client's name.
        conn : socket connection
            The socket connection to the client.
        addr : string
            The IP address of the client.

        Notes
        -----
        The request from the client follow the pattern:
        Initialize - provide client with its name, and command line arguments
            with the configuration files for datagen.py, sph-partition, etc.
        Repeated until the client disconnects-
            Requests for chunkIds to generate -
                The client will disconnect if the server sends
                it an empty list of chunkIds.
            Response with successfully generated chunkIds.
        Any chunkIds assigned to the client but not in the list of
        commpleted chunks are put in LIMBO.
        """
        # Connection and communication exceptions are caught so
        # other connections can continue.
        out_of_chunks = False
        try:
            print('Connected by', addr, name, conn)
            serv = DataGenConnection(conn)
            with self._client_lock:
                self._clients[name] = addr
            # receive init from client
            serv.servReqInit()
            # server sending back configuration information
            print("&&& ingest_dict=", self._ingest_dict)
            serv.servRespInit(name, self._cfg_fake_args, self._fakeCfgData, self._ingest_dict)
            # client requests partioner configuration files, starting with
            # pCfgIndex=0 and incrementing it until pCfgName==""
            pCfgDone = False
            while not pCfgDone:
                pCfgIndex = serv.servRespPartitionCfgFile()
                if pCfgIndex in self._partioner_cfg_dict:
                    pCfgTpl = self._partioner_cfg_dict[pCfgIndex]
                    pCfgName = pCfgTpl[0]
                    pCfgContents = pCfgTpl[1]
                else:
                    pCfgName = ""
                    pCfgContents = ""
                    pCfgDone = True
                serv.servSendPartionCfgFile(pCfgIndex, pCfgName, pCfgContents)
            # client requesting chunk list
            while self._loop and not out_of_chunks:
                clientReqChunkCount = serv.servRecvReqChunks()
                chunksForClient = list()
                # get the first clientReqChunkCount elements of self._chunksToSendSet
                with self._list_lock:
                    for chunk in itertools.islice(self._chunks_to_send_set, clientReqChunkCount):
                        chunksForClient.append(chunk)
                        cInfo = self._chunks_to_send[chunk]
                        cInfo.genStage = GenerationStage.ASSIGNED
                        cInfo.clientId = name
                        cInfo.clientAddr = addr
                    for chunk in chunksForClient:
                        self._chunks_to_send_set.discard(chunk)
                serv.servSendChunks(chunksForClient)
                if len(chunksForClient) == 0:
                    print("out of chunks to send, nothing more to send")
                    out_of_chunks = True
                    conn.close()
                else:
                    # receive completed chunks from client
                    completed_chunks = []
                    finished = False
                    while not finished:
                        completedC, finished, problem = serv.servRecvChunksComplete()
                        print("serv got", completedC, finished, problem)
                        completed_chunks.extend(completedC)
                    # Mark completed chunks as finished
                    with self._list_lock:
                        for completed in completed_chunks:
                            self._total_generated_chunks.add(completed)
                            cInfo = self._chunks_to_send[completed]
                            cInfo.genStage = GenerationStage.FINISHED
                    diff = serv.compareChunkLists(completed_chunks, chunksForClient)
                    if len(diff) > 0:
                        # Mark missing chunks as being in limbo.
                        with self._list_lock:
                            for missing in diff:
                                cInfo = self._chunks_to_send[missing]
                                cInfo.genStage = GenerationStage.LIMBO
        except socket.gaierror as e:
            print("breaking connection", addr, name, "socket.gaierror:", e)
        except socket.error as e:
            print("breaking connection", addr, name, "socket.error:", e)
        except DataGenError as e:
            print("breaking connection", addr, name, "DataGenError:", e.msg)

        print("_servToClient loop is done", addr, name)
        # Decrement the number of running client connections and
        # possibly end the program.
        with self._active_client_mtx:
            self._active_client_count -= 1
            if self._active_client_count == 0 and out_of_chunks:
                # Connect to our own socket to get past the accept
                # and break the loop.
                self._loop = False
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as termSock:
                    termSock.connect(('127.0.0.1', self._port))

    def chunksInState(self, genState):
        """Return a list of ChunkInfo where the genStage matches one in
        the provided genState list
        """
        chunks_in_state = list()
        for chk in self._chunks_to_send:
            chk_info = self._chunks_to_send[chk]
            if chk_info.genStage in genState:
                chunks_in_state.append(chk_info)
        return chunks_in_state

    def connectToIngest(self):
        """Test if ingest is available and send database info if it is.
        """
        if self._skip_ingest:
            print("Skipping ingest connect")
            return True
        if not self._ingest.isIngestAlive():
            raise RuntimeError("Failed to contact ingest", self._ingest)

        if self._skip_schema:
            print("Skipping database and schema file ingest.")
            return True
        db_jfile = self._db_name + ".json"
        db_jpath = os.path.join(self._ingest_cfg_dir, db_jfile)
        print("sending db config to ingest", db_jpath)
        if not self._ingest.sendDatabase(db_jpath):
            raise RuntimeError("Failed to send databse to ingest.", db_jpath, self._ingest)
        # Find all of the schema files in self._ingest_cfg_dir while
        # ignoring the database config file.
        entries = os.listdir(self._ingest_cfg_dir)
        files = list()
        for e in entries:
            full_path = os.path.join(self._ingest_cfg_dir, e)
            if os.path.isfile(full_path):
                ext = os.path.splitext(e)[1]
                if ext == '.json':
                    fname = os.path.basename(e)
                    if not fname == db_jfile:
                        files.append(full_path)
        # Send each config file to ingest
        for f in files:
            print("Sending schema file to ingest", f)
            if not self._ingest.sendTableSchema(f):
                raise RuntimeError("Failed to send schema file to ingest", f)
        return True

    def start(self):
        """Start the server and print the results.
        """
        print("Registering database and schema with ingest system.")
        self.connectToIngest()
        print("starting")
        self._servAccept()
        print("Done, generated ", self._total_generated_chunks)
        print("chunks failed chunks:", self.chunksInState([GenerationStage.LIMBO, GenerationStage.ASSIGNED]))
        counts = {GenerationStage.UNASSIGNED:0,
            GenerationStage.ASSIGNED:0,
            GenerationStage.FINISHED:0,
            GenerationStage.LIMBO:0}
        for chk in self._chunks_to_send:
            chk_info = self._chunks_to_send[chk]
            counts[chk_info.genStage] += 1
        print("Chunks generated=", counts[GenerationStage.FINISHED])
        print("Chunks assigned=", counts[GenerationStage.ASSIGNED])
        print("Chunks unassigned=", counts[GenerationStage.UNASSIGNED])
        print("Chunks limbo=", counts[GenerationStage.LIMBO])
        # Publish database if all chunks were generated.
        if self._skip_ingest:
            print("skipping publishing")
            return
        incompleteCount = (counts[GenerationStage.ASSIGNED]
            + counts[GenerationStage.UNASSIGNED] + counts[GenerationStage.LIMBO])
        if counts[GenerationStage.FINISHED] > 0 and incompleteCount == 0:
            print("All chunks generated and ingested, publishing", self._db_name)
            success, status, r_json = self._ingest.publishDatabase(self._db_name)
            if success:
                print("Published", self._db_name)
            else:
                print("ERROR failed to publish", self._db_name, status, r_json)
        else:
            print("Not publishing due to incomplete data/creation/ingestion")


def testA():
    argumentList = sys.argv[1:]
    print("&&& argumentList=", argumentList)
    options = "hksi:"
    long_options = ["help", "skipIngest", "skipSchema", "ingestCfg"]
    skip_ingest = False
    skip_schema = False
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        print("&&& arguments=", arguments)
        for arg, val in arguments:
            print("&&& arg=", arg)
            if arg in ("-h", "--help"):
                print("-h, --help  help")
                print("-k, --skipIngest  skip trying to ingest anything")
                print("-s, --skipSchema  skip sending schema, needed when schema was already sent.")
                return False
            elif arg in ("-k", "--skipIngest"):
                skip_ingest = True
            elif arg in ("-s", "--skipSchema"):
                skip_schema = True
    except getopt.error as err:
        print (str(err))
        exit(1)
    print("&&&skip_ingest=", skip_ingest, "skip_schema=", skip_schema, "values=", values)
    #dgServ = DataGenServer("serverCfg.yml", 0, 50000, skip_ingest, skip_schema) &&& restore
    dgServ = DataGenServer("serverCfg.yml", 0, 1000, skip_ingest, skip_schema)
    dgServ.start()

if __name__ == "__main__":
    testA()