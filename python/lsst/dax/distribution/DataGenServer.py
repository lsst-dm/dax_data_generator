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
import os
import socket
import threading
import yaml

from enum import Enum

from DataGenConnection import DataGenConnection
from DataGenConnection import DataGenError

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
    chunkId : integer chunk ID
    """

    def __init__(self, chunkId):
        self.chunkId = chunkId
        # `GenerationStage` of the chunk
        self.genStage = GenerationStage(GenerationStage.UNASSIGNED)
        # The client id of the client assigned to generate chunkId.
        self.clientId = '-1'
        # IP address of the client
        self.clientAddr = None

    def __repr__(self):
        return ("ChunkInfo " + str(self.chunkId) + ' ' + self.clientId +
                ' ' + str(self.clientAddr) + ' ' + str(self.genStage))

def testChunkInfo():
    """Test that __repr__ doesn't crash"""
    cInfo = ChunkInfo(9876)
    print("cInfo=", cInfo)


class DataGenServer:
    """This class is meant to provide clients with names, fake data configuration,
    and chunks that need to be generated, while keeping track of what has been
    generated where. The replicator should be able to identify duplicate chunks
    and mismatching chunks, so this process will not be concerned with that.
    """

    def __init__(self, cfgFileName, minChunkNum, maxChunkNum):
        """cfgFileName contains our port number and command line arguments
        to be sent to the fake data generating program. The contents of
        fakeCfgFileName will be copied to the clients and passed
        to the fake data genrating program. Failures creating this object
        should terminate the program.

        Parameters
        ----------
        cfgFileName : The name of the server configuration file
        minChunkNum : The bottom end of the range of chunkIds to generate.
        maxChunkNum : The top end of the range of chunks to generate.
            TODO: Both minChunkNum and maxChunkNum should be replaced by a file
            containing chunkIds to generate with a format like:
            "50-99, 105, 110, 140-300", that accepts ranges and
            individual chunkIds. This progam can then generate a file in this
            format containing failed chunkIds, which can then be fed back to
            the program.

        """

        self._cfgFileName = cfgFileName
        # Set of all chunkIds to generate. sphgeom::Chunker is used to limit
        # the list to valid chunks.
        totalChunks = set(range(minChunkNum, maxChunkNum))

        # Set to false to stop accepting and end the program
        self._loop = True
        # Sequence count, incremented to provide unique client names
        self._sequence = 1
        # lock to protect _sequence, _clients
        self._clientLock = threading.Lock()
        # lock to protect _totalGeneratedChunks, _chunksToSend,
        # _chunksToSendSet
        self._listLock = threading.Lock()
        # TODO: possibly use a file to set totalChunks
        # All the chunks generated so far
        self._totalGeneratedChunks = set()

        # Read configuration to set other values.
        with open(self._cfgFileName, 'r') as cfgFile:
            self._cfg = yaml.load(cfgFile)
            print("cfg", self._cfg)
        # The port number the host will listen to.
        self._port = self._cfg['server']['port']
        # The arguments that will be passed from server to
        # clients to dax_data_generator/bin/datagen.py.
        self._cfgFakeArgs = self._cfg['fakeDataGenerator']['arguments']
        print("port=", self._port, self._cfgFakeArgs)
        # The name and contents of the configuration file that will be passed
        # from server to clients to dax_data_generator/bin/datagen.py.
        fakeCfgFileName = self._cfg['fakeDataGenerator']['cfgFileName']
        print("fakeCfgFileName", fakeCfgFileName)
        with open(fakeCfgFileName, 'r') as file:
            self._fakeCfgData = file.read()
        print("fakeCfgData=", self._fakeCfgData)
        # Get the directory containing partioner configuration files.
        partionerCfgDir = self._cfg['partitioner']['cfgDir']
        print("partionerCfgDir=", partionerCfgDir)
        # Read all the files in that directory and their contents.
        self._partionerCfgDict = self._readPartionerCfgDir(partionerCfgDir)

        # List of client connection threads
        self._clientThreads = list()
        # Dictionary of clients by clientId
        self._clients = dict()

        # Build dictionary of info for chunks to send to workers.
        # Read the datagen configuration for chunker info.
        spec_globals = dict()
        exec(self._fakeCfgData, spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        chunker = spec_globals['chunker']
        allChunks = chunker.getAllChunks()
        self._chunksToSend = dict() # Dictionary of information on chunks to send
        # Set of chunks to send, desirable to have in order
        self._chunksToSendSet = set()
        print("Finding valid chunk numbers...")
        for chunk in totalChunks:
            if chunk in allChunks:
                chunkInfo = ChunkInfo(chunk)
                self._chunksToSend[chunk] = chunkInfo
                self._chunksToSendSet.add(chunk)
        print("len(totalChunks)=", len(totalChunks),
              "len(self._chunksToSendSet)=", len(self._chunksToSendSet))

        # Track all client connections so it is possible to
        # determine when the server's job is finished.
        self._activeClientCount = 0
        self._activeClientMtx = threading.Lock()

    def _readPartionerCfgDir(self, partionerCfgDir):
        """Read all the files ending with cfg in partionerCfgDir and
        make entries for them in a dictionary with keys indexNum, and
        values tuple of the file name and file contents. The keys
        must be sequential and start at 0.
        """
        entries = os.listdir(partionerCfgDir)
        files = list()
        for e in entries:
            if os.path.isfile(os.path.join(partionerCfgDir, e)):
                ext = os.path.splitext(e)[1]
                if ext == '.cfg':
                    files.append(os.path.basename(e))
        print("&&& partitionCfg files=", files, entries)
        fileDict = dict()
        index = 0
        for f in files:
            fName = os.path.join(partionerCfgDir, f)
            with open(fName, 'r') as file:
                fileData = file.read()
                fileDict[index] = (f, fileData)
                index += 1
        print("&&& fileDict", fileDict)
        return fileDict

    def _servAccept(self):
        """Accept connections from clients, spinning up a new thread
        to handle each one.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', self._port))
            s.listen()
            while self._loop:
                conn, addr = s.accept()
                print('Connected by', addr)
                if self._loop:
                    # start new thread
                    self._clientLock.acquire()
                    clientName = 'client' + str(self._sequence)
                    self._sequence += 1
                    self._clientLock.release()
                    print("starting thread", clientName, conn, addr)
                    thrd = threading.Thread(target=self._servToClient, args=(clientName, conn, addr))
                    self._clientThreads.append(thrd)
                    with self._activeClientMtx:
                        self._activeClientCount += 1
                    thrd.start()
        print("Accept loop shutting down")
        for j, thrd in enumerate(self._clientThreads):
            print("joining thread", j)
            thrd.join()
        print("All threads joined.")

    def _servToClient(self, name, conn, addr):
        """Handle the requests of a single client, which follow the pattern
        Initialize - provide client with its name, and command line arguments
            with the configuration file for datagen.py
        Repeated until the client disconnects-
            Requests for chunkIds to generate -
                The client will disconnect if the server sends
                it an empty list of chunkIds.
            Response with successfully generated chunkIds.
        Any chunkIds assigned to the client but not in the list of
        commpleted chunks are put in LIMBO.

        Parameters
        ----------
        name: The client's name.
        addr: The IP address of the client.
        conn: The socket connection to the client.
        """
        # Connection and communication exceptions are caught so
        # other connections can continue.
        outOfChunks = False
        try:
            print('Connected by', addr, name, conn)
            serv = DataGenConnection(conn)
            with self._clientLock:
                self._clients[name] = addr
            # receive init from client
            serv.servReqInit()
            # server sending back configuration information
            serv.servRespInit(name, self._cfgFakeArgs, self._fakeCfgData)
            # client requests partioner configuration files, starting with
            # pCfgIndex=0 and incrementing it until pCfgName==""
            pCfgDone = False
            while not pCfgDone:
                pCfgIndex = serv.servRespPartitionCfgFile()
                print("&&& pCfgIndex=", pCfgIndex)
                if pCfgIndex in self._partionerCfgDict:
                    print("&&& pCfgIndex=", pCfgIndex, " in ", self._partionerCfgDict)
                    pCfgTpl = self._partionerCfgDict[pCfgIndex]
                    pCfgName = pCfgTpl[0]
                    pCfgContents = pCfgTpl[1]
                else:
                    print("&&& pCfgIndex=", pCfgIndex, " not in ", self._partionerCfgDict)
                    pCfgName = ""
                    pCfgContents = ""
                    pCfgDone = True
                serv.servSendPartionCfgFile(pCfgIndex, pCfgName, pCfgContents)
            # client requesting chunk list
            while self._loop and not outOfChunks:
                clientReqChunkCount = serv.servRecvReqChunks()
                chunksForClient = list()
                # get the first clientReqChunkCount elements of self._chunksToSendSet
                with self._listLock:
                    for chunk in itertools.islice(self._chunksToSendSet, clientReqChunkCount):
                        chunksForClient.append(chunk)
                        cInfo = self._chunksToSend[chunk]
                        cInfo.genStage = GenerationStage.ASSIGNED
                        cInfo.clientId = name
                        cInfo.clientAddr = addr
                    for chunk in chunksForClient:
                        self._chunksToSendSet.discard(chunk)
                serv.servSendChunks(chunksForClient)
                if len(chunksForClient) == 0:
                    print("out of chunks to send, nothing more to send")
                    outOfChunks = True
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
                    with self._listLock:
                        for completed in completed_chunks:
                            self._totalGeneratedChunks.add(completed)
                            cInfo = self._chunksToSend[completed]
                            cInfo.genStage = GenerationStage.FINISHED
                    diff = serv.compareChunkLists(completed_chunks, chunksForClient)
                    if len(diff) > 0:
                        # Mark missing chunks as being in limbo.
                        with self._listLock:
                            for missing in diff:
                                cInfo = self._chunksToSend[missing]
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
        with self._activeClientMtx:
            self._activeClientCount -= 1
            if self._activeClientCount == 0 and outOfChunks:
                # Connect to our own socket to get past the accept
                # and break the loop.
                self._loop = False
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as termSock:
                    termSock.connect(('127.0.0.1', self._port))

    def chunksInState(self, genState):
        """Return a list of ChunkInfo where the genStage matches one in
        the provided genState list
        """
        chunksInState = list()
        for chk in self._chunksToSend:
            chkInfo = self._chunksToSend[chk]
            if chkInfo.genStage in genState:
                chunksInState.append(chkInfo)
        return chunksInState


    def start(self):
        print("starting")
        self._servAccept()
        print("Done, generated ", self._totalGeneratedChunks)
        print("chunks failed chunks:", self.chunksInState([GenerationStage.LIMBO, GenerationStage.ASSIGNED]))
        counts = { GenerationStage.UNASSIGNED:0,
            GenerationStage.ASSIGNED:0,
            GenerationStage.FINISHED:0,
            GenerationStage.LIMBO:0}
        for chk in self._chunksToSend:
            chkInfo = self._chunksToSend[chk]
            counts[chkInfo.genStage] += 1
        print("Chunks generated=", counts[GenerationStage.FINISHED])
        print("Chunks assigned=", counts[GenerationStage.ASSIGNED])
        print("Chunks unassigned=", counts[GenerationStage.UNASSIGNED])
        print("Chunks limbo=", counts[GenerationStage.LIMBO])

def testA():
    testChunkInfo()
    dgServ = DataGenServer("serverCfg.yml", 0, 50000)
    dgServ.start()

if __name__ == "__main__":
    testA()