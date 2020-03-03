#!/usr/bin/env python3

import itertools
import socket
import threading
import yaml

from enum import Enum

from DataGenConnection import DataGenConnection
from DataGenConnection import DataGenError

class GenerationStage(Enum):
    UNASSIGNED = 1
    ASSIGNED = 2
    FINISHED = 3
    LIMBO = 4


class ChunkInfo:
    """Information about a chunk including its status"""

    def __init__(self, chunkId):
        self.chunkId = chunkId
        self.genStage = GenerationStage(GenerationStage.UNASSIGNED)
        self.clientId = '-1'

    def __repr__(self):
        return("ChunkInfo " + str(self.chunkId) + ' ' + self.clientId + ' ' + str(self.genStage))

def testChunkInfo():
    cInfo = ChunkInfo(9876)
    print("cInfo=", cInfo)


class DataGenServer:
    """This class is meant to provide clients with names, fake data configuration, 
    and chunks that need to be generated, while keeping track of what has been
    generated where.
    """ 

    def __init__(self, cfgFileName, minChunkNum, maxChunkNum):
        """cfgFileName contains our port number and command line arguments
        to be sent to the fake data generating program. The contents of
        fakeCfgFileName will be copied to the clients and passed
        to the fake data genrating program.
        """
        self._loop = True # Set to false to stop accepting and end the program
        self._sequence = 1 # Increment to provide unique client names
        self._clientLock = threading.Lock() # lock to protect _sequence, _clients
        # lock to protect _totalGeneratedChunks, _totalChunks, _chunksToSend, 
        # _chunksToSendSet
        self._listLock = threading.Lock() 
        self._cfgFileName = cfgFileName
        # TODO: possibly use a file to set _totalChunks
        self._totalChunks = set(range(minChunkNum, maxChunkNum)) # chunks to generate
        print("&&& totalChunks", self._totalChunks)
        self._totalGeneratedChunks = set() # All the chunks generated so far
        with open(self._cfgFileName, 'r') as cfgFile:
            self._cfg = yaml.load(cfgFile)
            print("&&& cfg", self._cfg)
        self._host = socket.gethostname() # name of our host
        self._port = self._cfg['server']['port'] # port number to listen on
        self._cfgFakeArgs = self._cfg['fakeDataGenerator']['arguments']
        print("&&& host=", self._host, "port=", self._port, self._cfgFakeArgs)
        self._fakeCfgFileName = self._cfg['fakeDataGenerator']['cfgFileName'] 
        print("&&& fakeCfgFileName", self._fakeCfgFileName)
        with open(self._fakeCfgFileName, 'r') as file:
            self._fakeCfgData = file.read()
        print("&&& fakeCfgData=", self._fakeCfgData)
        self._clientThreads = list() # list of client threads
        self._clients = dict() # list of clients
        # Build dictionary of info for chunks to send to workers.
        self._chunksToSend = dict() 
        for chunk in self._totalChunks:
            chunkInfo = ChunkInfo(chunk)
            self._chunksToSend[chunk] = chunkInfo
        # A set of all chunks that have not yet been sent
        self._chunksToSendSet = self._totalChunks.copy()
        self._activeClientCount = 0
        self._activeClientMtx = threading.Lock()


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
                    # &&& threading.start_new_thread(self._servClient, (clientName, conn, addr))
                    print("starting thread", clientName, conn, addr)
                    thrd = threading.Thread(target=self._servToClient, args=(clientName, conn, addr))
                    self._clientThreads.append(thrd)
                    self._activeClientMtx.acquire()
                    self._activeClientCount += 1
                    self._activeClientMtx.release()
                    thrd.start()
        print("Accept loop shutting down")
        for j, thrd in enumerate(self._clientThreads):
            print("joining thread", j)
            thrd.join()
        print("All threads joined.")
        
    def _servToClient(self, name, conn, addr):
        # &&& Shouldn't this whole thing be ready to catch an exception from broken conn??? &&&
        try:
            print('Connected by', addr, name, conn)
            serv = DataGenConnection(conn)
            self._clientLock.acquire()
            self._clients[name] = addr
            self._clientLock.release()
            # receive init from client
            serv.servReqInit()
            # server sending back configuration information
            serv.servRespInit(name, self._cfgFakeArgs, self._fakeCfgData)
            # client requesting chunk list
            outOfChunks = False
            while self._loop and not outOfChunks:
                clientReqChunkCount = serv.servRecvReqChunks()
                chunksForClient = list()
                # get the first clientReqChunkCount elements of self._chunksToSendSet
                self._listLock.acquire()
                for chunk in itertools.islice(self._chunksToSendSet, clientReqChunkCount):
                    print("&&& sending chunk", chunk)
                    chunksForClient.append(chunk)
                    cInfo = self._chunksToSend[chunk]
                    cInfo.genStage = GenerationStage.ASSIGNED
                    cInfo.clientId = name
                for chunk in chunksForClient:   
                    self._chunksToSendSet.discard(chunk)
                self._listLock.release()
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
                    # &&&HERE Mark completed chunks as finished
                    self._listLock.acquire()
                    for completed in completed_chunks:
                        self._totalGeneratedChunks.add(completed)
                        cInfo = self._chunksToSend[completed]
                        cInfo.genStage = GenerationStage.FINISHED
                    self._listLock.release()
                    diff = serv.compareChunkLists(completed_chunks, chunksForClient)
                    if len(diff) > 0:
                        # Mark missing chunks as being in limbo.
                        self._listLock.acquire()
                        for missing in diff:
                            cInfo = self._chunksToSend[missing]
                            cInfo.genStage = GenerationStage.LIMBO
                        self._listLock.release()
        except socket.gaierror as e:
            print("breaking connection", addr, name, "socket.gaierror:", e)
        except socket.error as e:
            print("breaking connection", addr, name, "socket.error:", e)
        except DataGenError as e:
            print("breaking connection", addr, name, "DataGenError:", e.msg)

        print("_servToClient loop is done", addr, name)
        # Decrement the number of running client connections and 
        # possibly end the program.
        self._activeClientMtx.acquire()
        self._activeClientCount -= 1
        if self._activeClientCount == 0 and outOfChunks:
            # Connect to our own socket to get past the accept 
            # and break the loop.
            self._loop = False
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as termSock:
                termSock.connect(('127.0.0.1', self._port))
        self._activeClientMtx.release()
        
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
        print("&&& print in limbo and unassigned")
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
    dgServ = DataGenServer("serverCfg.yml", 7, 70)
    dgServ.start()

if __name__ == "__main__":
    testA()