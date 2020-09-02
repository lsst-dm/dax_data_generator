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

import socket
import threading
import time

from . import DataGenConnection
from . import DataIngest
from lsst.dax.data_generator import TimingDict


class ServerTestThrd(threading.Thread):
    """Class for testing the server side of messaging
    """

    def __init__(self, host, port, name, objects, visits, seed, cfg_file_contents,
                 maxCount, chunkListA, pCfgFiles, ingest_dict, timing_dict):
        super().__init__()
        self.success = None
        self.warnings = 0
        self.host = host
        self.port = port
        self.name = name
        self.objects = objects
        self.visits = visits
        self.seed = seed
        self.cfg_file_contents = cfg_file_contents
        self.maxCount = maxCount
        self.chunkListA = chunkListA
        self.pCfgFiles = pCfgFiles
        self.ingest_dict = ingest_dict
        self.timing_dict = timing_dict

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            conn, addr = s.accept()
            print('Connected by', addr)
            serv = DataGenConnection.DataGenConnection(conn)
            if not serv.testMethods():
                self.success = False
                raise RuntimeError("testMethodsFailed")
            # receive init from client
            serv.servReqInit()
            # server sending back configuration information for datagenerator
            serv.servRespInit(self.name, self.objects, self.visits, self.seed,
                              self.cfg_file_contents, self.ingest_dict)
            # client requests partioner configuration files.
            pCfgDone = False
            while not pCfgDone:
                pCfgIndex = serv.servRespPartitionCfgFile()
                if pCfgIndex in self.pCfgFiles:
                    pCfgTpl = self.pCfgFiles[pCfgIndex]
                    pCfgName = pCfgTpl[0]
                    pCfgContents = pCfgTpl[1]
                else:
                    pCfgName = ""
                    pCfgContents = ""
                    pCfgDone = True
                serv.servSendPartionCfgFile(pCfgIndex, pCfgName, pCfgContents)
            # client requesting chunk list
            maxCount = serv.servRecvReqChunks()
            if not maxCount == self.maxCount:
                self.success = False
                raise RuntimeError("serv test failed", self.name, maxCount)
            serv.servSendChunks(self.chunkListA)
            # Receive timing information from client
            timing_dict = serv.servRecvTiming()
            print("timing_dict", timing_dict)
            if timing_dict != self.timing_dict:
                self.success = False
                raise RuntimeError("serv test failed timing_dict mismatch",
                                   timing_dict, "\n", self.timing_dict)
            # receive completed chunks from client
            completed_chunks = []
            finished = False
            while not finished:
                completedC, finished, problem = serv.servRecvChunksComplete()
                print("serv got", completedC, finished, problem)
                completed_chunks.extend(completedC)
            # compare with original chunk list
            diff = serv.compareChunkLists(completed_chunks, self.chunkListA)
            print("final compare diff=", diff)
            if len(diff) != 0:
                self.success = False
                raise RuntimeError("mismatch in sent vs received lists", self.name, diff)
            self.warnings += serv.warnings
        print("ServerTestThrd.run finished")
        if self.success is None: self.success = True

class ClientTestThrd(threading.Thread):
    """Class for testing the client side of messaging
    """

    def __init__(self, host, port, name, objects, visits, seed, cfg_file_contents,
                 maxCount, chunkListA, pCfgFiles, ingest_dict, timing_dict):
        super().__init__()
        self.success = None
        self.warnings = 0
        self.host = host
        self.port = port
        self.name = name
        self.objects = objects
        self.visits = visits
        self.seed = seed
        self.cfg_file_contents = cfg_file_contents
        self.maxCount = maxCount
        self.chunkListA = chunkListA
        self.pCfgFiles = pCfgFiles
        self.ingest_dict = ingest_dict
        self.timing_dict = timing_dict

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            client = DataGenConnection.DataGenConnection(s)
            client.clientReqInit()
            name, objects, visits, seed, cfg_file_contents, ingest_dict = client.clientRespInit()
            print("ingest_dict=", ingest_dict)
            # Check that the values sent over the connection match what should have been sent.
            if (name == self.name
                and objects == self.objects
                and visits == self.visits
                and seed == self.seed
                and cfg_file_contents == self.cfg_file_contents
                and ingest_dict == self.ingest_dict):
                pass
            else:
                self.success = False
                raise RuntimeError("Client test failed", name, cfg_file_contents)
            # Request partioner configuration files from server
            pCfgIndex = 0
            pCfgDict = {}
            pCfgName = "nothing"
            while not pCfgName == "":
                client.clientReqPartitionCfgFile(pCfgIndex)
                indx, pCfgName, pCfgContents = client.clientRespPartionCfgFile()
                if indx != pCfgIndex:
                    self.success = False
                    raise RuntimeError("Client test failed", indx, pCfgName, pCfgContents)
                if not pCfgName == "":
                    pCfgDict[pCfgIndex] = (pCfgName, pCfgContents)
                pCfgIndex += 1
            if pCfgDict != self.pCfgFiles:
                print("ERROR in partioner configuration files", pCfgDict, self.pCfgFiles)
                self.success = False
            # Request chunks to generate
            client.clientReqChunks(self.maxCount)
            chunkListARecv, problem = client.clientRecvChunks()
            chunkARecvSet = set(chunkListARecv)
            chunkASet = set(self.chunkListA)
            chunkADiff = chunkASet.difference(chunkARecvSet)
            if chunkADiff:
                print("errors in chunk lists diff~", chunkADiff, problem)
                self.success = False
                raise RuntimeError("Client test chunks failed diff~", chunkADiff, problem)
            else:
                print("chunks read success")
            self.warnings += client.warnings
            # send back timing information
            client.clientReportTiming(self.timing_dict)
            # Send the list of completed chunks back
            completedChunks = chunkListARecv.copy()
            while len(completedChunks) > 0:
                completedChunks = client.clientReportChunksComplete(completedChunks)


        print("ClientTestThrd.run finished")
        if self.success is None: self.success = True


def testDataGenConnection(port, name, objects, visits, seed, cfg_file_contents, maxCount,
                          chunkListA, pCfgFiles, ingest_dict, timing_dict):
    """Short test to check that inputs to one side match outputs on the other.
    Both the client thread and server thread are given the same information.
    If transmitted information doesn't match what is expected, there is a
    problem with encoding/decoding.
    """
    host = "127.0.0.1"
    servThrd = ServerTestThrd(host, port, name, objects, visits, seed, cfg_file_contents,
                              maxCount, chunkListA, pCfgFiles, ingest_dict, timing_dict)
    clientThrd = ClientTestThrd(host, port, name, objects, visits, seed, cfg_file_contents,
                                maxCount, chunkListA, pCfgFiles, ingest_dict, timing_dict)
    servThrd.start()
    time.sleep(1)
    clientThrd.start()

    servThrd.join()
    clientThrd.join()
    if servThrd.success and clientThrd.success:
        print("SUCCESS")
        success = True
    else:
        print("FAILED")
        success = False

    if servThrd.warnings > 0 or clientThrd.warnings > 0 :
        print("There were warnings")
        print("  serv", servThrd.warnings)
        print("  client", clientThrd.warnings)
    return success, servThrd.warnings, clientThrd.warnings

def connectionTest():
    cListA = range(26,235)
    pCfgFiles = {0:("obj.cfg", "a lot of obj cfg info"),
                 1:("fs.cfg", "some forcedSource info"),
                 2:("junk_cfg", "blah blah junk\n more stuff")}
    ingest_dict = {'host':'mt.st.com', 'port':2461, 'auth': '1234',
                            'db':'afake_db', 'skip': False}
    timing_dict = TimingDict()
    timing_dict.add('gen_o', 345.23)
    timing_dict.add('gen_fs', 981.23)
    timing_dict.add('conv', 12.9999)
    timing_dict.add('trans', 42.1)
    timing_dict.add('del', 0.123)
    timing_dict.increment()
    success, s_warn1, c_warn1 = testDataGenConnection(14242, 'qt', 10000, 30, 178,
                          'bunch of json file entries', 28, cListA,
                          pCfgFiles, ingest_dict, timing_dict)
    if not success:
        print("First test failed")
        exit(1)

    ingest_dict = {'host':'mt.st.edu', 'port':0, 'auth': '',
                   'db':'diff_db', 'skip': True}
    timing_dict = TimingDict()
    success, s_warn2, c_warn2 = testDataGenConnection(14242, 'qt', 10000, 30, 1,
                          'bunch of json file entries', 28, cListA,
                          pCfgFiles, ingest_dict, timing_dict)

    print("success=", success, "serv_warn=", s_warn1, s_warn2, "client_warn=", c_warn1, c_warn2)


def dataIngestTest():
    ingest = DataIngest.DataIngest('localhost', 25080)
    # No point in continuing if the ingest system can't be contacted.
    if not ingest.isIngestAlive():
        print("ERROR ingest server not responding.")
        exit(1)
    # Try sending the database. This will fail if the database already exists.
    if not ingest.registerDatabase("configs/fakeIngestCfgsTest/test102.json"):
        print("ERROR failed to send database configuration to ingest.")
    # Ingest the test Object tables schema. This will fail if the
    # database already exists.
    if not ingest.registerTable("configs/fakeIngestCfgsTest/test102_Object.json"):
        print("ERROR failed to send Object table schema")
    # Start a transaction
    i_transaction = DataIngest.IngestTransaction(ingest, 'test102')
    transaction_status = None
    try:
        with i_transaction as t_id:
            chunk_id = 0
            # Get address of worker to handle this chunk.
            host, port = ingest.getChunkTargetAddr(t_id, chunk_id)
            # Send the chunk to the target worker
            table = 'Object'
            f_path = 'configs/fakeIngestCfgsTest/chunk_0.txt'
            out_str = ingest.sendChunkToTarget(host, port, t_id, table, f_path)
            print('host=', host, 'port=', port, "", out_str)
            i_transaction.abort = False
            # Transaction ends
        transaction_status = True
    except RuntimeError as err:
        transaction_status = False
        print("Transaction Failed ", i_transaction, "err=", err)
        exit(1)
    # code to publish
    success, status, content = ingest.publishDatabase('test102')
    if not success:
        print("Failed to publish ", transaction_status, status, content)
        exit(1)
    print("Success", transaction_status)
