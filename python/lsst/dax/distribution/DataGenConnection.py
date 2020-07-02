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


class DataGenError(Exception):
    def __init__(self, msg):
        super().__init__()
        self.msg = msg

class DataGenConnection():
    """Used for sending an receiving DataGenController messgaes.
    Then generator is the client and the DataGenController is the server.
    The client starts by getting the chunking information/configuration from
    the server and then asks for lists of chunks to generate. When the server
    replies with an empty list of chunks, the generator shuts down the
    connection.
    """

    # Message ids, must all have the same length in bytes.
    # and are followed by a 5 character integer (left padded with 0's)
    # containing the rest of the length of the message
    MSG_LENSTR_LEN = 5
    MAX_MSG_LEN = 90000
    C_INIT_R = 'C_INIT_R' # client initial request
    S_INIT_R = 'S_INIT_R' # server initial response
    C_PCFG_R = 'C_PCFG_R' # client asking for partioner config file
    S_PCFG_A = 'S_PCFG_A' # server answering with specified config file
    C_CHUNKR = 'C_CHUNKR' # client request for chunks to generate
    S_CNKLST = 'S_CNKLST' # server sending chunks to generate
    C_CKCOMP = 'C_CKCOMP' # client sending list of chunks completed
    C_CKCFIN = 'C_CKCFIN' # marks the end of the list

    def __init__(self, connection):
        self.conn = connection
        self.maxRecv = 5000        # Max number of bytes to receive at one time
        self.maxChunksInMsg = 1000 # Limit the number of chunks in a msg
        self.SEP = ':'             # Used to separate values in strings
        self.COMPLEXSEP = '~COMPLEX~' # Used to separate complex strings
        self.warnings = 0          # Sum of warnings generated by the class

    def _send_msg(self, msgId, msg):
        lenStr = str(len(msg)).zfill(self.MSG_LENSTR_LEN)
        if len(lenStr) > self.MSG_LENSTR_LEN:
            self.warnings += 1
            raise DataGenError("_send_msg msg length too long", msgId, lenStr)
        complete_msg = msgId + lenStr + msg
        print('_send_msg~', complete_msg, '~')
        print('conn', self.conn)
        self.conn.sendall(complete_msg.encode())

    def _recv_msg(self):
        msg_id = self._recv_msg_helper(len(self.C_INIT_R))
        msg_lenstr = self._recv_msg_helper(self.MSG_LENSTR_LEN)
        msg_len = int(msg_lenstr)
        print('_recv_msg', msg_id, msg_lenstr)
        msg = self._recv_msg_helper(msg_len)
        print('_recv_msg msg', msg)
        return msg_id, msg, msg_len

    def _recv_msg_helper(self, respLen):
        parts = []
        bytesRecv = 0
        while bytesRecv < respLen:
            partEncoded = self.conn.recv(min(respLen - bytesRecv, self.maxRecv))
            part = partEncoded.decode()
            if part == '':
                self.warnings += 1
                raise DataGenError("socket connection broken")
            parts.append(part)
            bytesRecv += len(part)
        return ''.join(parts)

    def _buildChunksMsg(self, chunk_list):
        """This function takes the chunks numbers in chunk_list and
        builds a text msg from them placing self.SEP between the chunk numbers.
        It returns the chunk_msg and a list of chunks that were used in making
        the message.
        """
        print("buildChunksMsg", chunk_list)
        chunk_msg = ''
        used_chunks = []
        first = True
        for chunk in chunk_list:
            sep = self.SEP
            if first:
                first = False
                sep = ''
            chunkStr = sep + str(chunk)
            newLength = len(chunk_msg) + len(chunkStr)
            if newLength > self.MAX_MSG_LEN or len(used_chunks) >= self.maxChunksInMsg:
                break
            chunk_msg += chunkStr
            used_chunks.append(chunk)
        return chunk_msg, used_chunks

    def _extractChunksFromMsg(self, msg):
        """This function turns the text string message generated by
        _buildChunksMsg back into a list of integer chunk ids.
        It returns a list of chunk ids and if there was a problem.
        """
        problem = False
        msg_split = msg.split(self.SEP)
        if len(msg_split) == 1 and len(msg_split[0]) == 0:
            print("nothing in msg")
            return list(), problem
        print("extract msg", msg, "msg_split", msg_split)
        # convert entire list back to int
        msg_ints = [ i for i in msg_split if len(i) > 0 and i.isnumeric() ]
        if not len(msg_ints) == len(msg_split):
            self.warnings += 1
            problem = True
            print("WARN there were non integer elements in chunk msg ", msg_split, "~~~", msg_ints)
        msg_ints = list(map(int, msg_ints))
        return msg_ints, problem

    def compareChunkLists(self, listA, listB):
        """Chunk lists should be sets, so duplicate chunks are ignored.
        Return a list of chunks that are in one set but not the other.
        """
        setA = set(listA)
        setB = set(listB)
        setDiff = setA ^ setB
        return setDiff

    def clientReqInit(self):
        """Connect to the server, request initialization information."""
        self._send_msg(self.C_INIT_R, '')
    def servReqInit(self):
        """Receive the initialization request from the client"""
        msg_id, msg, msg_len = self._recv_msg()
        print('servReqInit', msg_id, msg)
        if not msg_id == self.C_INIT_R:
            self.warnings += 1
            raise DataGenError('ERROR servRecvInit ' + str(msg_id) + ' ' + msg + ' ' +str(msg_len))
        return msg_id, msg
    def servRespInit(self, name, arg_string, cfg_file_contents):
        """Respond to the client initialization request by sending it
        a name, visits value, objects value, and the contents of the
        datagenerator configuration file.
        """
        sep = self.COMPLEXSEP
        msg = name + sep + arg_string + sep + cfg_file_contents
        self._send_msg(self.S_INIT_R, msg)
    def clientRespInit(self):
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.S_INIT_R:
            self.warnings += 1
            raise DataGenError('ERROR clientRespInit ' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        sep = self.COMPLEXSEP
        splt_msg = msg.split(sep)
        name = splt_msg[0]
        arg_string = splt_msg[1]
        cfg_file_contents = splt_msg[2]
        return name, arg_string, cfg_file_contents

    def clientReqPartitionCfgFile(self, index):
        """Request a partitioner configuration file"""
        print("clientReqChunks C_PCFGR")
        self._send_msg(self.C_PCFG_R, str(index))
    def servRespPartitionCfgFile(self):
        print("servRespPartitionCfgFile C_PCFGR")
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.C_PCFG_R:
            self.warnings += 1
            raise DataGenError('ERROR servRespPartitionCfgFile' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        return int(msg)
    def servSendPartionCfgFile(self, index, fileName, fileContents):
        """Send the fileName and contents to the requestor. If there are
        no more Files to send, fileName is empty.
        """
        print("servSendPartionCfgFile S_PCFG_A", index, fileName, len(fileContents))
        sep = self.COMPLEXSEP
        msg = str(index) + sep + fileName + sep + fileContents
        self._send_msg(self.S_PCFG_A, msg)
    def clientRespPartionCfgFile(self):
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.S_PCFG_A:
            self.warnings += 1
            raise DataGenError('ERROR clientRespPartionCfgFile ' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        sep = self.COMPLEXSEP
        splt_msg = msg.split(sep)
        index = splt_msg[0]
        fileName = splt_msg[1]
        fileContents = splt_msg[2]
        fileName = fileName.strip()
        iIndex = int(index)
        return iIndex, fileName, fileContents

    def clientReqChunks(self, max_count):
        print("clientReqChunks C_CHUNKR")
        msg = str(max_count)
        self._send_msg(self.C_CHUNKR, msg)
    def servRecvReqChunks(self):
        print("servRecvReqChunks C_CHUNKR")
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.C_CHUNKR:
            self.warnings += 1
            raise DataGenError('ERROR servRecvReqChunks' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        return int(msg)

    def servSendChunks(self, chunk_list):
        """Send a list of chunks to the client so they can be generated."""
        print("servSendChunks S_CNKLST", chunk_list)
        chunk_msg, sent_chunks = self._buildChunksMsg(chunk_list)
        self._send_msg(self.S_CNKLST, chunk_msg)
        return sent_chunks
    def clientRecvChunks(self):
        print("clientRecvChunks S_CNKLST")
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.S_CNKLST:
            self.warnings += 1
            raise DataGenError('ERROR clientRecvChunks' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        msg_chunks, problem = self._extractChunksFromMsg(msg)
        if problem:
            self.warnings += 1
            print("WARN clientRecvChunks problem with", msg, msg_chunks)
        return msg_chunks, problem

    def clientReportChunksComplete(self, chunk_list):
        """Send a list of compled chunks to the server and
        return a list of chunks that could not fit in the message.
        If there were no left over chunks, finish by sending a
        C_CKCFIN message.
        """
        print("clientReportChunksComplete C_CKCOMP", chunk_list)
        chunk_msg, completed_chunks = self._buildChunksMsg(chunk_list)
        self._send_msg(self.C_CKCOMP, chunk_msg)
        leftover = []
        if len(completed_chunks) != len(chunk_list):
            leftover = set(chunk_list) - set(completed_chunks)
        if len(leftover) == 0:
            self._send_msg(self.C_CKCFIN, '')
        return leftover
    def servRecvChunksComplete(self):
        """Receive a list of finished chunks from a client. The
        list ends with a C_CKCFIN message.
        Returns list of chunks, message finished, problem
        """
        print("servRecvChunksComplete C_CKCOMP")
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.C_CKCOMP:
            if msg_id == self.C_CKCFIN:
                # All chunks were sent
                return [], True, False
            else:
                self.warnings += 1
                raise DataGenError('ERROR servRecvChunksComplete ' +
                                   str(msg_id) + ' ' + str(msg_len) + '~' + msg + '~')
        msg_chunks, problem = self._extractChunksFromMsg(msg)
        if problem:
            self.warnings += 1
            print("servRecvChunksComplete problem with", msg, msg_chunks)
        return msg_chunks, False, problem

    def testMethods(self):
        success = None
        # Compare effectively identical chunk lists
        a = [0, 56, 23, 1000]
        b = [1000, 56, 23, 0]
        diff = self.compareChunkLists(a, b)
        if len(diff) != 0:
            print("compare identical lists failed", diff)
            success = False
        # test a has an extra element
        val = 345
        a.append(val)
        diff = self.compareChunkLists(a, b)
        if len(diff) == 1:
            if val not in diff:
                print("expected element missing, failed", val, diff)
                success = False
        else:
            print("compare a has more elements len failed", val, diff)
            success = False
        # test b has an extra element
        x = 73
        b.append(x)
        diff = self.compareChunkLists(a, b)
        if len(diff) == 2:
            if x not in diff or val not in diff:
                print("compare b has more elements val failed", diff)
                success = False
        else:
            print("compare b has more elements len failed", diff)
            success = False
        # Test a reasonable length range for ChunkMsg methods
        b = range(1,110)
        testMsg, usedChunks = self._buildChunksMsg(b)
        c, problem = self._extractChunksFromMsg(testMsg)
        diff = self.compareChunkLists(usedChunks, c)
        if len(diff) != 0 or problem or len(b) != len(usedChunks):
            success = False
            print("compareChunkLists failed", problem, b, usedChunks, c)
        # Test a very large range for ChunkMsgMethods
        allChunks = range(1,60000)
        b = allChunks
        sentChunks = []
        while len(b) > 0:
            testMsg, usedChunks = self._buildChunksMsg(b)
            c, problem = self._extractChunksFromMsg(testMsg)
            diff = self.compareChunkLists(usedChunks, c)
            if len(diff) != 0 or problem:
                success = False
                print("compareChunkLists failed", problem, b, usedChunks, c)
            sentChunks.extend(usedChunks)
            b = list(set(b) - set(usedChunks))
        diff = self.compareChunkLists(allChunks, sentChunks)
        if len(diff) != 0:
            print("large range chunkMsg failed", diff)
            success = False
        if success is None: success = True
        print("testMethods success=", success)
        return success

#####################################################
### Following only for testing
import threading # only used for testing
import time

class ServerTestThrd(threading.Thread):

    def __init__(self, host, port, name, arg_string, cfg_file_contents,
                 maxCount, chunkListA, pCfgFiles):
        super().__init__()
        self.success = None
        self.warnings = 0
        self.host = host
        self.port = port
        self.name = name
        self.arg_string = arg_string
        self.cfg_file_contents = cfg_file_contents
        self.maxCount = maxCount
        self.chunkListA = chunkListA
        self.pCfgFiles = pCfgFiles

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            conn, addr = s.accept()
            print('Connected by', addr)
            serv = DataGenConnection(conn)
            if not serv.testMethods():
                self.success = False
                raise RuntimeError("testMethodsFailed")
            # receive init from client
            serv.servReqInit()
            # server sending back configuration information for datagenerator
            serv.servRespInit(self.name, self.arg_string, self.cfg_file_contents)
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
        if self.success == None: self.success = True

class ClientTestThrd(threading.Thread):

    def __init__(self, host, port, name, arg_string, cfg_file_contents,
                 maxCount, chunkListA, pCfgFiles):
        super().__init__()
        self.success = None
        self.warnings = 0
        self.host = host
        self.port = port
        self.name = name
        self.arg_string = arg_string
        self.cfg_file_contents = cfg_file_contents
        self.maxCount = maxCount
        self.chunkListA = chunkListA
        self.pCfgFiles = pCfgFiles

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            client = DataGenConnection(s)
            client.clientReqInit()
            name, arg_string, cfg_file_contents = client.clientRespInit()
            if (name == self.name and arg_string == self.arg_string
                and cfg_file_contents == self.cfg_file_contents):
                pass
            else:
                self.success = False
                raise RuntimeError("Client test failed", name, arg_string, cfg_file_contents)
            # Request partioner configuration files
            pCfgIndex = 0
            pCfgDict = dict()
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
                print("errors in chunk lists diff~", chunkADiff)
                self.success = False
                raise RuntimeError("Client test chunks failed diff~", chunkADiff)
            else:
                print("chunks read success")
            self.warnings += client.warnings
            # Send the list of completed chunks back
            completedChunks = chunkListARecv.copy()
            while len(completedChunks) > 0:
                completedChunks = client.clientReportChunksComplete(completedChunks)


        print("ClientTestThrd.run finished")
        if self.success == None: self.success = True


def testDataGenConnection(port, name, arg_string, cfg_file_contents, maxCount, chunkListA, pCfgFiles):
    """Short test to check that inputs to one side match outputs on the other"""
    host = "127.0.0.1"
    servThrd = ServerTestThrd(host, port, name, arg_string, cfg_file_contents,
                              maxCount, chunkListA, pCfgFiles)
    clientThrd = ClientTestThrd(host, port, name, arg_string, cfg_file_contents,
                                maxCount, chunkListA, pCfgFiles)
    servThrd.start()
    time.sleep(1)
    clientThrd.start()

    servThrd.join()
    clientThrd.join()
    if servThrd.success and clientThrd.success:
        print("SUCCESS")
    else:
        print("FAILED")

    if servThrd.warnings > 0 or clientThrd.warnings > 0 :
        print("There were warnings")
        print("  serv", servThrd.warnings)
        print("  client", clientThrd.warnings)

if __name__ == "__main__":
    cListA = range(26,235)
    pCfgFiles = {0:("obj.cfg", "a lot of obj cfg info"),
                 1:("fs.cfg", "some forcedSource info"),
                 2:("junk_cfg", "blah blah junk\n more stuff")}
    testDataGenConnection(14242, 'qt', '--visits 30 --objects 10000',
                          'bunch of json file entries', 28, cListA,
                          pCfgFiles)


