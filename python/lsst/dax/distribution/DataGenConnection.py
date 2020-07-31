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
    replies with an empty list of chunks, the client shuts down the
    connection. The server ends when there are no more chunks to send
    and all connection threads have joined.
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
        self.COMPLEXSEP = '~COMP&@&~' # Used to separate complex strings
        self.warnings = 0          # Sum of warnings generated by the class

    def _send_msg(self, msg_id, msg):
        """Send a message across the connection.

        Parameters
        ----------
        msg_id : str
            Id string for the message.
        msg : str
            The message to send.
        """
        lenStr = str(len(msg)).zfill(self.MSG_LENSTR_LEN)
        if len(lenStr) > self.MSG_LENSTR_LEN:
            self.warnings += 1
            raise DataGenError("_send_msg msg length too long " + msg_id + " " + lenStr)
        complete_msg = msg_id + lenStr + msg
        print('_send_msg~', complete_msg, '~')
        print('conn', self.conn)
        self.conn.sendall(complete_msg.encode())

    def _recv_msg(self):
        """Receive a message sent with _send_msg.

        Return
        ------
        msg_id : str
            Id string for the message.
        msg : str
            The message received.
        msg_len : int
            The length of the message.
        """
        msg_id = self._recv_msg_helper(len(self.C_INIT_R))
        msg_lenstr = self._recv_msg_helper(self.MSG_LENSTR_LEN)
        msg_len = int(msg_lenstr)
        print('_recv_msg', msg_id, msg_lenstr)
        msg = self._recv_msg_helper(msg_len)
        print('_recv_msg msg', msg)
        return msg_id, msg, msg_len

    def _recv_msg_helper(self, resp_len):
        """A function to assist in rebuilding long messages.

        Parameters
        ----------
        resp_len : int
            The length of the message being received.

        Return
        ------
        resp : str
            The entire response message.
        """
        parts = []
        bytesRecv = 0
        while bytesRecv < resp_len:
            partEncoded = self.conn.recv(min(resp_len - bytesRecv, self.maxRecv))
            part = partEncoded.decode()
            if part == '':
                self.warnings += 1
                raise DataGenError("socket connection broken")
            parts.append(part)
            bytesRecv += len(part)
        return ''.join(parts)

    def _buildChunksMsg(self, chunk_list):
        """Build the chunk message from chunk_list

        Parameters
        ----------
        chunk_list : list of int
            List of chunk ids to put in the message

        Return
        ------
        chunk_msg : str
            Message containing the chunk numbers.
        used_chunks : list of int
            List of chunks actually used  in the message.

        Note
        ----
        This function takes the chunks numbers in chunk_list and
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

        Parameters
        ----------
        msg : str
            A message string generated by _buildChunksMsg.

        Return
        ------
        msg_ints : list of ints
            A list of integer chunk ids taken from the message.
        problem : bool
            True if there were problems converting str to int.
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

    # These functions are defined in the order in which they should be called
    # by client and server.
    def clientReqInit(self):
        """Connect to the server, request initialization information.
        """
        self._send_msg(self.C_INIT_R, '')
    def servReqInit(self):
        """Receive the initialization request from the client.
        """
        msg_id, msg, msg_len = self._recv_msg()
        print('servReqInit', msg_id, msg)
        if not msg_id == self.C_INIT_R:
            self.warnings += 1
            raise DataGenError('ERROR servRecvInit ' + str(msg_id) + ' ' + msg + ' ' +str(msg_len))
        return msg_id, msg
    def servRespInit(self, name, arg_string, cfg_file_contents, ingest_dict):
        """Respond to the client initialization request.

        Parameters
        ----------
        name : str
            name of the client
        arg_string : str
            arguments for datagen.py
        cfg_file_contents : str
            contents of the configguration file
        ingest_dict : dictionary
            Dictionary containing information about the ingest system.
            'host' : str, ingest system host name.
            'port' : int, ingest port number.
            'user' : str, ingest user name.
            'auth' : str, ingest user name.
            'db'   : str, name of the databse being created
            'skip  : bool, True if ingest is being skipped.

        Note
        ----
        Parameters for servRespInit are the return values for clientRespInit.
        """
        sep = self.COMPLEXSEP
        print("ingest_dict=", ingest_dict)
        skip_val = '0'
        if ingest_dict['skip']:
            skip_val = '1'
        msg = (name + sep + arg_string + sep + cfg_file_contents
            + sep + ingest_dict['host'] + sep + str(ingest_dict['port'])
            + sep + ingest_dict['user'] + sep + ingest_dict['auth']
            + sep + ingest_dict['db'] + sep + skip_val)
        self._send_msg(self.S_INIT_R, msg)
    def clientRespInit(self):
        """Unwrap the configuration information sent by the server.

        Return
        ------
        name : str
            Name for the client.
        arg_string : str
            Argument string for dataGen.py.
        cfg_file_contents : str
            Contents of the configuration file for dataGen.py.
        ingest_dict : dictionary
            Dictionary with ingest system configuration information
            where the keys must match those in servRespInit.
        """
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.S_INIT_R:
            self.warnings += 1
            raise DataGenError('ERROR clientRespInit ' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        sep = self.COMPLEXSEP
        splt_msg = msg.split(sep)
        name = splt_msg[0]
        arg_string = splt_msg[1]
        cfg_file_contents = splt_msg[2]
        ingest_dict = { 'host':splt_msg[3], 'port':int(splt_msg[4]), 'user':splt_msg[5], 'auth':splt_msg[6],
                        'db':splt_msg[7] }
        skip_val = splt_msg[8]
        ingest_dict['skip'] = bool(skip_val != '0')
        return name, arg_string, cfg_file_contents, ingest_dict

    def clientReqPartitionCfgFile(self, index):
        """Client request a partitioner configuration file from the server

        Parameters
        ----------
        index : int
            Indicates which partitioner configuration file the client wants.
            The client starts at 0 and keeps incrmenting by 1 until the server
            indicates the file does not exist. The number of files varies
            depending on the database.
        """
        print("clientReqChunks C_PCFGR")
        self._send_msg(self.C_PCFG_R, str(index))
    def servRespPartitionCfgFile(self):
        """Serv recieve the partition configuration index from the client.

        Return
        ------
        index : int
            Indicates which partitioner configuration file the client wants.
        """
        print("servRespPartitionCfgFile C_PCFGR")
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.C_PCFG_R:
            self.warnings += 1
            raise DataGenError('ERROR servRespPartitionCfgFile' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        return int(msg)
    def servSendPartionCfgFile(self, index, file_name, file_contents):
        """Send the file_name and contents to the requestor. If there are
        no more Files to send, file_name is empty.

        Parameters
        ----------
        index : int
            The index of the requested partitioner configuration file.
        file_name : str
            The name of the file at the index. This should be an empty
            string if there is no file at that index.
        file_contents : str
            The contents of the configuration file. This may be empty.
        """
        print("servSendPartionCfgFile S_PCFG_A", index, file_name, len(file_contents))
        sep = self.COMPLEXSEP
        msg = str(index) + sep + file_name + sep + file_contents
        self._send_msg(self.S_PCFG_A, msg)
    def clientRespPartionCfgFile(self):
        """Extract file name and contents from the message sent by the server.

        Return
        ------
        i_index : int
            Index number for the file and contents.
        file_name, file_contents : str
            Name of the file at that index and its contents.
        """
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.S_PCFG_A:
            self.warnings += 1
            raise DataGenError('ERROR clientRespPartionCfgFile ' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        sep = self.COMPLEXSEP
        splt_msg = msg.split(sep)
        index = splt_msg[0]
        file_name = splt_msg[1]
        file_contents = splt_msg[2]
        file_name = file_name.strip()
        i_index = int(index)
        return i_index, file_name, file_contents

    def clientReqChunks(self, max_count):
        """Client requesting chunks ids from the server.

        Parameters
        ----------
        max_count : int
            maximum number of chunk ids to be sent in one message.
        """
        print("clientReqChunks C_CHUNKR")
        msg = str(max_count)
        self._send_msg(self.C_CHUNKR, msg)
    def servRecvReqChunks(self):
        """Server getting the number of chunks requested in clientReqChunks.

        Return
        ------
        max_count : int
            Maximum number of chunk ids to send the client.
        """
        print("servRecvReqChunks C_CHUNKR")
        msg_id, msg, msg_len = self._recv_msg()
        if not msg_id == self.C_CHUNKR:
            self.warnings += 1
            raise DataGenError('ERROR servRecvReqChunks' + str(msg_id) + ' ' + str(msg_len) + ' ' + msg)
        return int(msg)

    def servSendChunks(self, chunk_list):
        """Send a list of chunks to the client so they can be generated.

        Parameters
        ----------
        chunk_list : list of int
            List of chunk ids for the client to generate.

        Return
        ------
        sent_chunks : list of int
            List of chunk ids that were actually sent to the client.
        """
        print("servSendChunks S_CNKLST", chunk_list)
        chunk_msg, sent_chunks = self._buildChunksMsg(chunk_list)
        self._send_msg(self.S_CNKLST, chunk_msg)
        return sent_chunks
    def clientRecvChunks(self):
        """Receive the list of chunks sent by the server.

        Return
        ------
        msg_chunks : list of int
            List of chunk ids to generate from the server.
        problem : bool
            True if there problems converting str to int.
        """
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

        Parameters
        ----------
        chunk_list : list of int
            List of completed chunks to send back to the server

        Return
        ------
        leftover : list of int
            List of chunks that did not fit in the message to the server.
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

        Return
        ------
        msg_chunks : list of int
            List of completed chunk ids from the client. The client may send
            more later.
        message_finished : bool
            True when the client is done sending completed chunk ids.
        problem : bool
            True if there were issues with conversions.
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
    """Class for testing the server side of messaging
    """

    def __init__(self, host, port, name, arg_string, cfg_file_contents,
                 maxCount, chunkListA, pCfgFiles, ingest_dict):
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
        self.ingest_dict = ingest_dict

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
            serv.servRespInit(self.name, self.arg_string, self.cfg_file_contents, self.ingest_dict)
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
    """Class for testing the client side of messaging
    """

    def __init__(self, host, port, name, arg_string, cfg_file_contents,
                 maxCount, chunkListA, pCfgFiles, ingest_dict):
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
        self.ingest_dict = ingest_dict

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            client = DataGenConnection(s)
            client.clientReqInit()
            name, arg_string, cfg_file_contents, ingest_dict = client.clientRespInit()
            print("ingest_dict=", ingest_dict)
            if (name == self.name and arg_string == self.arg_string
                and cfg_file_contents == self.cfg_file_contents
                and ingest_dict == self.ingest_dict):
                pass
            else:
                self.success = False
                raise RuntimeError("Client test failed", name, arg_string, cfg_file_contents)
            # Request partioner configuration files from server
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


def testDataGenConnection(port, name, arg_string, cfg_file_contents, maxCount,
                          chunkListA, pCfgFiles, ingest_dict):
    """Short test to check that inputs to one side match outputs on the other"""
    host = "127.0.0.1"
    servThrd = ServerTestThrd(host, port, name, arg_string, cfg_file_contents,
                              maxCount, chunkListA, pCfgFiles, ingest_dict)
    clientThrd = ClientTestThrd(host, port, name, arg_string, cfg_file_contents,
                                maxCount, chunkListA, pCfgFiles, ingest_dict)
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

if __name__ == "__main__":
    cListA = range(26,235)
    pCfgFiles = {0:("obj.cfg", "a lot of obj cfg info"),
                 1:("fs.cfg", "some forcedSource info"),
                 2:("junk_cfg", "blah blah junk\n more stuff")}
    ingest_dict = {'host':'mt.st.com', 'port':2461, 'user': 'person', 'auth': '1234',
                            'db':'afake_db', 'skip': False}
    success, s_warn1, c_warn1 = testDataGenConnection(14242, 'qt', '--visits 30 --objects 10000',
                          'bunch of json file entries', 28, cListA,
                          pCfgFiles, ingest_dict)
    if not success:
        print("First test failed")
        exit(1)

    ingest_dict = {'host':'mt.st.edu', 'port':0, 'user': '', 'auth': '',
                   'db':'diff_db', 'skip': True}
    success, s_warn2, c_warn2 = testDataGenConnection(14242, 'qt', '--visits 30 --objects 10000',
                          'bunch of json file entries', 28, cListA,
                          pCfgFiles, ingest_dict)

    print("success=", success, "serv_warn=", s_warn1, s_warn2, "client_warn=", c_warn1, c_warn2)



