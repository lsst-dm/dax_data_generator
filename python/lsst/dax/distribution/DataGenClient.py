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

import glob
import os
import socket
import subprocess

from DataGenConnection import DataGenConnection





class DataGenClient:
    """This class is used to connect to the DataGenServer and
    use the information it provides to generate appropriate
    fake chunks while reporting what chunks have been created.
    """

    def __init__(self, host, port, targetDir='fakeData'):
        self._loop = True # set to false to end the program
        self._host = host # server host
        self._port = port # server port
        self._client = None # DataGenConnection
        self._genArgStr = None # Arguments from the server for the generator.
        self._chunksPerReq = 5 # Chunks numbers wanted per request to the server.
        self._cfgFileName = 'gencfg.py' # name of the local configuration file for the generator
        self._cfgFileContents = None # contents of file to create
        self._datagenpy = '~/work/dax_data_generator/bin/datagen.py' # &&& this has to go
        #self._targetDir = 'fakedatatmp' &&&
        self._targetDir = targetDir
        self.makeDir(self._targetDir)

    def makeDir(self, dirName):
        if not os.path.exists(dirName):
            os.mkdir(dirName)
            print("&&& Directory " , dirName ,  " Created ")
        else:
            print("&&& Directory " , dirName ,  " already exists")

    def runProcess(self, cmd):
        print("&&& cmd", cmd)
        cwd = self._targetDir
        print("&&& cwd", cwd)
        process = subprocess.Popen(cmd, cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        outStr = process.communicate()
        print("&&& outStr", outStr)
        process.wait()
        print("&&& process result", process.returncode)
        return process.returncode, outStr

    def convertPg2Csv(self, chunkId):
        """Convert parquet files to csv for chunkId. The original parquet files are deleted.
        Return True if all files are converted successfully and a list of generated csv files.
        """
        success = None
        # Fint the relevant files in self._targetDir.
        findParquet = 'chunk' + str(chunkId) + '_*.parquet'
        findParquet = os.path.join(self._targetDir, findParquet)
        print("&&& cwd=", os.getcwd())
        #TODO: python3 has better version of glob that would make this code cleaner.
        chunkParquetPaths = glob.glob(findParquet)
        #remove the path
        chunkParquetFiles = list()
        for fn in chunkParquetPaths:
            chunkParquetFiles.append(os.path.basename(fn))
        print("&&& findParquet=", findParquet, " chunkParquetFiles=", chunkParquetFiles)

        outFileNames = list()
        if len(chunkParquetFiles) == 0:
            print("No parquet files found for", chunkId)
            return False, outFileNames
        for fName in chunkParquetFiles:
            outName = os.path.splitext(fName)[0] + ".csv"
            cmdStr = 'pq2csv --replace ' + fName + ' ' + outName
            print("&&& cwd=", os.getcwd())
            print("&&& running ps2csv:", cmdStr)
            genResult, genOut = self.runProcess(cmdStr)
            if genResult != 0:
                print("Failed to convert file", fName, genResult, genOut)
                success = False
            print("&&& cmdStr=", cmdStr, 'genResult=', genResult, 'genOut=', genOut)
            outFileNames.append(outName)
            #os.remove(os.path.join(self._targetDir, fName))
        print("&&& outFileNames=", outFileNames)
        if success == None:
            success = True
        return success, outFileNames


    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self._host, self._port))
            self._client = DataGenConnection(s)
            self._client.clientReqInit()
            self._name, self._genArgStr, self._cfgFileContents = self._client.clientRespInit()
            print("&&& name=", self._name, self._genArgStr, ":\n", self._cfgFileContents)
            # write the configuration file
            fileName = os.path.join(self._targetDir, self._cfgFileName)
            print("&&& write the configuration file &&&", fileName)
            with open(fileName, "w") as fw:
                fw.write(self._cfgFileContents)
            while self._loop:
                self._client.clientReqChunks(self._chunksPerReq)
                chunkListRecv, problem = self._client.clientRecvChunks()
                if problem:
                    print("&&& WARN there was a problem with", chunkListRecv)
                chunkRecvSet = set(chunkListRecv)
                if len(chunkRecvSet) == 0:
                    # no more chunks, close the connection
                    print("No more chunks to create, exiting")
                    self._loop = False
                else:
                    # &&& print the command lines
                    createdChunks = list()
                    for chunk in chunkRecvSet:
                        cmdStr = ("python " + self._datagenpy + " --chunk " + str(chunk) + " " +
                            self._genArgStr + " " + self._cfgFileName)
                        print("&&& running this:", cmdStr)
                        genResult, genOut = self.runProcess(cmdStr)
                        if genResult == 0:
                            # Convert parquet files to csv.
                            if not self.convertPg2Csv(chunk):
                                print("conversion to csv failed for", chunk)
                        else:
                            print("generator failed for", chunk)
                        print("&&& genOut", genOut)
                    # If no chunks were created, likely fatal error.
                    if len(createdChunks) == 0:
                        print("ERROR no chunks were successfully created, ending program")
                        self._loop = False
                    # Client sends the list of completed chunks back
                    while True:
                        # Keep sending created chunks back until there are none left.
                        # If createdChunks was empty initially, the server needs to be sent an
                        # empty list to indicate complete failure.
                        createdChunks = self._client.clientReportChunksComplete(createdChunks)
                        if len(createdChunks) == 0:
                            break

def testC():
    dgClient = DataGenClient("127.0.0.1", 13042)
    dgClient.run()


if __name__ == "__main__":
    testC()
