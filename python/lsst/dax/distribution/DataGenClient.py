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
import re
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
        self._spec = None # spec from exec(self._cfgFileContents)
        self._chunker = None # chunker from exec(self._cfgFileContents)
        self._overlap = 0.018 # overlap in degrees, about 1 arcmin. This should be put
                              # in example_spec as changing it will change the chunk contents.
        self._datagenpy = '~/work/dax_data_generator/bin/datagen.py' # &&& this has to go
        self._targetDir = targetDir
        self.makeDir(self._targetDir)

    def createFileName(self, chunkId, tableName, ext, edgeOnly=False, useTargPath=False):
        typeStr = 'CT_'
        if edgeOnly: typeStr = 'EO_'
        # If the tabelName is a wildcard, don't use typeStr
        if tableName == '*': typeStr = ''
        fn = 'chunk' + str(chunkId) + '_' + typeStr + tableName + '.' + ext
        if useTargPath:
            fn = os.path.join(self._targetDir, fn)
        return fn

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

    def _readDatagenConfig(self):
        """ Create a Chunker using the same configuration file as the datagen.py."""
        spec_globals = dict()
        exec(self._cfgFileContents, spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        self._spec = spec_globals['spec']
        self._chunker = spec_globals['chunker']
        print("&&& self._cfgFileContents=", self._cfgFileContents)
        print("&&& _spec=", self._spec)

    def convertPg2Csv(self, chunkId):
        """Convert parquet files to csv for chunkId. The original parquet files are deleted.
        Return True if all files are converted successfully and a list of generated csv files.
        """
        success = None
        # Find the relevant files in self._targetDir.
        #&&& findParquet = 'chunk' + str(chunkId) + '_*.parquet'
        #&&& findParquet = os.path.join(self._targetDir, findParquet)
        findParquet = self.createFileName(chunkId, '*', 'parquet', useTargPath=True)
        print("&&& cwd=", os.getcwd(), "findParquet=", findParquet)
        #TODO: python3 has better version of glob that would make this code cleaner.
        chunkParquetPaths = glob.glob(findParquet)
        # Remove the path from the file names.
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
            os.remove(os.path.join(self._targetDir, fName))
        print("&&& outFileNames=", outFileNames)
        if success == None:
            success = True
        return success, outFileNames

    def findCsvInTargetDir(self, chunkId, neighborChunks):
        """Return:
            - True, there are the required csv files for chunkId
            - a list of csv files for chunkId and neighborChunks
            - a list chunkId's that need to be generated edgeOnly
        """
        success = True
        foundCsv = list()
        neededEdgeOnly = list()
        # Find the relevant files in self._targetDir.
        #&&& findCsv = 'chunk' + str(chunkId) + '_*.csv'
        #&&& findCsv = os.path.join(self._targetDir, findCsv)
        findCsv = self.createFileName(chunkId, '*', 'csv', useTargPath=True)
        #TODO: python3 has better version of glob that would make this code cleaner.
        chunkCsvPaths = glob.glob(findCsv)
        print("&&& findCsv=", findCsv, " paths=", chunkCsvPaths)
        # Remove the path and add the files to a list
        chunkCsvFiles = list()
        for fn in chunkCsvPaths:
            chunkCsvFiles.append(os.path.basename(fn))
        print("&&& chunkCsvFiles=", chunkCsvFiles)
        # These cannot be edgeOnly, and there must be one for each entry in self._spec['spec']
        for tblName in self._spec:
            #&&& fn = "chunk" + str(chunkId) + "_" + tblName + ".csv"
            fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=False, useTargPath=False)
            print("&&& tblName=", tblName, "fn=", fn)
            if not fn in chunkCsvFiles:
                print("Failed to find ", fn, "in", chunkCsvFiles)
                success = False
                return success, foundCsv, neededEdgeOnly
            print("&&& spec key=", tblName, "found", fn)
            foundCsv.append(fn)
        # remove chunkId from neighborChunks, if it is there
        nbChunks = [val for val in neighborChunks if val != chunkId]
        # See if the neighbor chunk csv files exist. They can be complete or edge only.
        for nbCh in nbChunks:
            allTablesFound = True
            completeFound = False
            eOFound = False
            for tblName in self._spec:
                #&&& fn = "chunk" + str(nbCh) + "_" + tblName + ".csv"
                fn = self.createFileName(nbCh, tblName, 'csv', edgeOnly=False, useTargPath=False)
                #&&&fnEO = "chunk" + str(nbCh) + "_EO_" + tblName + ".csv"
                fnEO = self.createFileName(nbCh, tblName, 'csv', edgeOnly=True, useTargPath=False)
                print("&&& fn=", fn)
                print("&&& fnEO=", fnEO)
                if fn in chunkCsvFiles:
                    foundCsv.append(fn)
                    completeFound = True
                elif fnEO in chunkCsvFiles:
                    foundCsv.append(fnEO)
                    eOFound = True
                else:
                    allTablesFound = False
            if completeFound and eOFound:
                print("&&& both complete and edge only files found for neighbor chunk=", nbCh)
                # That shouldn't have happened.
                success = False
            if not allTablesFound:
                neededEdgeOnly.append(nbCh)
        return success, foundCsv, neededEdgeOnly

    def removeFilesForChunk(self, chunkId, edgeOnly=False, complete=False):
        """Remove expected files for these chunks from the target directory."""
        print("&&& removeFilesForChunk", chunkId, edgeOnly, complete)
        for tblName in self._spec:
            if edgeOnly:
                fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=True, useTargPath=True)
                if os.path.exists(fn):
                    print("&&& removing EO file", fn)
                    os.remove(fn)
                fn = self.createFileName(chunkId, tblName, 'parquet', edgeOnly=True, useTargPath=True)
                if os.path.exists(fn):
                    print("&&& removing EO file", fn)
                    os.remove(fn)
            if complete:
                fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=False, useTargPath=True)
                if os.path.exists(fn):
                    print("&&& removing CO file", fn)
                    os.remove(fn)
                fn = self.createFileName(chunkId, tblName, 'parquet', edgeOnly=False, useTargPath=True)
                if os.path.exists(fn):
                    print("&&& removing CO file", fn)
                    os.remove(fn)

    def _generateChunk(self, chunkId, edgeOnly=False):
        """Generate the csv files for a chunk.
        return 'success' if the chunk was made successfully
               'failed' if a valid version of the chunk could not be made
               'existed' if a valid complete version of the chunk already existed
                         and edgeOnly=True
        edgeOnly set to True will cause an edge onlyChunk to be generated.
            edgeOnly chunks will not be created if there is an existing complete chunk
        edgeOnly set to False will cause a complete chunk to be created and will result
            in edge only files for that chunk to be deleted.
        """
        if edgeOnly:
            # Check for existing csv files. If a full set of complete files are found or
            # a full set of edge only files are found, return 'existed'
            # If there is a full set of complete files, delete the edge only files.
            edgeOnlyCount = 0
            completeCount = 0
            spec = self._spec
            for tblName in spec:
                fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=True, useTargPath=True)
                if os.path.exists(fn):
                    print("&&& found complete fn=", fn)
                    edgeOnlyCount += 1
                fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=False, useTargPath=True)
                if os.path.exists(fn):
                    completeCount += 1
            print("&&& edgeOnlyCount=", edgeOnlyCount, "completeCount=", completeCount)
            if completeCount == len(spec) or edgeOnlyCount == len(spec):
                print("All expected tables already exist, will not generate. chunkid=", chunkId)
                if completeCount == len(spec):
                    print("Removing extraneous edgeOnly files")
                    self.removeFilesForChunk(chunkId, edgeOnly=True, complete=False)
                else: # Not a full set of complete files
                    print("Removing extraneous complete files")
                    self.removeFilesForChunk(chunkId, edgeOnly=False, complete=True)
                return 'exists'
        else:
            # Delete edge only files for this chunk if they exist.
            # The generator will overwrite existing complete files.
            self.removeFilesForChunk( chunkId, edgeOnly=True, complete=False)
        # Genrate the chunk parquet files.
        options = " --edgefirst "
        if edgeOnly: options += " --edgeonly "
        cmdStr = ("python " + self._datagenpy + options +
            " --chunk " + str(chunkId) + " " + self._genArgStr + " " + self._cfgFileName)
        print("&&& running this:", cmdStr)
        genResult, genOut = self.runProcess(cmdStr)
        if genResult == 0:
            # Convert parquet files to csv.
            if not self.convertPg2Csv(chunkId):
                print("conversion to csv failed for", chunkId)
                return 'failed'
        else:
            print("generator failed for", chunkId)
            return 'failed'
        print("&&& genOut", genOut)
        return 'success'

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self._host, self._port))
            self._client = DataGenConnection(s)
            self._client.clientReqInit()
            self._name, self._genArgStr, self._cfgFileContents = self._client.clientRespInit()
            print("&&& name=", self._name, self._genArgStr, ":\n", self._cfgFileContents)
            # Read the file to get access to an identical chunker in self._spec
            self._readDatagenConfig()
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
                    createdChunks = list()
                    withOverlapChunks = list()
                    haveAllCsvChunks = list()
                    ingestedChunks = list()
                    for chunk in chunkRecvSet:
                        #&&&# Genrate the chunk parquet files.
                        #&&&cmdStr = ("python " + self._datagenpy + " --chunk " + str(chunk) + " " +
                        #&&&    self._genArgStr + " " + self._cfgFileName)
                        #&&&print("&&& running this:", cmdStr)
                        #&&&genResult, genOut = self.runProcess(cmdStr)
                        #&&&if genResult == 0:
                        #&&&    # Convert parquet files to csv.
                        #&&&    if not self.convertPg2Csv(chunk):
                        #&&&        print("conversion to csv failed for", chunk)
                        #&&&else:
                        #&&&    print("generator failed for", chunk)
                        #&&&print("&&& genOut", genOut)
                        # Generate the csv files for the chunk
                        if self._generateChunk(chunk, edgeOnly=False) != 'failed':
                            createdChunks.append(chunk)
                    # Create edge only chunks as needed.
                    chunker = self._chunker
                    for chunk in createdChunks:
                        # Find the chunks that should be next to chunk
                        neighborChunks = chunker.getChunksAround(chunk, self._overlap)
                        # Find the output files for the chunk, name must match "chunk<id>_*.csv"
                        foundCsv, filesCsv, neededChunks = self.findCsvInTargetDir(chunk, neighborChunks)
                        print("&&& foundCsv=", foundCsv, "fCsv=", filesCsv, " needed=", neededChunks)
                        if not foundCsv:
                            print("Problems with finding essential csv files for creating overlap chunk=",
                                  chunk, filesCsv)
                            continue
                        # Create edgeOnly neededChunks
                        createdAllNeeded = True
                        for nCh in neededChunks:
                            genResult = self._generateChunk(nCh, edgeOnly=True)
                            print("&&& needed=", nCh, " genResult=", genResult)
                            if genResult == 'failed':
                                print("Failed to generate chunk", nCh)
                                createdAllNeeded = False
                                continue
                        if createdAllNeeded:
                            print("&&& create all edgeOnly for ", nCh)
                            haveAllCsvChunks.append(chunk)
                    # &&& Generate overlaps, register with ingest.
                    for chunk in haveAllCsvChunks: #&&&MUST create overlap tables and setup ingest &&&HERE
                        withOverlapChunks.append(chunk)
                    # &&&
                    # If no chunks were created, likely fatal error.
                    if len(withOverlapChunks) == 0: # &&&Must change to ingestedChunks
                        print("ERROR no chunks were successfully created, ending program")
                        self._loop = False

                    # Client sends the list of completed chunks back
                    while True:
                        # Keep sending created chunks back until there are none left.
                        # If createdChunks was empty initially, the server needs to be sent an
                        # empty list to indicate complete failure.
                        createdChunks = self._client.clientReportChunksComplete(createdChunks) # &&&Must change to ingestedChunks
                        if len(createdChunks) == 0:
                            break

def testC():
    dgClient = DataGenClient("127.0.0.1", 13042)
    dgClient.run()


if __name__ == "__main__":
    testC()
