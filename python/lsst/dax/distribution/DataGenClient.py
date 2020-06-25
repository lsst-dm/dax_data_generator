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

import errno
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
        self._datagenpy = '~/work/dax_data_generator/bin/datagen.py' # TODO: this has to go
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
        try:
            os.mkdir(dirName)
        except OSError as err:
            if err.errno != errno.EEXIST:
                print("ERROR directory creation", dirName, err)
                return False
        return True

    def removeFile(self, fName):
        """Return True if the file was removed, false otherwise."""
        print("removing file", fName)
        try:
            os.remove(fName)
        except OSError as err:
            print("ERROR remove failed", fName, err)
            return False
        return True

    def runProcess(self, cmd):
        cwd = self._targetDir
        print("cwd", cwd, "cmd=", cmd)
        process = subprocess.Popen(cmd, cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        outStr = process.communicate()
        process.wait()
        print("process result", process.returncode)
        if process.returncode != 0:
            print("out=", outStr)
        return process.returncode, outStr

    def _readDatagenConfig(self):
        """ Create a Chunker and spec using the same configuration file as the datagen.py."""
        specGlobals = dict()
        exec(self._cfgFileContents, specGlobals)
        assert 'spec' in specGlobals, "Specification file must define a variable 'spec'."
        assert 'chunker' in specGlobals, "Specification file must define a variable 'chunker'."
        self._spec = specGlobals['spec']
        self._chunker = specGlobals['chunker']
        print("_cfgFileContents=", self._cfgFileContents)
        print("_spec=", self._spec)

    def convertPg2Csv(self, chunkId):
        """Convert parquet files to csv for chunkId. The original parquet files are deleted.
        Return True if all files are converted successfully and a list of generated csv files.
        """
        success = None
        # Find the relevant files in self._targetDir.
        findParquet = self.createFileName(chunkId, '*', 'parquet', useTargPath=True)
        print("cwd=", os.getcwd(), "findParquet=", findParquet)
        #TODO: python3 has better version of glob that would make this code cleaner.
        chunkParquetPaths = glob.glob(findParquet)
        # Remove the path from the file names.
        chunkParquetFiles = list()
        for fn in chunkParquetPaths:
            chunkParquetFiles.append(os.path.basename(fn))
        print("findParquet=", findParquet, " chunkParquetFiles=", chunkParquetFiles)

        outFileNames = list()
        if len(chunkParquetFiles) == 0:
            print("No parquet files found for", chunkId)
            return False, outFileNames
        for fName in chunkParquetFiles:
            outName = os.path.splitext(fName)[0] + ".csv"
            cmdStr = 'pq2csv --replace ' + fName + ' ' + outName
            genResult, genOut = self.runProcess(cmdStr)
            if genResult != 0:
                print("ERROR Failed to convert file", fName, "cmd=", cmdStr,
                      "res=", genResult, "out=", genOut)
                success = False
            outFileNames.append(outName)
            self.removeFile(os.path.join(self._targetDir, fName))
        print("outFileNames=", outFileNames)
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
        findCsv = self.createFileName(chunkId, '*', 'csv', useTargPath=True)
        #TODO: python3 has better version of glob that would make this code cleaner.
        chunkCsvPaths = glob.glob(findCsv)
        print("findCsv=", findCsv, " paths=", chunkCsvPaths)
        # Remove the path and add the files to a list
        chunkCsvFiles = list()
        for fn in chunkCsvPaths:
            chunkCsvFiles.append(os.path.basename(fn))
        print("chunkCsvFiles=", chunkCsvFiles)
        # These cannot be edgeOnly, and there must be one for each entry in self._spec['spec']
        for tblName in self._spec:
            fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=False, useTargPath=False)
            if not fn in chunkCsvFiles:
                print("Failed to find ", fn, "in", chunkCsvFiles)
                success = False
                return success, foundCsv, neededEdgeOnly
            foundCsv.append(fn)
        # remove chunkId from neighborChunks, if it is there
        nbChunks = [val for val in neighborChunks if val != chunkId]
        # See if the neighbor chunk csv files exist. They can be complete or edge only.
        for nbCh in nbChunks:
            allTablesFound = True
            completeFound = False
            eOFound = False
            for tblName in self._spec:
                fn = self.createFileName(nbCh, tblName, 'csv', edgeOnly=False, useTargPath=False)
                fnEO = self.createFileName(nbCh, tblName, 'csv', edgeOnly=True, useTargPath=False)
                if fn in chunkCsvFiles:
                    foundCsv.append(fn)
                    completeFound = True
                elif fnEO in chunkCsvFiles:
                    foundCsv.append(fnEO)
                    eOFound = True
                else:
                    allTablesFound = False
            if completeFound and eOFound:
                print("WARN both complete and edgeOnly files found for neighbor chunk=", nbCh)
                # That shouldn't have happened, maybe left over from previous run.
                # Remove all of the files for nbCh and make sure it is added
                # to neededEdgeOnly.
                self.removeFilesForChunk(nbCh, edgeOnly=True, complete=True)
                allTablesFound = False
            if not allTablesFound:
                neededEdgeOnly.append(nbCh)
        return success, foundCsv, neededEdgeOnly

    def removeFilesForChunk(self, chunkId, edgeOnly=False, complete=False):
        """Remove expected files for these chunks from the target directory."""
        print("removeFilesForChunk", chunkId, edgeOnly, complete)
        for tblName in self._spec:
            fList = list()
            if edgeOnly:
                fList.append(self.createFileName(chunkId, tblName, 'csv',
                             edgeOnly=True, useTargPath=True))
                fList.append(self.createFileName(chunkId, tblName, 'parquet',
                             edgeOnly=True, useTargPath=True))
            else:
                fList.append(self.createFileName(chunkId, tblName, 'csv',
                             edgeOnly=False, useTargPath=True))
                fList.append(self.createFileName(chunkId, tblName, 'parquet',
                             edgeOnly=False, useTargPath=True))
            for fn in fList:
                if os.path.exists(fn):
                    print("removing file", fn)
                    if not self.removeFile(fn):
                        print("ERROR remove failed", fn, err)
                        return False
            return True

    def _removeWildCard(self, pattern):
        """Remove files matching 'pattern'. Return True if successful or no files found."""
        print("&&& pattern=", pattern)
        fList = glob.glob(pattern)
        for fn in fList:
            if not self.removeFile(fn):
                return False
        return True

    def _fillChunkDir(self, chunkId, neighborChunks):
        """ Create a directory with all the csv files needed for the
        partitioner to create the files needed to ingest the chunk.
        The directory is self._targetDir/<chunkId>"""
        print("&&&fillChunkDir chunkId=", chunkId, neighborChunks)
        # If the chunk directory already exists, empty it.
        if not os.path.exists(self._targetDir):
            print("ERROR targetDirectory does not exist.")
            return False
        dirName = os.path.join(self._targetDir, str(chunkId))
        if os.path.exists(dirName):
            # It shouldn't exist, delete its .csv contents and index.bin
            # TODO: May need to remove other files.
            fNames = list()
            fNames.append(os.path.join(self._targetDir, '*.csv'))
            fNames.append(os.path.join(self._targetDir, 'index.bin'))
            for nm in fNames:
                if not self._removeWildCard(nm):
                    print("ERROR fillChunkDir remove failed", nm)
                    return False
        else:
            if not self.makeDir(dirName):
                print("ERROR directory creation", dirName)
                return False
        cList = neighborChunks.copy()
        if not chunkId in cList:
            cList.append(chunkId)
        for cId in cList:
            # Only the 'CT' or 'EO' csv files should exist, so hard link
            # all csv files for the chunks.
            # &&& MAY have to rename the links to remove 'CT_' and 'EO_'
            pattern = 'chunk'+str(cId)+'_*.csv'
            pattern = os.path.join(self._targetDir, pattern)
            fList = glob.glob(pattern)
            for fn in fList:
                linkName = os.path.basename(fn)
                linkName = os.path.join(self._targetDir, str(chunkId), linkName)
                try:
                    os.link(fn, linkName)
                except  OSError as err:
                    if err.errno != errno.EEXIST:
                        print("ERROR fillChunkDir link failed", fn, linkName)
                        return False
        return True

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
            # If there is a full set of complete files, delete the edge only files and return.
            edgeOnlyCount = 0
            completeCount = 0
            spec = self._spec
            for tblName in spec:
                fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=True, useTargPath=True)
                if os.path.exists(fn):
                    edgeOnlyCount += 1
                fn = self.createFileName(chunkId, tblName, 'csv', edgeOnly=False, useTargPath=True)
                if os.path.exists(fn):
                    completeCount += 1
            print("edgeOnlyCount=", edgeOnlyCount, "completeCount=", completeCount)
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
            # Delete files for this chunk if they exist.
            self.removeFilesForChunk( chunkId, edgeOnly=True, complete=True)
        # Genrate the chunk parquet files.
        options = " --edgefirst "
        if edgeOnly: options += " --edgeonly "
        cmdStr = ("python " + self._datagenpy + options +
            " --chunk " + str(chunkId) + " " + self._genArgStr + " " + self._cfgFileName)
        genResult, genOut = self.runProcess(cmdStr)
        if genResult == 0:
            # Convert parquet files to csv.
            if not self.convertPg2Csv(chunkId):
                print("conversion to csv failed for", chunkId)
                return 'failed'
        else:
            print("ERROR Generator failed for", chunkId, " cmd=", cmdStr,
                  "out=", genOut)
            return 'failed'
        return 'success'

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self._host, self._port))
            self._client = DataGenConnection(s)
            self._client.clientReqInit()
            self._name, self._genArgStr, self._cfgFileContents = self._client.clientRespInit()
            print("name=", self._name, self._genArgStr, ":\n", self._cfgFileContents)
            # Read the file to get access to an identical chunker in self._spec
            self._readDatagenConfig()
            # write the configuration file
            fileName = os.path.join(self._targetDir, self._cfgFileName)
            with open(fileName, "w") as fw:
                fw.write(self._cfgFileContents)
            while self._loop:
                self._client.clientReqChunks(self._chunksPerReq)
                chunkListRecv, problem = self._client.clientRecvChunks()
                if problem:
                    print("WARN there was a problem with", chunkListRecv)
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
                        print("foundCsv=", foundCsv, "fCsv=", filesCsv, " needed=", neededChunks)
                        if not foundCsv:
                            print("ERROR Problems with finding essential csv for creating overlap chunk=",
                                  chunk, filesCsv)
                            continue
                        # Create edgeOnly neededChunks
                        createdAllNeeded = True
                        for nCh in neededChunks:
                            genResult = self._generateChunk(nCh, edgeOnly=True)
                            if genResult == 'failed':
                                print("ERROR Failed to generate chunk", nCh)
                                createdAllNeeded = False
                                continue
                        if createdAllNeeded:
                            print("Created all needed edgeOnly for ", nCh)
                            # Put hardlinks to all the files needed for a chunk in
                            # a specific directory for the partioner to use to create
                            # the overlap tables and so on.
                            if self._fillChunkDir(chunk, neighborChunks):
                                haveAllCsvChunks.append(chunk)
                    # TODO: Generate overlaps, register with ingest.
                    for chunk in haveAllCsvChunks:
                        # TODO: MUST create overlap tables
                        withOverlapChunks.append(chunk)

                    # TODO: Maybe report withOverlapChunks to server. Chunks that get this far
                    #       but do not get reported as ingested would be easier to track down
                    #       and check.
                    for chunk in withOverlapChunks:
                        # TODO: MUST ingest chunks with overlaps &&&
                        ingestedChunks.append(chunk)

                    # If no chunks were created, likely fatal error. Asking for more
                    # chunks to create would just cause more problems.
                    if len(ingestedChunks) == 0:
                        print("ERROR no chunks were successfully ingested, ending program")
                        self._loop = False

                    # Client sends the list of completed chunks back
                    while True:
                        # Keep sending created chunks back until there are none left.
                        # If ingestedChunks is empty initially, the server needs to be sent an
                        # empty list to indicate failure to ingest anything.
                        ingestedChunks = self._client.clientReportChunksComplete(ingestedChunks)
                        if len(ingestedChunks) == 0:
                            break

def testC():
    dgClient = DataGenClient("127.0.0.1", 13042)
    dgClient.run()


if __name__ == "__main__":
    testC()
