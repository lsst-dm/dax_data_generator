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
import shutil
import socket
import subprocess

from DataGenConnection import DataGenConnection


class DataGenClient:
    """This class is used to connect to the DataGenServer and build chunks.

    Parameters
    ----------
    host : str
        Name of the server's host.
    port : int
        Server's port number.
    target_dir : str, optional
        The client's working directory. Clients must have different working
        directories.
    chunks_per_req : int, optional
        The number of chunks wanted per request from the server.

    Note
    ----
    This class is used to connect to the DataGenServer and uses the information
    the server provides to generate appropriate fake chunks while reporting
    what chunks have been created and registered with the ingest system.
    """

    def __init__(self, host, port, target_dir='fakeData', chunks_per_req=5):
        self._host = host
        self._port = port
        self._target_dir = os.path.abspath(target_dir)
        self._chunksPerReq = chunks_per_req
        self._loop = True # set to false to end the program
        self._gen_arg_str = None # Arguments from the server for the generator.
        self._client = None # DataGenConnection
        self._cfg_file_name = 'gencfg.py' # name of the local config file for the generator
        self._cfg_file_contents = None # contents of the config file.
        self._spec = None # spec from exec(self._cfgFileContents)
        self._chunker = None # chunker from exec(self._cfgFileContents)
        self._overlap = 0.018 # overlap in degrees, about 1 arcmin. TODO: This should be put
                              # in the cfg_file from the server as changing it will change
                              # the chunk contents.
        self._datagenpy = '~/work/dax_data_generator/bin/datagen.py' # TODO: this has to go
        self._pt_cfg_dir = 'partitionCfgs' # sub-dir of _targetDir for partitioner configs
        self._pt_cfg_dict = None # Dictionary that stores partioner config files.
        self.makeDir(self._target_dir)
        self.makeDir(os.path.join(self._target_dir, self._pt_cfg_dir))

    def createFileName(self, chunk_id, table_name, ext, edge_only=False, use_targ_path=False):
        """Create a consistent file name given the input parameters.

        Parameters
        ----------
        chunk_id : int
            Chunk id number.
        table_name : str
            Name of the table related to this file.
        ext : str
            Extension of the file.
        edge_only : bool, optional
            If True, the name indicates the file only contains information
            about objects near the edges of the chunk needed for overlap.
            If False, the name indicates the file contains all objects in
            the chunk.
        use_targ_path : bool, optional
            If True, the file name will start with self._target_dir

        Return :
            fn : str
            The name to use for the file.
        """
        typeStr = 'CT_'
        if edge_only: typeStr = 'EO_'
        # If the tabelName is a wildcard, don't use typeStr
        if table_name == '*': typeStr = ''
        fn = 'chunk' + str(chunk_id) + '_' + typeStr + table_name + '.' + ext
        if use_targ_path:
            fn = os.path.join(self._target_dir, fn)
        return fn

    def makeDir(self, dirName):
        """Make a directory catching the already exists exception.
        """
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

    def runProcess(self, cmd, cwd=None):
        """Run a process.

        Parameters
        ----------
        cmd : str
            The command to be run. This includes all command line arguments.
        cwd : str, optional
            The current working directory for the command.
            If this is None, cwd will be set to self._target_dir before
            running cmd.

        Return
        ------
        process.returncode : int
            Process depenedant, but non-zero usually indicates failure.
        out_str : str
            Process terminal output.
        """
        if not cwd:
            cwd = self._target_dir
        print("cwd", cwd, "cmd=", cmd)
        process = subprocess.Popen(cmd, cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out_str = process.communicate()
        process.wait()
        if process.returncode != 0:
            print("out=", out_str)
        return process.returncode, out_str

    def _readDatagenConfig(self):
        """ Create a Chunker and spec using the same configuration file as the datagen.py.
        """
        spec_globals = dict()
        exec(self._cfg_file_contents, spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        self._spec = spec_globals['spec']
        self._chunker = spec_globals['chunker']
        print("_cfgFileContents=", self._cfg_file_contents)
        print("_spec=", self._spec)

    def convertPg2Csv(self, chunk_id):
        """Convert parquet files to csv for chunk_id. The original parquet files are deleted.

        Parameters
        ----------
        chunk_id : int
            The chunk id number.

        Return
        ------
        success : bool
            Return True if all files are converted successfully.
        out_file_names : list of str
            A list of names of generated csv files.
        """
        success = None
        # Find the relevant files in self._targetDir.
        find_parquet = self.createFileName(chunk_id, '*', 'parquet', use_targ_path=True)
        print("cwd=", os.getcwd(), "findParquet=", find_parquet)
        #TODO: python3 has better version of glob that would make this code cleaner.
        chunk_parquet_paths = glob.glob(find_parquet)
        # Remove the path from the file names.
        chunk_parquet_files = list()
        for fn in chunk_parquet_paths:
            chunk_parquet_files.append(os.path.basename(fn))
        print("findParquet=", find_parquet, " chunkParquetFiles=", chunk_parquet_files)

        out_file_names = list()
        if len(chunk_parquet_files) == 0:
            print("No parquet files found for", chunk_id)
            return False, out_file_names
        for fname in chunk_parquet_files:
            out_name = os.path.splitext(fname)[0] + ".csv"
            cmd = 'pq2csv --replace ' + fname + ' ' + out_name
            gen_result, gen_out = self.runProcess(cmd)
            if gen_result != 0:
                print("ERROR Failed to convert file", fname, "cmd=", cmd,
                      "res=", gen_result, "out=", gen_out)
                success = False
            out_file_names.append(out_name)
            self.removeFile(os.path.join(self._target_dir, fname))
        if success == None:
            success = True
        return success, out_file_names

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
        findCsv = self.createFileName(chunkId, '*', 'csv', use_targ_path=True)
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
            fn = self.createFileName(chunkId, tblName, 'csv', edge_only=False, use_targ_path=False)
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
                fn = self.createFileName(nbCh, tblName, 'csv', edge_only=False, use_targ_path=False)
                fnEO = self.createFileName(nbCh, tblName, 'csv', edge_only=True, use_targ_path=False)
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
                if not self.removeFilesForChunk(nbCh, edgeOnly=True, complete=True):
                    print("WARN failed to remove files for nbCh=", nbCh)
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
                             edge_only=True, use_targ_path=True))
                fList.append(self.createFileName(chunkId, tblName, 'parquet',
                             edge_only=True, use_targ_path=True))
            else:
                fList.append(self.createFileName(chunkId, tblName, 'csv',
                             edge_only=False, use_targ_path=True))
                fList.append(self.createFileName(chunkId, tblName, 'parquet',
                             edge_only=False, use_targ_path=True))
            for fn in fList:
                if os.path.exists(fn):
                    print("removing file", fn)
                    if not self.removeFile(fn):
                        print("ERROR remove failed", fn, err)
                        return False
        return True

    def _fillChunkDir(self, chunkId, neighborChunks):
        """ Create a directory with all the csv files needed for the
        partitioner to create the files needed to ingest the chunk.
        The directory is self._targetDir/<chunkId>"""
        print("fillChunkDir chunkId=", chunkId, neighborChunks)
        # If the chunk directory already exists, empty it.
        if not os.path.exists(self._target_dir):
            print("ERROR targetDirectory does not exist.")
            return False
        chunkIdStr = str(chunkId)
        if chunkIdStr == "":
            print("ERROR chunkIdStr is empty")
            return False
        dirName = os.path.join(self._target_dir, str(chunkId))
        if os.path.exists(dirName):
            # It shouldn't exist, delete it
            shutil.rmtree(dirName)
        if not self.makeDir(dirName):
            print("ERROR directory creation", dirName)
            return False
        cList = neighborChunks.copy()
        if not chunkId in cList:
            cList.append(chunkId)
        for cId in cList:
            # Only the 'CT' or 'EO' csv files should exist, so hard link
            # all csv files for the chunks.
            pattern = 'chunk'+str(cId)+'_*.csv'
            pattern = os.path.join(self._target_dir, pattern)
            fList = glob.glob(pattern)
            for fn in fList:
                linkName = os.path.basename(fn)
                linkName = os.path.join(self._target_dir, str(chunkId), linkName)
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
                fn = self.createFileName(chunkId, tblName, 'csv', edge_only=True, use_targ_path=True)
                if os.path.exists(fn):
                    edgeOnlyCount += 1
                fn = self.createFileName(chunkId, tblName, 'csv', edge_only=False, use_targ_path=True)
                if os.path.exists(fn):
                    completeCount += 1
            print("edgeOnlyCount=", edgeOnlyCount, "completeCount=", completeCount)
            if completeCount == len(spec) or edgeOnlyCount == len(spec):
                print("All expected tables already exist, will not generate. chunkid=", chunkId)
                if completeCount == len(spec):
                    print("Removing extraneous edgeOnly files")
                    if not self.removeFilesForChunk(chunkId, edgeOnly=True, complete=False):
                        print("WARN failed to remove extraneous csv for", chunkId)
                else: # Not a full set of complete files
                    print("Removing extraneous complete files")
                    if not self.removeFilesForChunk(chunkId, edgeOnly=False, complete=True):
                        print("WARN failed to remove incomplete csv for", chunkId)
                return 'exists'
        else:
            # Delete files for this chunk if they exist.
            if not self.removeFilesForChunk( chunkId, edgeOnly=True, complete=True):
                print("WARN failed to remove all files for chunk=", chunkId)
        # Genrate the chunk parquet files.
        options = " "
        if edgeOnly: options += " --edgeonly "
        cmdStr = ("python " + self._datagenpy + options +
            " --chunk " + str(chunkId) + " " + self._gen_arg_str + " " + self._cfg_file_name)
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

    def _createOverlapTables(self, chunkId):
        """Create the overlap files from the files in the chunk directory.
        This needs to be done for each table which has an matching partitioner
        configuration file. Once all the overlap files for the target chunk
        have been made, all the extra files are deleted.
        """
        # Everything happens in the ovlDir directory
        ovlDir = os.path.join(self._target_dir, str(chunkId))
        entries = os.listdir(ovlDir)
        files = list()
        for e in entries:
            if os.path.isfile(os.path.join(ovlDir, e)):
                files.append(os.path.basename(e))
            else:
                print("not a file ", os.path.join(ovlDir, e))
        # for each configuration file in self._partitionerCfgs
        # sph-partition -c (cfgdir)/Object.cfg --mr.num-workers 1 --out.dir outdir --in chunk0_CT_Object.csv
        # --in chunk402_CT_Object.csv --in chunk401_CT_Object.csv --in chunk400_CT_Object.csv
        # --in chunk404_EO_Object.csv --in chunk403_CT_Object.csv
        for cfg in self._pt_cfg_dict.items():
            # Determine the table name from the config file name.
            cfgFName = cfg[1][0]
            tblName = os.path.splitext(cfgFName)[0]
            # The list of --in files needs to be generated. It
            # needs to have all the .csv files for tblName.
            inCsvFiles = list()
            reg = re.compile(r"chunk\w*_" + tblName + r"\.csv")
            for f in files:
                m = reg.match(f)
                if m:
                    inCsvFiles.append(f)
            inStr = ""
            for csv in inCsvFiles:
                inStr += " --in " + csv
            cfgFPath = os.path.join(self._target_dir, self._pt_cfg_dir, cfgFName)
            outDir = os.path.join(ovlDir, "outdir" + tblName)
            cmd = "sph-partition -c " + cfgFPath + " --mr.num-workers 1 "
            cmd += " --out.dir " + outDir + " " + inStr
            genResult, genOut = self.runProcess(cmd, cwd=ovlDir)
            if genResult != 0:
                # Return False, leave data for diagnostics.
                print("ERROR failed to create chunk and ovelap .txt files", genOut, "cmd=", cmd)
                return False
            # Delete the .txt files for chunk numbers other than chunkId.
            entries = os.listdir(outDir)
            reg = re.compile(r"^chunk_" + str(chunkId) + r"(_overlap)?\.txt$")
            for ent in entries:
                fn = os.path.basename(ent)
                m = reg.match(fn)
                if m:
                    print("keeping ", fn)
                else:
                    os.remove(os.path.join(outDir, ent))
            return True

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self._host, self._port))
            self._client = DataGenConnection(s)
            self._client.clientReqInit()
            self._name, self._gen_arg_str, self._cfg_file_contents = self._client.clientRespInit()
            print("name=", self._name, self._gen_arg_str, ":\n", self._cfg_file_contents)
            # Read the datagen config file to get access to an identical chunker and spec.
            self._readDatagenConfig()
            # Write the configuration file
            fileName = os.path.join(self._target_dir, self._cfg_file_name)
            with open(fileName, "w") as fw:
                fw.write(self._cfg_file_contents)
            # Request partioner configuration files from server
            pCfgIndex = 0
            pCfgDict = dict()
            pCfgName = "nothing"
            while not pCfgName == "":
                self._client.clientReqPartitionCfgFile(pCfgIndex)
                indx, pCfgName, pCfgContents = self._client.clientRespPartionCfgFile()
                if indx != pCfgIndex:
                    self.success = False
                    raise RuntimeError("Client got wrong pCfgIndex=", pCfgIndex,
                                       "indx=", indx, pCfgName)
                print("pCfgName=", pCfgName)
                if not pCfgName == "":
                    pCfgDict[pCfgIndex] = (pCfgName, pCfgContents)
                pCfgIndex += 1
            self._pt_cfg_dict = pCfgDict
            # Write those files to the partitioner config directory
            # (mostly for diagnostic purposes)
            pCfgDir = os.path.join(self._target_dir, self._pt_cfg_dir)
            for it in pCfgDict.items():
                pCfgName = os.path.join(pCfgDir, it[1][0])
                print("writing ", it[0], "name=", pCfgName)
                with open(pCfgName, "w") as fw:
                    fw.write(it[1][1])
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
                    # Generate overlap tables and files for ingest.
                    for chunk in haveAllCsvChunks:
                        if self._createOverlapTables(chunk):
                            print("created overlap for chunk", chunk)
                            withOverlapChunks.append(chunk)
                        else:
                            print("ERROR failed to create overlap for chunk", chunk)
                    # TODO: Maybe report withOverlapChunks to server. Chunks that get this far
                    #       but do not get reported as ingested would be easier to track down
                    #       and examine.
                    # TODO: Register with ingest. &&& It looks like this is probably easier to do
                    #                                 when generating the overlaps.
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
                        # empty list to indicate the failure to ingest anything.
                        ingestedChunks = self._client.clientReportChunksComplete(ingestedChunks)
                        if len(ingestedChunks) == 0:
                            break

def testC():
    dgClient = DataGenClient("127.0.0.1", 13042)
    dgClient.run()


if __name__ == "__main__":
    testC()
