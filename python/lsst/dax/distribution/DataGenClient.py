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
from DataIngest import DataIngest



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
        self._gen_arg_str = None # Arguments from the server for the generator.
        self._cl_conn = None # DataGenConnection
        self._cfg_file_name = 'gencfg.py' # name of the local config file for the generator
        self._cfg_file_contents = None # contents of the config file.
        self._datagenpy = '~/work/dax_data_generator/bin/datagen.py' # TODO: MUST stop hard coding this
        self._pt_cfg_dir = 'partitionCfgs' # sub-dir of _targetDir for partitioner configs
        self._pt_cfg_dict = None # Dictionary that stores partioner config files.
        self.makeDir(self._target_dir)
        self.makeDir(os.path.join(self._target_dir, self._pt_cfg_dir))

        # Values set from transferred self._cfgFileContents
        self._spec = None # spec from exec(self._cfgFileContents)
        self._chunker = None # chunker from exec(self._cfgFileContents)
        self._edge_width = None # Width of edges in edge only generation.

        # Ingest values
        self._ingest = None
        self._skip_ingest = True
        self._db_name = ''
        self._transaction_id = -1

    def _setIngest(self, ingest_dict):
        """Create ingest object from ingest_dict values.

        Parameters
        ----------
        ingest_dict : dictionary
            Dictionary containing information about the ingest system.
            'host' : str, ingest system host name.
            'port' : int, ingest port number.
            'user' : str, ingest user name.
            'auth' : str, ingest user name.
            'db'   : str, name of the databse being created
            'skip  : bool, true if ingest is being skipped.

        Note
        ----
        The keys in ingest_dict should match those in servRespInit and clientRespInit.
        """
        ingd = ingest_dict
        self._ingest = DataIngest(ingd['host'], ingd['port'], ingd['user'], ingd['auth'])
        self._skip_ingest = ingd['skip']
        self._db_name = ingd['db']

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

    def makeDir(self, dir_name):
        """Make a directory catching the already exists exception.

        Parameters
        ----------
        dir_name : str
            Name of the directory to create.

        Return
        ------
        sucess : bool
            True if directory was created or already existed.
        """
        try:
            os.mkdir(dir_name)
        except OSError as err:
            if err.errno != errno.EEXIST:
                print("ERROR directory creation", dir_name, err)
                return False
        return True

    def removeFile(self, f_name):
        """Return True if the file f_name was removed, false otherwise.
        """
        print("removing file", f_name)
        try:
            os.remove(f_name)
        except OSError as err:
            print("ERROR remove failed", f_name, err)
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
        spec_globals = {}
        exec(self._cfg_file_contents, spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        assert 'edge_width' in spec_globals, "Specification file must define a variable 'edge_width'."
        self._spec = spec_globals['spec']
        self._chunker = spec_globals['chunker']
        self._edge_width = spec_globals['edge_width']
        print("_cfgFileContents=", self._cfg_file_contents)
        print("_spec=", self._spec)

    def findCsvInTargetDir(self, chunk_id, neighbor_chunks):
        """Find files required csv to generate overlap for chunk_id.

        Parameters
        ----------
        chunk_id : int
            The chunk which needs an overlap table.
        neighbor_chunks : list of int
            A list of all the chunk_ids for the chunks next to chunk_id.

        Return
        ------
            success : bool
                True, there are the required csv files for chunk_id.
            foundCsv : list of str
                A list of csv files for chunkId and neighborChunks.
            neededEdgeOnly : list of int
                A list chunk ids that need to be generated edgeOnly.

        Note
        ----
            To generate the overlap file, there needs to be the
            complete csv file for chunk_id and either complete
            or edge only csv files for all of its neighbors.
        """
        success = True
        foundCsv = []
        neededEdgeOnly = []
        # Find the relevant files in self._targetDir.
        findCsv = self.createFileName(chunk_id, '*', 'csv', use_targ_path=True)
        #TODO: python3 has better version of glob that would make this code cleaner.
        chunkCsvPaths = glob.glob(findCsv)
        print("findCsv=", findCsv, " paths=", chunkCsvPaths)
        # Remove the path and add the files to a list
        chunkCsvFiles = []
        for fn in chunkCsvPaths:
            chunkCsvFiles.append(os.path.basename(fn))
        print("chunkCsvFiles=", chunkCsvFiles)
        # These cannot be edgeOnly, and there must be one for each entry in self._spec['spec']
        for tblName in self._spec:
            fn = self.createFileName(chunk_id, tblName, 'csv', edge_only=False, use_targ_path=False)
            if not fn in chunkCsvFiles:
                print("Failed to find ", fn, "in", chunkCsvFiles)
                success = False
                return success, foundCsv, neededEdgeOnly
            foundCsv.append(fn)
        # remove chunkId from neighborChunks, if it is there
        nbChunks = [val for val in neighbor_chunks if val != chunk_id]
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
                if not self.removeFilesForChunk(nbCh, edge_only=True, complete=True):
                    print("WARN failed to remove files for nbCh=", nbCh)
                allTablesFound = False
            if not allTablesFound:
                neededEdgeOnly.append(nbCh)
        return success, foundCsv, neededEdgeOnly

    def removeFilesForChunk(self, chunk_id, edge_only=False, complete=False):
        """Remove files for chunk_id from the target directory.

        Parameters
        ----------
        chunk_id : int
            Chunk id number for the csv and parquet files to remove.
        edge_only : bool
            When True, remove edge only csv and parquet files.
        complete : bool
            When True, remove complete csv and parquet files

        Return
        ------
        success : bool
            True if all files were removed.
        """
        print("removeFilesForChunk", chunk_id, edge_only, complete)
        for tblName in self._spec:
            fList = []
            if edge_only:
                fList.append(self.createFileName(chunk_id, tblName, 'csv',
                             edge_only=True, use_targ_path=True))
                fList.append(self.createFileName(chunk_id, tblName, 'parquet',
                             edge_only=True, use_targ_path=True))
            if complete:
                fList.append(self.createFileName(chunk_id, tblName, 'csv',
                             edge_only=False, use_targ_path=True))
                fList.append(self.createFileName(chunk_id, tblName, 'parquet',
                             edge_only=False, use_targ_path=True))
            for fn in fList:
                if os.path.exists(fn):
                    print("removing file", fn)
                    if not self.removeFile(fn):
                        print("ERROR remove failed", fn)
                        return False
        return True

    def _fillChunkDir(self, chunk_id, neighborChunks):
        """Create and fill a directory with all the csv files needed for the
        partitioner to make chunk_id.

        Parameters
        ----------
        chunk_id : int
            Chunk id for the chunk that needs an overlap table.
        neighbor_chunks : list of int
            Chunk id numbers for the chunks next to chunk_id.

        Return
        ------
        success : bool
            True indicates success.

        Note
        ----
        The directory is self._targetDir/<chunkId> and should contain
        all the csv files needed for the partioner to build chunk and
        overlap files for ingest. One file for each table in each chunk.
        """
        print("fillChunkDir chunkId=", chunk_id, neighborChunks)
        # If the chunk directory already exists, empty it.
        if not os.path.exists(self._target_dir):
            print("ERROR targetDirectory does not exist.")
            return False
        chunkIdStr = str(chunk_id)
        if chunkIdStr == "":
            print("ERROR chunkIdStr is empty")
            return False
        dirName = os.path.join(self._target_dir, str(chunk_id))
        if os.path.exists(dirName):
            # It shouldn't exist, delete it
            shutil.rmtree(dirName)
        if not self.makeDir(dirName):
            print("ERROR directory creation", dirName)
            return False
        cList = neighborChunks.copy()
        if not chunk_id in cList:
            cList.append(chunk_id)
        for cId in cList:
            # Only the 'CT' or 'EO' csv files should exist, so hard link
            # all csv files for the chunks.
            pattern = 'chunk'+str(cId)+'_*.csv'
            pattern = os.path.join(self._target_dir, pattern)
            fList = glob.glob(pattern)
            for fn in fList:
                linkName = os.path.basename(fn)
                linkName = os.path.join(self._target_dir, str(chunk_id), linkName)
                try:
                    os.link(fn, linkName)
                except  OSError as err:
                    if err.errno != errno.EEXIST:
                        print("ERROR fillChunkDir link failed", fn, linkName)
                        return False
        return True

    def _generateChunk(self, chunk_id, edge_only=False):
        """Generate the csv files for a chunk.

        Parameters
        ----------
        edge_only : bool
            True - will cause an edge only Chunk to be generated. The edge
                only chunks will not be created if there is an existing
                complete chunk.
            False - will cause a complete chunk to be created and will result
                in existing csv files for that chunk id to be deleted.

        Return
        ------
        status : str
            'success' if the chunk was made successfully
            'failed'  if a valid version of the chunk could not be made
            'existed' if a valid complete version of the chunk already existed
                      and edgeOnly=True
        """
        if edge_only:
            # Check for existing csv files. If a full set of complete files are found or
            # a full set of edge only files are found, return 'existed'
            # If there is a full set of complete files, delete the edge only files and return.
            edgeOnlyCount = 0
            completeCount = 0
            spec = self._spec
            for tblName in spec:
                fn = self.createFileName(chunk_id, tblName, 'csv', edge_only=True, use_targ_path=True)
                if os.path.exists(fn):
                    edgeOnlyCount += 1
                fn = self.createFileName(chunk_id, tblName, 'csv', edge_only=False, use_targ_path=True)
                if os.path.exists(fn):
                    completeCount += 1
            print("edgeOnlyCount=", edgeOnlyCount, "completeCount=", completeCount)
            if completeCount == len(spec) or edgeOnlyCount == len(spec):
                print("All expected tables already exist, will not generate. chunkid=", chunk_id)
                if completeCount == len(spec):
                    print("Removing extraneous edgeOnly files")
                    if not self.removeFilesForChunk(chunk_id, edge_only=True, complete=False):
                        print("WARN failed to remove extraneous csv for", chunk_id)
                else: # Not a full set of complete files
                    print("Removing extraneous complete files")
                    if not self.removeFilesForChunk(chunk_id, edge_only=False, complete=True):
                        print("WARN failed to remove incomplete csv for", chunk_id)
                return 'exists'
        else:
            # Delete files for this chunk if they exist.
            if not self.removeFilesForChunk( chunk_id, edge_only=True, complete=True):
                print("WARN failed to remove all files for chunk=", chunk_id)
        # Genrate the chunk parquet files.
        options = " "
        if edge_only:
            options += " --edgeonly "
        cmdStr = ("python " + self._datagenpy + options +
            " --chunk " + str(chunk_id) + " " + self._gen_arg_str + " " + self._cfg_file_name)
        genResult, genOut = self.runProcess(cmdStr)
        if genResult != 0:
            print("ERROR Generator failed for", chunk_id, " cmd=", cmd, "args=", args,
                  "out=", genOut)
            return 'failed'
        return 'success'

    def _createRecvChunks(self, chunk_recv_set):
        """Create csv files for all tables in chunk_recv_set.

        Parameters
        ----------
        chunk_recv_set : set of int
            Set of chunk ids most recently received from the server.

        Return
        ------
        created_chunks : list of int
            List of chunk ids where all csv tables were created.
        """
        created_chunks = []
        for chunk_id in chunk_recv_set:
            # Generate the csv files for the chunk
            if self._generateChunk(chunk_id, edge_only=False) != 'failed':
                created_chunks.append(chunk_id)
        return created_chunks

    def _createNeighborChunks(self, created_chunks):
        """Create neighbor chunks for all created chunks as needed.

        Parameters
        ----------
        created_chunks : list of int
            List of chunk ids that have been created from the most recent
            list sent by the server.

        Return
        ------
        have_all_csv_chunks : list of int
            List of chunk ids from created_chunks where all the neccessary
            neighbor chunks could be created or already existed.

        Note
        ----
        Neighbor chunks may be edge only, but the tables in created_chunks
        must be complete chunks.
        """
        chunker = self._chunker
        have_all_csv_chunks = []
        for chunk in created_chunks:
            # Find the chunks that should be next to chunk
            neighborChunks = chunker.getChunksAround(chunk, self._edge_width)
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
                genResult = self._generateChunk(nCh, edge_only=True)
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
                    have_all_csv_chunks.append(chunk)
        return have_all_csv_chunks

    def _createOverlapTables(self, chunkId):
        """Create ingest files and pass them to the ingest system.

        Parameters
        ----------
        chunk_id : int
            Chunk id number for which overlap and ingest files are created.

        Return
        ------
        success : bool
            True if overlap and ingest files were created and sent to ingest.

        Note
        ----
        This needs to be done for each table in the chunk which has a
        matching partitioner configuration file. The partitioner creates
        files for all input chunks, chunk_id and all of its neighbors.
        Once all the ingest and overlap files for the target chunk
        have been made, all the extra files are deleted to prevent the
        possible ingest of duplicate data.
        """
        # Everything happens in the ovlDir directory
        ovlDir = os.path.join(self._target_dir, str(chunkId))
        entries = os.listdir(ovlDir)
        files = []
        for e in entries:
            if os.path.isfile(os.path.join(ovlDir, e)):
                files.append(os.path.basename(e))
            else:
                print("not a file ", os.path.join(ovlDir, e))
        # for each configuration file in self._partitionerCfgs something like this for Object chunk 0
        # sph-partition -c (cfgdir)/Object.cfg --mr.num-workers 1 --out.dir outdirObject
        # --in chunk0_CT_Object.csv --in chunk402_CT_Object.csv
        # --in chunk401_CT_Object.csv --in chunk400_CT_Object.csv
        # --in chunk404_EO_Object.csv --in chunk403_CT_Object.csv
        info_list = [] # A list of tuples (tblName, fullPathFile)
        for cfg in self._pt_cfg_dict.items():
            # Determine the table name from the config file name.
            cfgFName = cfg[1][0]
            tblName = os.path.splitext(cfgFName)[0]
            # The list of --in files needs to be generated. It
            # needs to have all the .csv files for tblName.
            inCsvFiles = []
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
                print("ERROR failed to create chunk and overlap .txt files", genOut, "cmd=", cmd)
                return False
            # Delete the .txt files for chunk numbers other than chunk_id.
            entries = os.listdir(outDir)
            reg = re.compile(r"^chunk_" + str(chunkId) + r"(_overlap)?\.txt$")
            for ent in entries:
                fn = os.path.basename(ent)
                full_path = os.path.join(outDir, ent)
                m = reg.match(fn)
                if m:
                    print("keeping ", fn, tblName, full_path)
                    info_list.append((tblName, full_path))
                else:
                    os.remove(full_path)
        for info in info_list:
            print("info=", info, "0=", info[0], "1=", info[1])
            self._addChunkToTransaction(chunkId, table=info[0], f_path=info[1])
        return True

    def _startTransaction(self):
        """ Start a transaction or raise a RuntimeException
        """
        if self._skip_ingest:
            # Return an invalid id
            print("skipping ingest")
            self._transaction_id = -1
            return
        success, id = self._ingest.startTransaction(self._db_name)
        if not success:
            print("ERROR Failed to start transaction ", self._db_name)
            raise RuntimeError("ERROR failed to start transaction ", self._db_name)
        self._transaction_id = id
        print("-----------------------------------------------")
        print("Transaction started ", self._db_name, "id=", id)
        return

    def _addChunkToTransaction(self, chunk_id, table, f_path):
        """ Add chunk-table file to the transaction or raise a RuntimeError.

        Parameters
        ----------
        chunk_id : int
            Chunk id number of the file to add to the transaction.
        table : str
            Name of the table in the file to add to the transaction.
        f_path : str
            The full path to file to add to the transaction.

        Return
        ------
        r_code : int
            Return code from program execution.
        out_str : str
            Output string from program execution.

        Note
        ----
        The called functions raise RuntimeErrors if they fail.
        """
        if self._skip_ingest:
            print("skipping ingest", chunk_id, table, f_path)
            return 0, 'skip'
        t_id = self._transaction_id
        host, port = self._ingest.getChunkTargetAddr(t_id, chunk_id)
        print("Sending to", host, ":", port, "info", t_id, table, chunk_id, f_path)
        r_code, out_str =self._ingest.sendChunkToTarget(host, port, t_id, table, f_path)
        print("Added to Transaction ", host, ":", port, "info", r_code, out_str)
        return r_code, out_str

    def _endTransaction(self, abort):
        """End the transaction, aborting if indicated.

        Parameters
        ----------
        abort : bool
            True if the transaction should be aborted.

        Return
        ------
        success : bool
            True if successful
        status : int
            Status value of the put action.
        content : json or None
            Information about the success or failure of the operation.
        """
        print("Transaction end abort=", abort)
        t_id = self._transaction_id
        self._transaction_id = -1
        if t_id == -1:
            print("No active transaction to end")
            return True, -1, None
        success, status, content = self._ingest.endTransaction(t_id, abort)
        return success, status, content

    def _sendIngestedChunksToServer(self, chunks_to_send):
        """Send chunk ids back to the server until the list is empty.

        Parameters
        ----------
        chunks_to_send : list of int
            List of ingested chunk id numbers to send back to the server to
            indicate that they have been ingested. The list is destroyed as
            it is sent.

        Note
        ----
        If the initial 'chunks_to_send' list is empty, it is important to send
        it to the server to indicate there was a local problem and that the
        server should abandon this connection.
        """
        while True:
            chunks_to_send = self._cl_conn.clientReportChunksComplete(chunks_to_send)
            if len(chunks_to_send) == 0:
                break

    def run(self):
        """Connect to the server and do everything until the server
        runs out of chunks for this client to generate and ingest.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self._host, self._port))
            self._cl_conn = DataGenConnection(s)
            self._cl_conn.clientReqInit()
            self._name, self._gen_arg_str, self._cfg_file_contents, ingest_dict = self._cl_conn.clientRespInit()
            print("ingest_dict=", ingest_dict)
            self._setIngest(ingest_dict)
            print("name=", self._name, self._gen_arg_str, ":\n", self._cfg_file_contents)
            print("skip_ingest=", self._skip_ingest)
            # Read the datagen config file to get access to an identical chunker and spec.
            self._readDatagenConfig()
            # Write the configuration file
            fileName = os.path.join(self._target_dir, self._cfg_file_name)
            with open(fileName, "w") as fw:
                fw.write(self._cfg_file_contents)
            # Request partioner configuration files from server.
            # The server has x number of these files, so the client will keep
            # incrementing the index and asking for a file until the server
            # responds with an empty file name.
            pCfgIndex = 0
            pCfgDict = {}
            pCfgName = "nothing"
            while not pCfgName == "":
                self._cl_conn.clientReqPartitionCfgFile(pCfgIndex)
                indx, pCfgName, pCfgContents = self._cl_conn.clientRespPartionCfgFile()
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
            pCfgDir = os.path.join(self._target_dir, self._pt_cfg_dir)
            for index, cfg_info in pCfgDict.items():
                pCfgName = os.path.join(pCfgDir, cfg_info[0])
                print("writing ", index, "name=", pCfgName)
                with open(pCfgName, "w") as fw:
                    fw.write(cfg_info[1])

            # Start creating and ingesting chunks.
            loop = True
            while loop:
                self._cl_conn.clientReqChunks(self._chunksPerReq)
                chunkListRecv, problem = self._cl_conn.clientRecvChunks()
                if problem:
                    print("WARN there was a problem with", chunkListRecv)
                chunkRecvSet = set(chunkListRecv)
                if len(chunkRecvSet) == 0:
                    # no more chunks, close the connection
                    print("No more chunks to create, exiting")
                    loop = False
                    break
                withOverlapChunks = []
                ingestedChunks = []
                # Create chunks received in the list
                createdChunks = self._createRecvChunks(chunkRecvSet)
                # Create edge only chunks as needed.
                haveAllCsvChunks = self._createNeighborChunks(createdChunks)
                # Generate overlap tables and files for ingest (happens within
                # the transaction).
                # Start the transaction
                abort = False
                if len(haveAllCsvChunks):
                    self._startTransaction()
                try:
                    for chunk in haveAllCsvChunks:
                        if self._createOverlapTables(chunk):
                            print("created overlap for chunk", chunk)
                            withOverlapChunks.append(chunk)
                        else:
                            print("ERROR failed to create overlap for chunk", chunk)
                except Exception as exc:
                    # Abort the transaction if possible.
                    print("ERROR transaction failed ", exc)
                    abort = True
                # TODO: It may be better to have the server end the transaction as
                #       it could give a better record of which chunks got ingested.
                # If no transaction was started, _endTransaction does nothing.
                success, status, content = self._endTransaction(abort)
                if success and not abort:
                    for chunk in withOverlapChunks:
                        ingestedChunks.append(chunk)
                else:
                    print("Failed ingest, transaction failed or aborted ", status, content)
                # If no chunks were created, likely fatal error. Asking for more
                # chunks to create would just cause more problems.
                if len(ingestedChunks) == 0:
                    print("ERROR no chunks were successfully ingested, ending program")
                    loop = False

                # Client sends the list of completed chunks back
                self._sendIngestedChunksToServer(ingestedChunks)


def testC():
    dgClient = DataGenClient("127.0.0.1", 13042)
    dgClient.run()


if __name__ == "__main__":
    testC()
