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
import time
import traceback
from pathlib import PurePosixPath

from .DataGenConnection import DataGenConnection
from .DataIngest import DataIngest
from lsst.dax.data_generator import DataGenerator
from lsst.dax.data_generator import TimingDict


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
    what chunks have been created and registered with the ingest system back to
    the server.
    """

    def __init__(self, host, port, retry=False, target_dir='fakeData', chunks_per_req=5):
        self._host = host
        self._port = port
        self._name = "-1"
        self._retry = retry  # Retry connection if true
        self._target_dir = os.path.abspath(target_dir)
        self._chunksPerReq = chunks_per_req
        self._gen_arg_str = None  # Arguments from the server for the generator.
        self._cl_conn = None  # DataGenConnection
        self._cfg_file_name = 'gencfg.py'  # name of the local config file for the generator
        self._cfg_file_contents = None  # contents of the config file.
        self._pt_cfg_dir = os.path.join(self._target_dir, 'partitionCfgs')  # sub-dir for partitioner configs
        self._pt_cfg_dict = None  # Dictionary that stores partioner config files.
        self._pregen_dir = os.path.join(self._target_dir, 'pregenerated')  # sub-dir for pre-generated files
        self.makeDir(self._target_dir)
        self.makeDir(self._pt_cfg_dir)
        self.makeDir(self._pregen_dir)

        # Values set from transferred self._cfgFileContents (see _readDatagenConfig)
        self._spec = None  # spec from exec(self._cfgFileContents)
        self._directors = None  # directors from exec(self._cfgFileContents)
        self._chunker = None  # chunker from exec(self._cfgFileContents)
        self._edge_width = None  # float Width of edges in edge only generation.
        # DataGenerator, cannot be initialized until '_spec' received from server
        self._data_gen = None
        self._objects = None  # int number of objects set
        self._visits = None  # int number of visits
        self._seed = None  # int random number seed

        # Ingest values
        self._ingest = None
        self._skip_ingest = True
        self._db_name = ''
        self._transaction_id = -1
        self._keep_csv = True  # keep intermediate files for debugging

        # timing information
        self._timing_dict = TimingDict()

    def _setIngest(self, ingest_dict):
        """Create ingest object from ingest_dict values.

        Parameters
        ----------
        ingest_dict : dictionary
            Dictionary containing information about the ingest system.
            'host' : str, ingest system host name.
            'port' : int, ingest port number.
            'auth' : str, ingest authorization.
            'db'   : str, name of the databse being created
            'skip' : bool, true if ingest is being skipped.
            'keep' : bool, true if intermediate files should be kept.

        Note
        ----
        The keys in ingest_dict should match those in servRespInit and clientRespInit.
        """
        ingd = ingest_dict
        self._ingest = DataIngest(ingd['host'], ingd['port'], ingd['auth'])
        self._skip_ingest = ingd['skip']
        self._db_name = ingd['db']
        self._keep_csv = ingd['keep']

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
        typeStr = "EO_" if edge_only else "CT_"
        # If the tabelName is a wildcard, don't use typeStr
        if table_name == '*':
            typeStr = ''
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
        process = subprocess.run(cmd, cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out_str = str(process.stdout)
        if process.returncode != 0:
            print("out=", out_str)
        return process.returncode, out_str

    def _readDatagenConfig(self):
        """ Create a Chunker and spec using the same configuration file as the datagen.py.
        """
        # Load the python configuration file used to generate the synthetic data.
        # spec defines tables and columns.
        # chunker defines the partitioning scheme
        # edge_width should be at least as wide as the partitioning overlap.
        spec_globals = {}
        exec(self._cfg_file_contents, spec_globals)
        assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
        assert 'directors' in spec_globals, "Specification file must define a variable 'directors'."
        assert 'chunker' in spec_globals, "Specification file must define a variable 'chunker'."
        assert 'edge_width' in spec_globals, "Specification file must define a variable 'edge_width'."
        self._spec = spec_globals['spec']
        self._directors = spec_globals['directors']
        self._chunker = spec_globals['chunker']
        self._edge_width = spec_globals['edge_width']
        print("_cfgFileContents=", self._cfg_file_contents)
        print("_spec=", self._spec)
        self._data_gen = DataGenerator(self._spec, self._chunker, pregen_dir=self._pregen_dir)

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
        # TODO: python3 has better version of glob that would make this code cleaner.
        chunkCsvPaths = glob.glob(findCsv)
        print("findCsv=", findCsv, " paths=", chunkCsvPaths)
        # Remove the path and add the files to a list
        chunkCsvFiles = []
        for fn in chunkCsvPaths:
            chunkCsvFiles.append(os.path.basename(fn))
        print("chunkCsvFiles=", chunkCsvFiles)
        # These cannot be edgeOnly, and there must be one for
        # each entry in self._spec['spec']
        for tblName in self._spec:
            if "from_file" in self._spec[tblName]:
                print("skipping pregenerated", tblName)
                continue
            fn = self.createFileName(chunk_id, tblName, 'csv', edge_only=False, use_targ_path=False)
            if fn not in chunkCsvFiles:
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

        Returns
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
                    print(f"chunk_id={chunk_id} removing fn={fn}")
                    if not self.removeFile(fn):
                        print("ERROR remove failed", fn)
                        return False
        return True

    def _fillChunkDir(self, chunk_id, neighbor_chunks):
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
        print("fillChunkDir chunkId=", chunk_id, neighbor_chunks)
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
        cList = neighbor_chunks.copy()
        if chunk_id not in cList:
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
                except OSError as err:
                    if err.errno != errno.EEXIST:
                        print("ERROR fillChunkDir link failed", fn, linkName)
                        return False
        return True

    def _datGenChunk(self, chunk_id, edge_only):
        row_counts = {"CcdVisit": self._visits, "Object": self._objects}

        # ForcedSource count is defined by visits and objects.
        if("ForcedSource" in self._spec):
            row_counts["ForcedSource"] = None

        self._data_gen.timingdict = TimingDict()
        tables = self._data_gen.make_chunk(chunk_id, edge_width=self._edge_width, edge_only=edge_only)
        self._data_gen.timingdict.increment()
        self._timing_dict.combine(self._data_gen.timingdict)
        print("tables=", tables)

        for table_name, table in tables.items():
            edge_type = "EO" if edge_only else "CT"
            fname = "chunk{:d}_{:s}_{:s}.csv".format(chunk_id, edge_type, table_name)
            fname = os.path.join(self._target_dir, fname)
            table.to_csv(fname, header=False, index=False)

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
        print(f"generateChunk chunk_id={chunk_id} edge_only={edge_only}")
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
            spec_count = 0
            for sp in spec:
                if "from_file" not in spec[sp]:
                    spec_count += 1
            print(f"spec_count={spec_count} edgeOnlyCount={edgeOnlyCount} completeCount={completeCount}")
            if completeCount == spec_count or edgeOnlyCount == spec_count:
                print("All expected tables already exist, will not generate. chunkid=", chunk_id)
                if completeCount == spec_count:
                    print("Removing extraneous edgeOnly files")
                    if not self.removeFilesForChunk(chunk_id, edge_only=True, complete=False):
                        print("WARN failed to remove extraneous csv for", chunk_id)
                else:  # Not a full set of complete files
                    print("Removing extraneous complete files")
                    if not self.removeFilesForChunk(chunk_id, edge_only=False, complete=True):
                        print("WARN failed to remove incomplete csv for", chunk_id)
                return 'exists'
        else:
            # Delete files for this chunk if they exist.
            if not self.removeFilesForChunk(chunk_id, edge_only=True, complete=True):
                print("WARN failed to remove all files for chunk=", chunk_id)
        # Genrate the chunk csv files.
        try:
            self._datGenChunk(chunk_id, edge_only)
        except IndexError as ie:
            print(f"ERROR Generator failed for {chunk_id} error={ie}")
            return 'failed'
        except RuntimeError as re:
            print(f"ERROR Generator failed for {chunk_id} error={re}")
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
                self._timing_dict.increment()  # increment the count of chunks
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

        Returns
        -------
        chunks_added : bool
            True if chunks were added to the transaction for ingest.

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
            e_path = os.path.join(ovlDir, e)
            if os.path.isfile(e_path):
                fstats = os.stat(e_path)
                if fstats.st_size > 0:
                    files.append(os.path.basename(e))
                else:
                    print(f"file {e_path} has size zero {fstats.st_size}")
            else:
                print("not a file ", os.path.join(ovlDir, e))
        if not files:
            print("No files with data were found, nothing to partition")
            return False
        # for each configuration file in self._partitionerCfgs something like this for Object chunk 0
        # sph-partition -c (cfgdir)/Object.cfg --mr.num-workers 1 --out.dir outdirObject
        # --in.path chunk0_CT_Object.csv --in.path chunk402_CT_Object.csv
        # --in.path chunk401_CT_Object.csv --in.path chunk400_CT_Object.csv
        # --in.path chunk404_EO_Object.csv --in.path chunk403_CT_Object.csv
        # Determine which tables need to be created first.
        info_list = []
        for director, children in self._directors.items():
            # Make the director table and index
            cfg = self._pt_cfg_dict[director]
            cfg_path = cfg[0]
            index_path = self._callPartitioner(chunkId, director, cfg_path, ovlDir, files, info_list)
            # create child tables using index_path
            for child in children:
                cfg = self._pt_cfg_dict[child]
                cfg_path = cfg[0]
                if not self._callPartitioner(chunkId, child, cfg_path, ovlDir, files, info_list, index_path):
                    raise RuntimeError("Error calling partitioner")

        # Add the tables to the ingest transaction
        for info in info_list:
            print("info=", info, "0=", info[0], "1=", info[1])
            st_time = self._timing_dict.start()
            self._addChunkToTransaction(chunkId, table=info[0], f_path=info[1])
            self._timing_dict.end("ingest", st_time)
        return True

    def _callPartitioner(self, chunk_id, tbl_name, cfg_fname, ovl_dir, files, info_list, index_path=None):
        """ Call sph-partition to create '.txt' files for ingest.

        Parameters
        ----------
        chunk_id : int
            Chunk id number.
        tbl_name : str
            Table name.
        cfg_fname : str
            Configuration file name
        ovl_dir : str
            Overlap directory.
        files : list of str
            List of generated csv files in the ovl_dir.
        info_list : list of str
            List of files that should be ingested.
        index_path : str (optional)
            Full path of the index.txt file that should be used to determine
            what chunk child table entries belong to.
            If this is None, the current table is a director table and an
            index file should be generated.

        Returns
        -------
        index_path : str
            The Full path to the index file created for this chunk or used to
            determine to which chunk child table rows belong.
        """
        print(f"callPartitioner {chunk_id}, {tbl_name}, {cfg_fname}")
        st_time = self._timing_dict.start()
        # The list of --in.path files needs to be generated. It
        # needs to have all the .csv files for tblName.
        inCsvFiles = []
        reg = re.compile(r"chunk\w*_" + tbl_name + r"\.csv")
        for f in files:
            m = reg.match(f)
            if m:
                inCsvFiles.append(f)
        inStr = ""
        cfgFPath = os.path.join(self._pt_cfg_dir, cfg_fname)
        outDir = os.path.join(ovl_dir, "outdir" + tbl_name)

        if not inCsvFiles:
            print(f"No files with data for table {tbl_name} were found")
            self._timing_dict.end("overlap", st_time)
            return index_path

        for csv in inCsvFiles:
            inStr += " --in.path " + csv
        # If index_path empty or undefined, this must be a director table.
        index_name = f"chunk_{tbl_name.lower()}_index.txt"
        if not index_path:
            id_url = ""
            index_path = os.path.join(outDir, index_name)
        else:
            id_url = f"--part.id-url=file://{index_path}"

        # Put the pieces of the command together and call the partitioner.
        cmd = "sph-partition -c " + cfgFPath + " --mr.num-workers 1 "
        cmd += id_url + " --out.dir " + outDir + " " + inStr
        genResult, genOut = self.runProcess(cmd, cwd=ovl_dir)
        if genResult != 0:
            # Raise exception and leave data for diagnostics.
            raise RuntimeError("ERROR failed to create chunk and overlap " + genOut + " cmd=" + cmd)
        # Delete the .txt files for files other than chunk_id
        # and chunk_index.txt in outDir.
        entries = os.listdir(outDir)
        reg = re.compile(r"^chunk_" + str(chunk_id) + r"(_overlap)?\.txt$")
        for ent in entries:
            fn = os.path.basename(ent)
            full_path = os.path.join(outDir, ent)
            m = reg.match(fn)
            if m:
                print("keeping ", fn, tbl_name, full_path)
                info_list.append((tbl_name, full_path))
            else:
                if fn == index_name:
                    print("keeping index ", full_path)
                else:
                    os.remove(full_path)
        self._timing_dict.end("overlap", st_time)
        return index_path

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
        out_str = self._ingest.sendChunkToTarget(host, port, t_id, table, f_path)
        print("Added to Transaction ", host, ":", port, "info", out_str)
        return out_str

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

    def deleteAllKeepConfig(self):
        """Since ingest is complete for this batch, delete everything
        that isn't a configuration file.
        """
        # Remove the chunk files
        path = os.path.join(self._target_dir, 'chunk*.csv')
        print("deleting ", path)
        files = glob.glob(path)
        for f in files:
            os.remove(f)
        # Remove the chunk sub directories, including their files.
        dirs = os.listdir(self._target_dir)
        for dir in dirs:
            full_path = os.path.join(self._target_dir, dir)
            if dir.isdecimal() and os.path.isdir(full_path):
                print("deleting dir", full_path)
                shutil.rmtree(full_path)

    def run(self):
        """Connect to the server and do everything until the server
        runs out of chunks for this client to generate and ingest.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"host={self._host} port={self._port}")
            connected = False
            while not connected:
                try:
                    s.connect((self._host, self._port))
                    connected = True
                except socket.error:
                    print(f"socket failed to connect {self._host}:{self._port}")
                    if not self._retry:
                        exit(1)
                    time.sleep(5)
            self._cl_conn = DataGenConnection(s)
            self._cl_conn.clientReqInit()
            cri = self._cl_conn.clientRespInit()
            self._name = cri[0]
            self._objects = cri[1]
            self._visits = cri[2]
            self._seed = cri[3]
            self._cfg_file_contents = cri[4]
            ingest_dict = cri[5]
            print("ingest_dict=", ingest_dict)
            self._setIngest(ingest_dict)
            print("cfg_file_contents:\n", self._cfg_file_contents)
            print(f'name={self._name} objects={self._objects} visits={self._visits} seed={self._seed}'
                  f'skip_ingest={self._skip_ingest}')
            # Read the datagen config file to get access to an identical chunker and spec.
            self._readDatagenConfig()
            # Write the configuration file
            fileName = os.path.join(self._target_dir, self._cfg_file_name)
            with open(fileName, "w") as fw:
                fw.write(self._cfg_file_contents)
            cfg_success, pCfgDict = self._cl_conn.clientGetFiles("partition cfg")
            if not cfg_success:
                raise RuntimeError("Client failed to receive partitioner config files.")
            self._pt_cfg_dict = {}
            for cfg in pCfgDict.items():
                cfg_fname = cfg[1][0]
                # Table name should be config name with extenstion removed.
                ext = PurePosixPath(cfg_fname).suffix
                if ext != ".cfg":
                    raise RuntimeError(f"Unexpected partitioner config file sent {cfg_fname}")
                table_name = PurePosixPath(cfg_fname).stem
                self._pt_cfg_dict[table_name] = cfg[1]
            # Write those files to the partitioner config directory
            for index, cfg_info in pCfgDict.items():
                pCfgName = os.path.join(self._pt_cfg_dir, cfg_info[0])
                print("writing config", index, "name=", pCfgName)
                with open(pCfgName, "w") as fw:
                    fw.write(cfg_info[1])

            # Read in pregenerated files
            pregen_success, pregen_dict = self._cl_conn.clientGetFiles("pregen files")
            if not pregen_success:
                raise RuntimeError("Client failed to receive pregenerated files.")
            # Write pregenerated files to their directory
            for index, file_info in pregen_dict.items():
                pregen_name = os.path.join(self._pregen_dir, file_info[0])
                print("writing pregen", index, "name=", pregen_name)
                with open(pregen_name, "w") as fw:
                    fw.write(file_info[1])

            # Start creating and ingesting chunks.
            loop = True
            while loop:
                self._cl_conn.clientReqChunks(self._chunksPerReq)
                transaction_id, chunkListRecv, problem = self._cl_conn.clientRecvChunks()
                self._transaction_id = transaction_id
                print("transaction_id = ", self._transaction_id)
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
                st_time = self._timing_dict.start()
                createdChunks = self._createRecvChunks(chunkRecvSet)
                self._timing_dict.end("primeChunks", st_time)
                st_time = self._timing_dict.start()
                # Create edge only chunks as needed.
                st_time = self._timing_dict.start()
                haveAllCsvChunks = self._createNeighborChunks(createdChunks)
                self._timing_dict.end("neighborChunks", st_time)
                # Generate overlap tables and files for ingest (happens within
                # the transaction).
                # Start the transaction
                abort = False
                try:
                    for chunk in haveAllCsvChunks:
                        self._createOverlapTables(chunk)
                        print("created overlap for chunk", chunk)
                        withOverlapChunks.append(chunk)
                except Exception as exc:
                    # Abort the transaction if possible.
                    print("ERROR transaction failed ", exc)
                    traceback.print_exc()
                    abort = True
                if not abort:
                    for chunk in withOverlapChunks:
                        ingestedChunks.append(chunk)
                else:
                    print("Failed ingest, transaction failed or aborted ")
                # If no chunks were created, likely fatal error. Asking for more
                # chunks to create would just cause more problems.
                if len(ingestedChunks) == 0:
                    print("ERROR no chunks were successfully ingested, ending program")
                    loop = False

                # client sends timing info back to server.
                print(self._timing_dict.report())
                self._cl_conn.clientReportTiming(self._timing_dict)
                self._timing_dict = TimingDict()

                # Client sends the list of completed chunks back
                self._sendIngestedChunksToServer(ingestedChunks)

                # Remove files and directories if specified
                if not self._keep_csv:
                    self.deleteAllKeepConfig()

