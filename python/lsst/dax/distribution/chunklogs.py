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

import os


class ChunkListFile:
    """Read and write a set of chunks to a file.
    """

    def __init__(self, fname):
        self._fname = fname
        if self._fname:
            self._fname = os.path.expanduser(self._fname)
            self._fname = os.path.abspath(self._fname)
        self.file_wopen = False
        self.chunk_set = set()

    def read(self):
        with open(self._fname, 'r') as list_file:
            f_raw = list_file.read()
        self.parse(f_raw)

    def parse(self, raw, separator='\n'):
        """Parse the raw string for chunk numbers to put in chunk_set.

        Parameters
        ----------
        raw : str
            String to parse for chunk id numbers.
        separator : str optional
            Character or string used to separate id numbers. The normal
            separator is '\n' but ',' is more appropriate for strings
            entered on the command line.

        Note
        ----
        Extra separators are ignored.
        """
        split_raw = raw.split(separator)
        for st in split_raw:
            if ':' in st:
                st_split = st.split(':')
                if len(st_split) != 2:
                    raise ValueError(f"value error in st={st} {st_split}")
                val_a = int(st_split[0])
                val_b = int(st_split[1])
                if val_a > val_b:
                    tmp = val_a
                    val_a = val_b
                    val_b = tmp
                for j in range(val_a, val_b + 1):
                    self.chunk_set.add(j)
            elif not st or st.isspace():
                # ignore empty file and multiple separator in a row
                pass
            else:
                val = int(st)
                self.chunk_set.add(val)

    def intersectWithValid(self, valid_ids):
        """Remove all elements from chunk set that are not in valid_ids.

        Paramters
        ---------
        valid_ids : list of int
            List of valid chunk id numbers.

        Note
        ----
        Partioning scheme chunks are not contiguous, so this function is used
        to remove invalid chunks from the chunk set.
        """
        set_valid = set(valid_ids)
        self.chunk_set.intersection_update(set_valid)

    def toStr(self):
        """Return a string of self.chunk_set to write to disk.
        """
        return self.toStrDsk(self.chunk_set)

    def toStrDsk(self, aset):
        """Return a string of aset to write to disk.
        """
        return '' if not aset else '\n'.join(str(j) for j in aset)

    def write(self):
        """Write the set to file, overwriting previous file.
        """
        if not self._fname:
            print("ChunkFileList cannot be written since it doesn't have a name.")
            return
        print(f"self._fname {self._fname}")
        with open(self._fname, 'w') as list_file:
            list_file.write(self.toStr())
        self.file_wopen = True

    def add(self, chunk_ids):
        """Add chunk_ids to the set, if it was not already in the set append to the file.

        Parameters
        ----------
        chunk_ids : list of ints
            Chunk id numbers to add to the set and possibly append to the file.
        """
        needed = [id for id in chunk_ids if id not in self.chunk_set]

        self.chunk_set.update(needed)

        # Only write to file if a file has already been opened for writing.
        if self.file_wopen:
            with open(self._fname, 'a') as list_file:
                list_file.write('\n' + self.toStrDsk(needed))


class ChunkLogs:
    """This class is used to create the list of chunks to generate
    and to track which chunks have been completed and which chunks have
    had problems.
        The inputs can either be from the command line indicating the
    range of desired chunks or from file(s). Using the range will
    cause the result set to contain all valid chunk ids in that range
    as partitioning schemes are not contiguous.
        This class is expected to generate a series of output files
    (essentially logs). These files can be used as inputs to this class,
    allowing a new instance of the program to continue from the
    previous state.
        The files are appended with simple '\n' separated integer lists
    to make appending values simple. Extra '\n' are ignored.

    Parameters
    ----------
    target : str
        Name of target set input file, can be None. If None, the target
        set of chunk ids will include all valid chunk ids. 'raw' can
        modify the input set from target.
    completed : str
        Name of completed set input file, can be None or empty. All the
        chunk ids in this file has been completed and doesn't need to
        be generated again.
    assigned : str
        Name of assigned set input file, can be None or empty. All the
        chunk ids in this file were assigned to clients to be created.
        Any file in the assigned list and not in the completed list had
        an issue being generated and needs to be removed from this file,
        and possibly the limbo file, by hand before being generated.
    limbo : str
        Name of limbo set input file, can be None or empty. All the
        chunk ids where the client could not generate or register the
        chunk are placed here. These need to be checked by hand before
        trying to generate them. Once checked, they also need to be
        removed from the assigned file.
    raw : str
        Arguments to generate target chunks from a string such as
        '0:1000,4321,6832` which would be all valid chunks from
        0 to 1000 (inclusive), 4321, and 6832.
    """

    def __init__(self, target, completed=None, assigned=None, limbo=None, raw=None):
        # Chunks that need to be created.
        self._target = ChunkListFile(target)
        # Chunks that were completed in a previous run.
        self._completed = ChunkListFile(completed)
        # Chunks that were assigned to workers but not registered complete.
        self._assigned = ChunkListFile(assigned)
        # Chunks where other chunks in the group did complete.
        self._limbo = ChunkListFile(limbo)
        # raw text list from command line
        self._target_raw = raw

        # self.build() will use this the above information to create
        # the result_set, which includes all chunk ids that need
        # to be created.
        self.result_set = set()

    def report(self):
        """Return a report string.
        """
        # problem_set is the set of all chunks that should be checked
        # by hand before being generated again. Once checked, problem
        # chunk Ids should be removed from assigned and limbo logs.
        problem_set = self._assigned.chunk_set.difference(self._completed.chunk_set)
        problem_set.update(self._limbo.chunk_set)
        notstarted_set = self._target.chunk_set.difference(self._completed.chunk_set)
        notstarted_set.difference_update(problem_set)
        rpt = f'Problem chunk ids:\n{problem_set}\n\n'
        rpt += f'Log counts:\n'
        rpt += f' Target:     {len(self._target.chunk_set)}\n'
        rpt += f' Assigned:   {len(self._assigned.chunk_set)}\n'
        rpt += f' Completed:  {len(self._completed.chunk_set)}\n'
        rpt += f' Limbo:      {len(self._limbo.chunk_set)}\n\n'
        rpt += f' Problem:    {len(problem_set)}\n'
        rpt += f' not started:{len(notstarted_set)}'
        return rpt

    def build(self, all_valid_chunks):
        """Build this object from paramters and files.

        Parameters
        ----------
        all_valid_chunks : list of int
            List of all valid chunks, used to remove invalid chunk ids
            from target.

        Note
        ------
        self.result_set is produced here. it is a set of target chunks with
        all invalid, completed, assigned, and limbo chunks removed.
        """

        # Handle raw string input
        raw_in = None
        if self._target_raw:
            raw_in = ChunkListFile(None)
            # Use comma for separator since this came from the command line.
            raw_in.parse(self._target_raw, ',')

        if self._target._fname:
            # Read the target file. If raw text input
            # was provided, use the intersection of that and the
            # target file.
            self._target.read()
            if raw_in:
                self._target.chunk_set.intersection_update(raw_in.chunk_set)
        elif raw_in:
            # Since there's no target file, use provided raw string by itself.
            self._target = raw_in
        else:
            # Nothing provided by user, the target is all valid chunks.
            self._target.chunk_set = set(all_valid_chunks)
        # Make sure no invalid chunks are in the target set.
        if all_valid_chunks is not None:
            self._target.intersectWithValid(all_valid_chunks)

        # result_set is the target set with all chunks found in
        # complete, assigned, and limbo removed. Technically
        # this could just be target - assigned, but users
        # are expected to edit the files and ingesting the
        # same chunk twice could be bad.
        self.result_set = self._target.chunk_set.copy()

        # Remove chunks from result_set if they are in completed,
        # assigned, or limbo.
        lst = [self._completed, self._assigned, self._limbo]
        for item in lst:
            if item._fname:
                item.read()
                self.result_set.difference_update(item.chunk_set)
        return

    def write(self):
        """Write the output files to disk.
        """
        lst = [self._target, self._completed, self._assigned, self._limbo]
        for item in lst:
            item.write()

    @staticmethod
    def createNames(path_header):
        """Create chunk log file names

        Parameters
        ----------
        path_header : str
            path to give to the output files.

        Return
        ------
        target : str
            Target file name
        completed : str
            Completed file name
        assigned : str
            Assigned file name
        limbo : str
            Limbo file name
        """
        if path_header is None:
            path_header = ''
        target = os.path.join(path_header, "target.clg")
        completed = os.path.join(path_header, "completed.clg")
        assigned = os.path.join(path_header, "assigned.clg")
        limbo = os.path.join(path_header, "limbo.clg")
        return target, completed, assigned, limbo

    def createOutput(self, path_header):
        """Create an output ChunkLogs object base on this one.

        Parameters
        ----------
        path_header : str
            path to give to the output files.

        Note
        ----
        It copies the contents of self chunk_set's to the new
        object's chunk_set's.
        This function does not write the files, write(self)
        needs to be called the files would be:
            path_header = "~/log/" would create files
            ~/log/target.clg
            ~/log/completed.clg
            ~/log/assigned.clg
            ~/log/limbo.clg
        """
        if path_header is None:
            path_header = ''
        targf, compf, assif, limbf = ChunkLogs.createNames(path_header)
        logs_out = ChunkLogs(targf, compf, assif, limbf)
        logs_out._target.chunk_set = self._target.chunk_set.copy()
        logs_out._completed.chunk_set = self._completed.chunk_set.copy()
        logs_out._assigned.chunk_set = self._assigned.chunk_set.copy()
        logs_out._limbo.chunk_set = self._limbo.chunk_set.copy()
        logs_out.result_set = self.result_set.copy()
        return logs_out

    @staticmethod
    def checkFiles(in_dir):
        """Check if the files exist in in_dir.

        Parameters
        ----------
        in_dir : str
            The directory where input log files should be found.

        Returns
        -------
        targf : str
            Target set log file name.
        compf : str
            Completed set log file name (may be None).
        assif: str
            Assigned set log file name (may be None).
        limbf : str
            Limbo set log file name (may be None).
        If directory or target file don't exist, raise FileNotFound.
        """
        in_dir = os.path.expanduser(in_dir)
        in_dir = os.path.abspath(in_dir)
        targf, compf, assif, limbf = ChunkLogs.createNames(in_dir)
        # Check if in_dir exists
        if not os.path.exists(in_dir):
            raise FileNotFoundError(in_dir)
        # Check if targf exists
        if os.path.exists(targf):
            print("found target file {targf}")
        else:
            raise FileNotFoundError(targf)
        lst = [compf, assif, limbf]
        for f in lst:
            if os.path.exists(f):
                print(f"found {f}")
            else:
                print(f"not found {f} setting to None")
                f = None
        return targf, compf, assif, limbf

    def addAssigned(self, chunk_ids):
        """Add chunk_ids to assigned chunk set.

        Parameters
        ----------
        chunk_ids : list of int
            List of chunk ids to add.

        Note
        ----
        These will be written to disk if the file already started.
        """
        self._assigned.add(chunk_ids)

    def addCompleted(self, chunk_ids):
        """Add chunk_ids to completed chunk set.

        Parameters
        ----------
        chunk_ids : list of int
            List of chunk ids to add.

        Note
        ----
        These will be written to disk if the file already started.
        """
        self._completed.add(chunk_ids)

    def addLimbo(self, chunk_ids):
        """Add chunk_ids to limbo chunk set.

        Parameters
        ----------
        chunk_ids : list of int
            List of chunk ids to add.

        Note
        ----
        These will be written to disk if the file already started.
        """
        self._limbo.add(chunk_ids)

