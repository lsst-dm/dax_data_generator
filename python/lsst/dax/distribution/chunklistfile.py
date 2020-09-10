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


class ChunkListFile:
    """Read and write a set of chunks to a file.
    """

    def __init__(self, fname):
        self._fname = fname
        self.file_wopen = False
        self.chunk_set = set()

    def read(self):
        with open(self._fname, 'r') as list_file:
            f_raw = list_file.read()
        self.parse(f_raw)

    def parse(self, raw):
        """Parse the raw string for chunk numbers to put in chunk_set.
        """
        split_raw = raw.split(',')
        for st in split_raw:
            if ':' in st:
                st_split = st.split(':')
                if len(st_split) == 2:
                    val_a = int(st_split[0])
                    val_b = int(st_split[1])
                    if val_a > val_b:
                        tmp = val_a
                        val_a = val_b
                        val_b = tmp
                    for j in range(val_a, val_b + 1): #&&& probbaly a pythonic way to do this.
                        self.chunk_set.add(j)
                else:
                    raise ValueError(f"value error in st={st} {st_split}")
            elif not st or st.isspace():
                pass #ignore empty file and multiple commas in a row
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
        return '' if not aset else ','.join(str(j) for j in aset)

    def write(self):
        """Write the set to file.
        """
        if not self._fname:
            print("ChunkFileList cannot be written since it doesn't have a name.")
            return
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
                list_file.write("," + self.toStrDsk(needed))


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
        The files are appended with simple comma separated integer lists
    to make appending values simple.

    """

    def __init__(self, target, completed=None, assigned=None, limbo=None):
        # Chunks that need to be created.
        self._target    = ChunkListFile(target)
        # Chunks that were completed in a previous run.
        self._completed = ChunkListFile(completed)
        # Chunks that were assigned to workers but not registered complete.
        self._assigned  = ChunkListFile(assigned)
        # Chunks where other chunks in the group did complete.
        self._limbo     = ChunkListFile(limbo)
        self.result_set = set()

    def report(self):
        """Return a report string.
        """
        # problem_set is the set of all chunks that should be checked
        # by hand before being generated again. Once checked, problem
        # chunk Ids should be removed from assigned and limbo logs.
        problem_set = self._assigned.chunk_set.difference(self._completed.chunk_set)
        problem_set.update(self._limbo.chunk_set)
        rpt = f'Problem chunk ids:\n{problem_set}\n\n'
        rpt += f'Log counts:\n'
        rpt += f' Target:   {len(self._target.chunk_set)}\n'
        rpt += f' Assigned: {len(self._assigned.chunk_set)}\n'
        rpt += f' Completed:{len(self._completed.chunk_set)}\n'
        rpt += f' Limbo:    {len(self._limbo.chunk_set)}\n'
        rpt += f' Problem:  {len(problem_set)}'
        return rpt




    def build(self, all_valid_chunks, target_raw=None):
        """Build this object from paramters and files.

        Parameters
        ----------
        all_valid_chunks : list of int
            List of all valid chunks, used to remove invalid chunk ids
            from target.
        target_raw : str
            If not None, create _target from this string.

        Note
        ------
        self.result_set is produced here. it is a set of target chunks with
        all invalid, completed, assigned, and limbo chunks removed.
        """

        if self._target._fname:
            self._target.read()
        elif not target_raw:
            # Target is all valid chunks
            self._target.chunk_set = set(all_valid_chunks)
        else:
            # Parse provided raw string
            self._target.parse(target_raw)
        self._target.intersectWithValid(all_valid_chunks)

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

    def createOutput(self, path_header):
        """Create an output ChunkLogs object base on this one.

        Parameters
        ----------
        path_header : str
            path and prefix to give to the output files.

        Note
        ----
        It copies the contents of self chunk_set's to the new
        object's chunk_set's.
        This function does not write the files, write(self)
        needs to be called the files would be:
            path_header = "~/log/fdb_" would create files
            ~/log/fdb_target.out
            ~/log/fdb_completed.out
            ~/log/fdb_assigned.out
            ~/log/fdb_limbo.out
        """
        if path_header is None:
            path_header = ''
        newcls = ChunkLogs(
            path_header + "target.out",
            path_header + "completed.out",
            path_header + "assigned.out",
            path_header + "limbo.out")
        newcls._target.chunk_set = self._target.chunk_set.copy()
        newcls._completed.chunk_set = self._completed.chunk_set.copy()
        newcls._assigned.chunk_set = self._assigned.chunk_set.copy()
        newcls._limbo.chunk_set = self._limbo.chunk_set.copy()
        newcls.result_set = self.result_set.copy()
        return newcls

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
        self._assigned.add(chunk_ids)
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

