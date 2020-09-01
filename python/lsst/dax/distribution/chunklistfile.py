
import file



class ChunkListFile:
    """Read and write a set of chunks to a file.
    """

    def __init__(self, fname):
        self._fname = fname
        self.chunk_set = set()

    def read(self):
        with open(self._fname, 'r') as list_file:
            f_raw = list_file.read()
        self._parse(f_raw)

    def _parse(self, raw):
        split_raw = raw.split(',')
        for s in split_raw:
            #&&& catch invalid entries, per 's'
            if ':' in s:
                s_split = s.split(':')
                if len(s_split) == 2:
                    val_a = int(s_split[0])
                    val_b = int(s_split[1])
                    if val_a > val_b:
                        tmp = val_a
                        val_a = val_b
                        val_b = tmp
                    for j in range(val_a, val_b + 1):
                        self.chunk_set.add(j)
                else:
                    pass #&&& invalid entry
            else:
                val = int(s)
                self.chunk_set.add(val)
