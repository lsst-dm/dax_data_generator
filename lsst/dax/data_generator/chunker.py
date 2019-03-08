
__all__ = ["Chunker", "SphericalBox"]


class SphericalBox:

    def __init__(self, lonMin, lonMax, latMin, latMax):
        self.lonMin = lonMin
        self.lonMax = lonMax
        self.latMin = latMin
        self.latMax = latMax

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "SphericalBox({:f},{:f},{:f},{:f})".format(
            self.lonMin, self.lonMax, self.latMin, self.latMax
        )


class Chunker:
    """
    This is a temporary mock for creating chunks,
    should be replaced with the real partitioning package.
    """

    def __init__(self, overlap, numStripes, numSubStripesPerStripe):
        self.overlap = overlap
        self.numStripes = numStripes
        self.numSubStripesPerStripe = numSubStripesPerStripe

    def getChunkBounds(self, chunkId):
        """
        Returns
        -------
        chunk : SphericalBox
        """
        if chunkId == 1:
            return SphericalBox(0, 0.5, 0, 0.5)
        else:
            return ValueError

    def locate(self, position):
        """
        Find the non-overlap location of the given position.
        """
        lon, lat = position
        chunk = self.getChunkBounds(1)
        result = (lon > chunk.lonMin) * (lon < chunk.lonMax)
        result *= (lat > chunk.latMin) * (lat < chunk.latMax)
        return 1*result



