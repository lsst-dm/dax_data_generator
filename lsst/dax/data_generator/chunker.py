
from lsst import sphgeom

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
    This is a shim to the sphgeom Chunker. It may later be reasonable to
    eliminate this and have other components call sphgeom directly.
    """

    def __init__(self, overlap, numStripes, numSubStripesPerStripe):
        self.overlap = overlap
        self.numStripes = numStripes
        self.numSubStripesPerStripe = numSubStripesPerStripe
        self.chunker = sphgeom.Chunker(numStripes, numSubStripesPerStripe)

    def getChunkBounds(self, chunkId):
        """
        Returns
        -------
        chunk : SphericalBox
        """

        stripe = self.chunker._getStripe(chunkId)
        return self.chunker.getChunkBoundingBox(stripe, chunkId)

    def locate(self, position):
        """
        Find the non-overlap location of the given position.
        """
        lon, lat = position
        center = sphgeom.lonLat.LonLat.fromDegrees(lon, lat)
        region = sphgeom.Box(center)
        chunks = self.chunker.getChunksIntersecting(region)
        return chunks[0]



