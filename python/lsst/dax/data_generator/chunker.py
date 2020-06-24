
import math

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


def boxStrDeg(box):
    bLonADeg = str(box.getLon().getA().asDegrees())
    bLonBDeg = str(box.getLon().getB().asDegrees())
    bLatADeg = str(box.getLat().getA().asDegrees())
    bLatBDeg = str(box.getLat().getB().asDegrees())
    s = "ra[" + bLonADeg + bLonBDeg + "], dec[" + bLatADeg + bLatBDeg + "]"
    return s


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
        chunkInStripe = self.chunker._getChunk(chunkId, stripe)
        return self.chunker.getChunkBoundingBox(stripe, chunkInStripe)

    def locate(self, position):
        """
        Find the non-overlap location of the given position.
        """
        lon, lat = position
        center = sphgeom.lonLat.LonLat.fromDegrees(lon, lat)
        region = sphgeom.Box(center)
        chunks = self.chunker.getChunksIntersecting(region)
        return chunks[0]

    def getAllChunks(self):
        return self.chunker.getAllChunks()

    def getChunksIntersecting(self, region):
        return self.chunker.getChunksIntersecting(region)

    def getChunksAround(self, chunkId, overlapWidth):
        """Return a list of chunks surrounding 'chunkId' that
        should allow overlap tables to be created.
        'overlapWidth' is in degrees.
        """
        # Increase the bounds of the box by the overlapWidth
        overlapWRads = overlapWidth * (math.pi/180.0)
        box = self.getChunkBounds(chunkId)
        print("&&&!!chunkId=", chunkId, " box=", box, " overlapWRads=", overlapWRads)
        # Increase Latitude by overlapWRads in both directions
        # cap values at PI/2 and -PI/2.
        latA = box.getLat().getA().asRadians()
        latB = box.getLat().getB().asRadians()
        if latA > latB:
            return list()
        latA = latA - overlapWRads
        latB = latB + overlapWRads
        halfPi = math.pi/2.0
        if latA < -halfPi:
            latA = -halfPi
        if latB > halfPi:
            latB = halfPi

        # Increase Longitude by overlapWRads. If delta > 2PI shrint
        # increase to make it 2PI
        lonA = box.getLon().getA().asRadians()
        lonB = box.getLon().getB().asRadians()
        # sphgeom Intervals are supposed to have A always smaller than B,
        # but the AngleIntrvals in Box get normalized to [0, 2PI)
        twoPi = 2.0*math.pi
        if (lonA > lonB):
            lonA -= twoPi
        overlapLon = overlapWRads
        if ((lonB - lonA) + 2.0*overlapWRads) > twoPi:
            overlapLon = (twoPi - (lonB - lonA))/2.0
        lonA = lonA - overlapLon
        lonB = lonB + overlapLon

        biggerBox = sphgeom.Box.fromRadians(lonA, latA, lonB, latB)
        print("&&& lonA=", lonA, "latA=", latA, "lonB=", lonB, "latB=", latB)
        print("&&& box=", boxStrDeg(box))
        print("&&& big=", boxStrDeg(biggerBox))

        # Use
        chunks = self.getChunksIntersecting(biggerBox)
        # &&& chunks = self.getChunksIntersecting(box)
        self.printChunkBoundsDegrees(chunkId)
        for ch in chunks: self.printChunkBoundsDegrees(ch)
        return chunks

    def printChunkBoundsDegrees(self, chunkId):
        box = self.getChunkBounds(chunkId)
        st = boxStrDeg(box)
        print("chunk=", chunkId, "box=", st)

