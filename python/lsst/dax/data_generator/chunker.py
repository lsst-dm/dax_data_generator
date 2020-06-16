
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
        # sphgeom Intervals require that A < B, or the area is empty
        latA = box.getLat().getA().asRadians()
        latB = box.getLat().getB().asRadians()
        print("&&& latA=", latA, "latB=", latB)
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
        print("&&& lonA=", lonA, "lonB=", lonB)
        lonAI = box.getLon()
        print("&&& lonAI=", lonAI)
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
        print("&&& box=", box)
        print("&&& biggerBox=", biggerBox)
        bLonADeg = box.getLon().getA().asDegrees()
        bLonBDeg = box.getLon().getB().asDegrees()
        bLatADeg = box.getLat().getA().asDegrees()
        bLatBDeg = box.getLat().getB().asDegrees()
        print("&&& box=[", bLonADeg, bLonBDeg, "], [", bLatADeg, bLatBDeg, "]")
        bLonADeg = biggerBox.getLon().getA().asDegrees()
        bLonBDeg = biggerBox.getLon().getB().asDegrees()
        bLatADeg = biggerBox.getLat().getA().asDegrees()
        bLatBDeg = biggerBox.getLat().getB().asDegrees()
        print("&&& big=[", bLonADeg, bLonBDeg, "], [", bLatADeg, bLatBDeg, "]")

        # Use
        #chunks = self.getChunksIntersecting(biggerBox)
        chunks = self.getChunksIntersecting(box)
        self.printChunkBoundsDegrees(chunkId)
        for ch in chunks: self.printChunkBoundsDegrees(ch)
        return chunks

    def printChunkBoundsDegrees(self, chunkId):
        box = self.getChunkBounds(chunkId)
        raA = box.getLon().getA().asDegrees()
        raB = box.getLon().getB().asDegrees()
        subtracted = False
        if raA > raB:
            raA -= 360.0
            subtracted = True
        decA = box.getLat().getA().asDegrees()
        decB = box.getLat().getB().asDegrees()
        print("chunk=", chunkId, "ra=", raA, raB, "Dec=", decA, decB, "sub=", subtracted)

