
import math

from lsst import sphgeom

__all__ = ["Chunker", "SphericalBox"]


class SphericalBox:

    def __init__(self, lon_min, lon_max, lat_min, lat_max):
        self.lon_min = lon_min
        self.lon_max = lon_max
        self.lat_min = lat_min
        self.lat_max = lat_max

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "SphericalBox({:f},{:f},{:f},{:f})".format(
            self.lon_min, self.lon_max, self.lat_min, self.lat_max
        )


def boxStrDeg(box):
    lon_a_deg = str(box.getLon().getA().asDegrees())
    lon_b_deg = str(box.getLon().getB().asDegrees())
    lat_a_deg = str(box.getLat().getA().asDegrees())
    lat_b_deg = str(box.getLat().getB().asDegrees())
    s = ("ra[" + lon_a_deg + ", " +  lon_b_deg
       + "], dec[" + lat_a_deg + ", " + lat_b_deg + "]")
    return s


class Chunker:
    """
    This is a shim to the sphgeom Chunker. It may later be reasonable to
    eliminate this and have other components call sphgeom directly.
    """

    def __init__(self, overlap, num_stripes, num_sub_stripes_per_stripe):
        self.overlap = overlap
        self.num_stripes = num_stripes
        self.num_sub_stripes_per_stripe = num_sub_stripes_per_stripe
        self.chunker = sphgeom.Chunker(num_stripes, num_sub_stripes_per_stripe)

    def getChunkBounds(self, chunk_id):
        """
        Returns
        -------
        chunk : SphericalBox
        """
        stripe = self.chunker.getStripe(chunk_id)
        chunkInStripe = self.chunker.getChunk(chunk_id, stripe)
        return self.chunker.getChunkBoundingBox(stripe, chunkInStripe)

    def locate(self, position):
        """
        Find the non-overlap location of the given position.
        """
        lon, lat = position
        center = sphgeom.LonLat.fromDegrees(lon, lat)
        region = sphgeom.Box(center)
        chunks = self.chunker.getChunksIntersecting(region)
        return chunks[0]

    def getAllChunks(self):
        return self.chunker.getAllChunks()

    def getChunksIntersecting(self, region):
        return self.chunker.getChunksIntersecting(region)

    def getChunksAround(self, chunk_id, overlap_width):
        """Return a list of chunks surrounding 'chunkId' that
        should allow overlap tables to be created.

        Parameters
        ----------
        chunk_id : int
            Chunk id number
        overlap_width : float
            Overlap width in degrees.

        Return
        ------
        chunks : list of int
            List of chunk ids around 'chunk_id'. The list includes 'chunk_id'
        """
        # Increase the bounds of the box by the overlapWidth
        overlap_w_rads = overlap_width * (math.pi/180.0)
        box = self.getChunkBounds(chunk_id)
        # Increase Latitude by overlapWRads in both directions
        # cap values at PI/2 and -PI/2.
        lat_a = box.getLat().getA().asRadians()
        lat_b = box.getLat().getB().asRadians()
        if lat_a > lat_b:
            return []
        lat_a = lat_a - overlap_w_rads
        lat_b = lat_b + overlap_w_rads
        halfPi = math.pi/2.0
        if lat_a < -halfPi:
            lat_a = -halfPi
        if lat_b > halfPi:
            lat_b = halfPi

        # Increase Longitude by overlapWRads. If delta > 2PI shrint
        # increase to make it 2PI
        lon_a = box.getLon().getA().asRadians()
        lon_b = box.getLon().getB().asRadians()
        # sphgeom Intervals are supposed to have A always smaller than B,
        # but the AngleIntrvals in Box get normalized to [0, 2PI)
        two_pi = 2.0*math.pi
        if (lon_a > lon_b):
            lon_a -= two_pi
        overlap_lon = overlap_w_rads
        if ((lon_b - lon_a) + 2.0*overlap_w_rads) > two_pi:
            overlap_lon = (two_pi - (lon_b - lon_a))/2.0
        lon_a = lon_a - overlap_lon
        lon_b = lon_b + overlap_lon

        bigger_box = sphgeom.Box.fromRadians(lon_a, lat_a, lon_b, lat_b)

        # Use
        chunks = self.getChunksIntersecting(bigger_box)
        self.printChunkBoundsDegrees(chunk_id)
        for ch in chunks: self.printChunkBoundsDegrees(ch)
        return chunks

    def printChunkBoundsDegrees(self, chunk_id):
        box = self.getChunkBounds(chunk_id)
        st = boxStrDeg(box)
        print("chunk=", chunk_id, "box=", st)

