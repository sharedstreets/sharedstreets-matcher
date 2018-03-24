package io.sharedstreets.matcher.output.tiles;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TileId implements Comparable {

    private static final int TILE_SIZE = 256;
    private static final double MIN_LAT = -85.05112878;
    private static final double MAX_LAT = 85.05112878;
    private static final double MIN_LON = -180;
    private static final double MAX_LON = 180;
    private static int mMaxZoomLevel = 22;

    public int z;
    public int x;
    public int y;

    transient private int hashCode;

    public static TileId lonLatToTileId(int z, double lon, double lat) {

        lat = clip(lat, MIN_LAT, MAX_LAT);
        lon = clip(lon, MIN_LON, MAX_LON);

        final double x = (lon + 180) / 360;
        final double sinLatitude = Math.sin(lat * Math.PI / 180);
        final double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

        final int mapSize = TILE_SIZE << (z < mMaxZoomLevel ? z : mMaxZoomLevel);

        int px = (int) clip(x * mapSize + 0.5, 0, mapSize - 1);
        int py = (int) clip(y * mapSize + 0.5, 0, mapSize - 1);

        TileId tileId = new TileId();
        tileId.x = px / TILE_SIZE;
        tileId.y = py / TILE_SIZE;
        tileId.z = z;

        return tileId;
    }

    private static double clip(final double n, final double minValue, final double maxValue) {
        return Math.min(Math.max(n, minValue), maxValue);
    }

    public String toString() {
        return z + "-" + x + "-" + y;
    }

    @Override
    public int hashCode() {
        if(hashCode == 0)
            hashCode = new HashCodeBuilder(17, 31).append(z).append(x).append(y).hashCode();
        return hashCode;
    }


    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TileId))
            return false;
        if (obj == this)
            return true;

        TileId id = (TileId)obj;
        return this.x == id.x && this.y == id.y && this.z == id.z;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof TileId))
            return -1;
        if (o == this)
            return 0;

        TileId id = (TileId)o;

        if(this.z == id.z) {
            if(this.x == id.x) {
                if(this.y == id.y)
                    return 0;
                else if(this.y < id.y) {
                    return 1;
                }
                else
                    return -1;
            }
            else {
                if(this.x < id.x)
                    return 1;
                else
                    return -1;
            }
        }
        else {
            if(this.z > id.z)
                return 1;
            else
                return -1;
        }
    }
}
