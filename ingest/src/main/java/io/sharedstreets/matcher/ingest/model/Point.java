package io.sharedstreets.matcher.ingest.model;


public class Point {

    public double lon;
    public double lat;

    public Point() {
        lon = 0.0;
        lat = 0.0;
    }

    public Point(double lon, double lat) {
        this.lon = lon;
        this.lat = lat;
    }
}
