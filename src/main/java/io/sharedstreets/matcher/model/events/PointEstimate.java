package io.sharedstreets.matcher.model.events;


import io.sharedstreets.matcher.model.Point;

public class PointEstimate {

    public Point observedPoint;
    public Point matchedPoint;
    public long time;
    public double speed;
    public double sequenceprob;
    public double filterprob;
}
