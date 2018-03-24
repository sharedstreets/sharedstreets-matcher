package io.sharedstreets.matcher.model.events;


import io.sharedstreets.barefoot.roadmap.RoadPoint;
import io.sharedstreets.matcher.model.Point;

import java.io.Serializable;
import java.util.HashMap;

public class SnappedEvent implements Serializable {

    public long edgeId;
    public double edgeFraction;
    public Point observedPoint;
    public Point matchedPoint;
    public long time;
    public double speed;
    public HashMap<String, Double> eventData;

    // capture hmm internals for calibration
    public double sequenceProbability;
    public double filterProbability;

}
