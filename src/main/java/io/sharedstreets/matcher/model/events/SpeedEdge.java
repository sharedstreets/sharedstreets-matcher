package io.sharedstreets.matcher.model.events;


import io.sharedstreets.matcher.model.Point;

import java.io.Serializable;

public class SpeedEdge implements Serializable {

    public static int MS_PER_HOUR = 3600000;

    public long edgeId;
    public double startFaction;
    public double endFraction;

    public long time;
    public double speed;


    // TODO track observation window size (time + distance) for speed event

    @Override
    public String toString() {
        return edgeId + " " + (((double)((int)((speed * 2.23694) * 10)))/10) + "mph";
    }

    public int getHour() {
        return (int)(this.time / MS_PER_HOUR);
    }
}
