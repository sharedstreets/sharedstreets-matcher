package io.sharedstreets.matcher.model.aggregation;


import java.util.*;

public class SpeedHistogram {

    // speed count map -- bins are in km/h
    public HashMap<Integer, Integer> speedDistribution = new HashMap<>();

    @Override
    public String toString() {
        return (this.meanSpeed() * 0.621371) + " mph";
    }

    public void addSpeed(double speed, int count) {
        // convert speed to km/h and round to int for binning
        int speedKmh = (int)Math.round(speed * 3.6);

        if(!this.speedDistribution.containsKey(speedKmh))
            this.speedDistribution.put(speedKmh, 0);

        this.speedDistribution.put(speedKmh, this.speedDistribution.get(speedKmh) + count);
    }

    public double meanSpeed() {

        double weightedSpeedSum = 0;
        double speedCount = 0;

        for(Map.Entry<Integer,Integer> entry : this.speedDistribution.entrySet()) {
            weightedSpeedSum += entry.getKey() * entry.getValue();
            speedCount += entry.getValue();
        }

        return weightedSpeedSum / speedCount;
    }

    public void merge(SpeedHistogram aggregatedSpeeds2) {

        for(int speedBin : aggregatedSpeeds2.speedDistribution.keySet()){
            if(!this.speedDistribution.containsKey(speedBin))
                this.speedDistribution.put(speedBin, 0);

            this.speedDistribution.put(speedBin, this.speedDistribution.get(speedBin) + aggregatedSpeeds2.speedDistribution.get(speedBin));
        }
    }
}
