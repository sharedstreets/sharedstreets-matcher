package io.sharedstreets.matcher.model.aggregation;


import io.sharedstreets.matcher.model.Week;
import io.sharedstreets.matcher.output.protobuf.SharedStreetsSpeedsProto;

import java.util.HashMap;
import java.util.Map;


// stores weekly cycle of speed observations
// for details see https://github.com/sharedstreets/sharedstreets-ref-system/blob/master/proto/speeds.proto

// cycle is hardwired for hour period TODO: allow any period size

public class WeeklySpeedCycle {

    Week week;

    public HashMap<Integer, SpeedHistogram> speedsForPeriod = new HashMap<>();

    public WeeklySpeedCycle(Week week) {
        this.week = week;
    }

    public void addSpeedEvent(double speed, PeriodicTimestamp time) throws Exception {

        if(!speedsForPeriod.containsKey(time.period))
            speedsForPeriod.put(time.period, new SpeedHistogram());

        speedsForPeriod.get(time.period).addSpeed(speed, 1);
    }

    public SharedStreetsSpeedsProto.SharedStreetsWeeklySpeeds getProtobuf(String referenceId) {

        SharedStreetsSpeedsProto.SharedStreetsWeeklySpeeds.Builder weeklySpeedsBuilder = SharedStreetsSpeedsProto.SharedStreetsWeeklySpeeds.newBuilder()
                .setReferenceId(referenceId)
                .setPeriodSize(SharedStreetsSpeedsProto.PeriodSize.OneHour);

        for(Map.Entry<Integer, SpeedHistogram> entry : speedsForPeriod.entrySet()) {
            SpeedHistogram speedHistogram = entry.getValue();

            SharedStreetsSpeedsProto.SpeedHistogramByPeriod.Builder speedHistogramByPeriod = SharedStreetsSpeedsProto.SpeedHistogramByPeriod.newBuilder();

            speedHistogramByPeriod.addPeriodOffset(entry.getKey());

            SharedStreetsSpeedsProto.SpeedHistogram.Builder speedHistogramBuilder = SharedStreetsSpeedsProto.SpeedHistogram.newBuilder();

            for(Map.Entry<Integer, Integer> speedEntry : speedHistogram.speedDistribution.entrySet()) {
                speedHistogramBuilder.addSpeedBin(speedEntry.getKey());
                speedHistogramBuilder.addObservationCount(speedEntry.getValue());
            }
            speedHistogramByPeriod.addHistogram(speedHistogramBuilder);
            weeklySpeedsBuilder.addSpeedsByPeriod(speedHistogramByPeriod);
        }

        SharedStreetsSpeedsProto.SharedStreetsWeeklySpeeds speedData = weeklySpeedsBuilder.build();
        return speedData;
    }

}
