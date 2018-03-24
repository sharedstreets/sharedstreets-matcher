package io.sharedstreets.matcher.model.aggregation;


import io.sharedstreets.matcher.model.Week;
import io.sharedstreets.matcher.model.events.SnappedEvent;
import io.sharedstreets.matcher.output.protobuf.SharedStreetsLinearReferencesProto;

import java.util.HashMap;

public class WeeklyBinnedLinearEvents {

    int numberOfBins = 1;
    double length = 0;
    double binFractionSize;
    Week week;

    // linear bin --> weekly period --> dataType + value
    HashMap<Integer, HashMap<Integer, HashMap<String,Long>>> binCountMap = new HashMap<>();
    HashMap<Integer, HashMap<Integer, HashMap<String,Double>>> binValueMap = new HashMap<>();

    public WeeklyBinnedLinearEvents(int numberOfBins, double length, Week week) {
        this.numberOfBins = numberOfBins;
        this.length = length;
        this.binFractionSize = 1.0 / this.numberOfBins;
        this.week = week;
    }

    public SharedStreetsLinearReferencesProto.SharedStreetsWeeklyBinnedLinearReferences getProtobuf(String referenceId) {

        SharedStreetsLinearReferencesProto.SharedStreetsWeeklyBinnedLinearReferences.Builder binBuilder = SharedStreetsLinearReferencesProto.SharedStreetsWeeklyBinnedLinearReferences.newBuilder();

        binBuilder.setReferenceId(referenceId);
        binBuilder.setNumberOfBins(this.numberOfBins);
        binBuilder.setReferenceLength(Math.round(this.length * 100)); // ship length as centimeteBinnedLinearEventsrs

        for(Integer bin : this.binCountMap.keySet()) {
            SharedStreetsLinearReferencesProto.BinnedPeriodicData.Builder binnedPeriodicData = SharedStreetsLinearReferencesProto.BinnedPeriodicData.newBuilder();
            for(Integer period : this.binCountMap.get(bin).keySet()) {
                SharedStreetsLinearReferencesProto.DataBin.Builder binnedData = SharedStreetsLinearReferencesProto.DataBin.newBuilder();
                for(String dataType : this.binCountMap.get(bin).get(period).keySet()) {
                    binnedData.addDataType(dataType);
                    binnedData.addCount(this.binCountMap.get(bin).get(period).get(dataType));

                    if(this.binValueMap.get(bin).get(period).containsKey(dataType))
                        binnedData.addValue(this.binValueMap.get(bin).get(period).get(dataType));
                }
                binnedPeriodicData.addPeriodOffset(period);
                binnedPeriodicData.addBins(binnedData);
            }
            binBuilder.addBinPosition(bin);
            binBuilder.addBinnedPeriodicData(binnedPeriodicData);

        }

        return binBuilder.build();
    }

    public void addEvent(int period, SnappedEvent event) {
        int bin;

        if(event.edgeFraction >= 1.0)
            bin = this.numberOfBins -1;
        else
            bin = (int)Math.floor(event.edgeFraction / this.binFractionSize);

        if(!binCountMap.containsKey(bin)) {
            binCountMap.put(bin, new HashMap<Integer,HashMap<String,Long>>());
            binValueMap.put(bin, new HashMap<Integer,HashMap<String,Double>>());
        }

        if(!binCountMap.get(bin).containsKey(period)) {
            binCountMap.get(bin).put(period, new HashMap<String,Long>());
            binValueMap.get(bin).put(period, new HashMap<String,Double>());
        }

        for(String eventType : event.eventData.keySet()) {
            if(!binCountMap.get(bin).get(period).containsKey(eventType)) {
                binCountMap.get(bin).get(period).put(eventType, new Long(0));
            }

            if(!binValueMap.get(bin).get(period).containsKey(eventType)) {
                binValueMap.get(bin).get(period).put(eventType, new Double(0));
            }

            this.binCountMap.get(bin).get(period).put(eventType, this.binCountMap.get(bin).get(period).get(eventType) + 1);

            if(event.eventData.get(eventType) != null)
                this.binValueMap.get(bin).get(period).put(eventType, this.binValueMap.get(bin).get(period).get(eventType) + event.eventData.get(eventType));
        }
    }
}
