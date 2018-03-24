package io.sharedstreets.matcher.model.aggregation;


import io.sharedstreets.matcher.model.Week;
import io.sharedstreets.matcher.model.events.SnappedEvent;
import io.sharedstreets.matcher.output.protobuf.SharedStreetsLinearReferencesProto;

import java.util.HashMap;

public class BinnedLinearEvents {

    int numberOfBins = 1;
    double length = 0;
    double binFractionSize;
    Week week;

    // linear bin --> dataType + value
    HashMap<Integer, HashMap<String,Double>> binMap = new HashMap<>();

    public BinnedLinearEvents(int numberOfBins, double length, Week week) {
        this.numberOfBins = numberOfBins;
        this.length = length;
        this.binFractionSize = 1.0 / this.numberOfBins;
        this.week = week;
    }

    public SharedStreetsLinearReferencesProto.SharedStreetsBinnedLinearReferences getProtobuf(String referenceId) {

        SharedStreetsLinearReferencesProto.SharedStreetsBinnedLinearReferences.Builder binBuilder = SharedStreetsLinearReferencesProto.SharedStreetsBinnedLinearReferences.newBuilder();

        binBuilder.setReferenceId(referenceId);
        binBuilder.setNumberOfBins(this.numberOfBins);
        binBuilder.setReferenceLength(Math.round(this.length * 100)); // ship length as centimeteBinnedLinearEventsrs

        for(Integer bin : this.binMap.keySet()) {
            SharedStreetsLinearReferencesProto.DataBin.Builder binnedData = SharedStreetsLinearReferencesProto.DataBin.newBuilder();
            for(String dataType : this.binMap.get(bin).keySet()){

                binnedData.addDataType(dataType);
                binnedData.addValue(this.binMap.get(bin).get(dataType));
            }
            binBuilder.addBinPosition(bin);
            binBuilder.addBins(binnedData);
        }

        return binBuilder.build();
    }

    public void addEvent(SnappedEvent event) {
        int bin;

        if(event.edgeFraction >= 1.0)
            bin = this.numberOfBins -1;
        else
            bin = (int)Math.floor(event.edgeFraction / this.binFractionSize);

        if(!binMap.containsKey(bin)) {
            binMap.put(bin, new HashMap<String,Double>());
        }

        for(String eventType : event.eventData.keySet()) {
            if(!binMap.get(bin).containsKey(eventType)) {
                binMap.get(bin).put(eventType, new Double(0));
            }

            this.binMap.get(bin).put(eventType, this.binMap.get(eventType).get(bin) + 1);
        }

    }
}
