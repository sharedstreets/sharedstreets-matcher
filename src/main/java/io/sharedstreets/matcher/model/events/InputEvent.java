package io.sharedstreets.matcher.model.events;


import io.sharedstreets.matcher.input.Ingest;
import io.sharedstreets.matcher.model.Point;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InputEvent implements Serializable {

    static public HashMap<String, Long> vehicleIdStringMap  = new HashMap<>();
    static public HashMap<Long, String> vehicleIdLongMap  = new HashMap<>();
    static AtomicLong nextId = new AtomicLong(1l);

    static synchronized Long getVehicleIdFromString(String vehicleIdString) {

        if(!vehicleIdStringMap.containsKey(vehicleIdString)) {
            Long id = nextId.getAndIncrement();
            vehicleIdStringMap.put(vehicleIdString, id);
            vehicleIdLongMap.put(id, vehicleIdString);

        }

        return vehicleIdStringMap.get(vehicleIdString);
    }

    public Long vehicleId;
    public Long time;

    public Point point;
    public Point matchedPoint;

    public HashMap<String, Double> eventData;

    public boolean included;

    public Ingest.InputEventProto toProto() {

        Ingest.InputEventProto.Builder inputEventProto = Ingest.InputEventProto.newBuilder();

        inputEventProto.setTime(this.time);
        inputEventProto.setVehicleId(vehicleIdLongMap.get(this.vehicleId));
        inputEventProto.setLat(this.point.lat);
        inputEventProto.setLon(this.point.lon);

        if(this.eventData != null) {
            for(String eventType : eventData.keySet()) {
                Ingest.EventData.Builder eventDataProto = Ingest.EventData.newBuilder();

                eventDataProto.setEventType(eventType);
                if(eventData.get(eventType) != null)
                    eventDataProto.setEventValue(eventData.get(eventType));

                inputEventProto.addEventData(eventDataProto);
            }
        }

        return inputEventProto.build();
    }

    public static InputEvent fromProto(Ingest.InputEventProto inputEventProto) {
        InputEvent inputEvent = new InputEvent();
        inputEvent.time = inputEventProto.getTime();

        inputEvent.vehicleId = getVehicleIdFromString(inputEventProto.getVehicleId());
        inputEvent.point = new Point(inputEventProto.getLon(), inputEventProto.getLat());

        for(Ingest.EventData eventData : inputEventProto.getEventDataList()) {
            if(inputEvent.eventData == null)
                inputEvent.eventData = new HashMap<>();

            inputEvent.eventData.put(eventData.getEventType(), eventData.getEventValue());
        }
        return inputEvent;
    }

}