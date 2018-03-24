package io.sharedstreets.matcher.ingest.input;


import com.jsoniter.JsonIterator;
import io.sharedstreets.matcher.ingest.model.Point;
import io.sharedstreets.matcher.ingest.model.InputEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JsonEventExtractor {
    public static List<InputEvent> extractEvents(String value) throws IOException {

        InputEvent event = new InputEvent();
        event.point = new Point();

        JsonIterator iter = JsonIterator.parse(value);

        String eventType = null;
        Double eventValue = null;

        for (String field = iter.readObject(); field != null; field = iter.readObject()) {

            if(field.equals("timestamp"))
                event.time = iter.readLong() * 1000;
            else if(field.equals("longitude"))
                event.point.lon = Double.parseDouble(iter.readString());
            else if(field.equals("latitude"))
                event.point.lat = Double.parseDouble(iter.readString());
            else if(field.equals("id"))
                event.vehicleId = iter.readString();
            else if(field.equals("eventType"))
                eventType = iter.readString();
            else if(field.equals("eventValue"))
                eventValue = iter.readDouble();
            else
                iter.read(); // no-op
        }

        if(eventType != null && !eventType.trim().equals("")) {
            event.eventData = new HashMap<>();
            event.eventData.put(eventType, eventValue);
        }

        // single event per line
        // TODO add grouped trace in JSON format
        ArrayList list = new ArrayList<InputEvent>();
        list.add(event);

        return list;
    }
}
