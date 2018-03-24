package io.sharedstreets.matcher.ingest.input;


import io.sharedstreets.matcher.ingest.model.Point;
import io.sharedstreets.matcher.ingest.model.InputEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CsvEventExtractor {
    public static List<InputEvent> extractEvents(String value) throws IOException {

        InputEvent event = new InputEvent();
        event.point = new Point();

        String splitValue[] = value.split(",");

        if( splitValue.length == 6 ) {

            event.time = Long.parseLong(splitValue[0]);
            event.vehicleId = splitValue[1];
            event.point.lat = Double.parseDouble(splitValue[2]);
            event.point.lon = Double.parseDouble(splitValue[3]);

            if(!splitValue[4].trim().equals("")) {
                event.eventData = new HashMap<>();

                Double eventValue = 0.0;

                if(!splitValue[5].trim().equals(""))
                    eventValue = Double.parseDouble(splitValue[5]);

                event.eventData.put(splitValue[4], eventValue);
            }
        }

        // single event per line
        ArrayList list = new ArrayList<InputEvent>();
        list.add(event);

        return list;
    }
}
