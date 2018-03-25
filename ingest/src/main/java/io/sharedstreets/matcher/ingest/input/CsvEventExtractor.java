package io.sharedstreets.matcher.ingest.input;


import io.sharedstreets.matcher.ingest.model.Point;
import io.sharedstreets.matcher.ingest.model.InputEvent;
import org.joda.time.DateTime;
import sun.nio.ch.IOStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CsvEventExtractor {
    public static List<InputEvent> extractEvents(String value) throws IOException {

        ArrayList list = new ArrayList<InputEvent>();

        InputEvent event = new InputEvent();
        event.point = new Point();

        String splitValue[] = value.split(",");

        if( splitValue.length >= 4 ) {


            event.vehicleId = splitValue[0];

            try {
                event.time = Long.parseLong(splitValue[1]);
            } catch (Exception e) {
                // not a number fallback to date string
            }
            if(event.time == null) {
                try {
                    event.time =  new DateTime(splitValue[1]).getMillis();
                } catch (Exception e) {
                    // not a date
                    throw new IOException("Unable to parse data value");
                }
            }
            event.point.lat = Double.parseDouble(splitValue[2]);
            event.point.lon = Double.parseDouble(splitValue[3]);

            if(splitValue.length >= 5 && !splitValue[4].trim().equals("")) {
                event.eventData = new HashMap<>();

                Double eventValue = 0.0;

                if(splitValue.length >= 6 && !splitValue[5].trim().equals(""))
                    eventValue = Double.parseDouble(splitValue[5]);

                event.eventData.put(splitValue[4], eventValue);
            }

            // single event per line
            list.add(event);
        }



        return list;
    }
}
