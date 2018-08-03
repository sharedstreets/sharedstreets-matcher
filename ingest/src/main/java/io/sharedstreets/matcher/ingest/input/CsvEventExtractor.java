package io.sharedstreets.matcher.ingest.input;


import io.sharedstreets.matcher.ingest.Ingester;
import io.sharedstreets.matcher.ingest.model.Point;
import io.sharedstreets.matcher.ingest.model.InputEvent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.IOStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CsvEventExtractor {

    static Logger logger = LoggerFactory.getLogger(CsvEventExtractor.class);
    public static List<InputEvent> extractEvents(String value, boolean verbose) throws IOException {

        ArrayList list = new ArrayList<InputEvent>();

        InputEvent event = new InputEvent();
        event.point = new Point();

        String splitValue[] = value.split(",");

        if( splitValue.length >= 4 ) {


            event.vehicleId = splitValue[0];

            try {
                event.time = Long.parseLong(splitValue[1]);
            } catch (Exception e1) {
                // not a number fallback to date string
                if(event.time == null) {
                    try {
                        event.time =  new DateTime(splitValue[1]).getMillis();
                    } catch (Exception e2) {
                        // not a date
                        if(verbose)
                            logger.error("Unable to parse date value" + splitValue[1]);
                        throw new IOException("Unable to parse date value" + splitValue[1]);
                    }
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
        else {
            if(verbose) {
                logger.error("Unable to parse line, only "  + splitValue.length + " fields: " + value);
            }
        }



        return list;
    }
}
