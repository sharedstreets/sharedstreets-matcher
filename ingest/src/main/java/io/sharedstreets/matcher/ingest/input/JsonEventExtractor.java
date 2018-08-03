package io.sharedstreets.matcher.ingest.input;


import com.jsoniter.JsonIterator;
import io.sharedstreets.matcher.ingest.model.Point;
import io.sharedstreets.matcher.ingest.model.InputEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JsonEventExtractor {

    static Logger logger = LoggerFactory.getLogger(JsonEventExtractor.class);

    public static List<InputEvent> extractEvents(String value, boolean verbose) throws IOException {

        ArrayList list = new ArrayList<InputEvent>();

        try {

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

            list.add(event);
        }
        catch(Exception e) {

            logger.error("Unable to parse line: " + value);

        }

        return list;
    }
}
