package io.sharedstreets.matcher.ingest.input;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import io.sharedstreets.matcher.ingest.model.InputEvent;
import io.sharedstreets.matcher.ingest.model.Point;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class DcfhvEventExtractor {


    static Logger logger = LoggerFactory.getLogger(DcfhvEventExtractor.class);

    static FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("UTC"));

    static InputEvent parseJsonLocation(String value, boolean verbose) {

        try {
            InputEvent event = new InputEvent();
            event.point = new Point();

            JsonIterator iter = JsonIterator.parse(value);

            for (String field = iter.readObject(); field != null; field = iter.readObject()) {

                if(field.toLowerCase().equals("longitude")) {
                    if (iter.whatIsNext() == ValueType.NULL)
                        return null;
                    else
                        event.point.lon = iter.readDouble();
                }
                else if(field.toLowerCase().equals("latitude")) {
                    if(iter.whatIsNext() == ValueType.NULL)
                        return null;
                    else
                        event.point.lat = iter.readDouble();
                }

                else
                    iter.read();
            }

            return event;
        } catch (Exception e) {
            if(verbose)
                logger.error("Unable to parse line: " + value);
            return null;
        }

    }

    public static List<InputEvent> extractEvents(String value, boolean verbose) throws IOException, ParseException {

        // replace Excel quote escaping
        String cleanString = value.replace("\"{", "{").replace("}\"", "}").replace("\"\"", "\"");

        // split tab delimited lines
        String[] parts = cleanString.split("\t");

        // line parts key

//        0 = "ID"
//        1 = "AssignmentID"
//        2 = "DateCreated"
//        3 = "Milage"
//        4 = "PassengerNum"
//        5 = "StartTime"
//        6 = "DestinationID"
//        7 = "OriginID"
//        8 = "EndTime"
//        9 = "TripFareAmount"
//        10 = "ExtraFareAmount"
//        11 = "GratuityAmount"
//        12 = "SurchargeAmount"
//        13 = "TollAmount"
//        14 = "TotalAmount"
//        15 = "ClientID"
//        16 = "VehicleID"
//        17 = "DriverID"
//        18 = "Origin"
//        19 = "Destination"
//        20 = "FacecardID"
//        21 = "PVIN"
//        22 = "Status"
//        23 = "DiscountAmount"
//        24 = "FareType"
//        25 = "PaymentType"

        ArrayList list = new ArrayList<InputEvent>();

        if(parts.length > 21 && !parts[0].equals("ID")) {

            InputEvent pickupEvent = parseJsonLocation(parts[18], verbose);

            if(pickupEvent != null) {
                Date date = formatter.parse(parts[5].split("\\.")[0]);

                if (date.getDate() == 10 || date.getDate() == 11) {
                    pickupEvent.eventData = new HashMap<>();
                    pickupEvent.vehicleId = parts[21];
                    pickupEvent.time = date.getTime();
                    pickupEvent.eventData.put("pickup", null);
                    list.add(pickupEvent);
                }
                else {
                    if(verbose)
                        logger.error("Unable to parse line: " + value);

                }
            }

            InputEvent dropoffEvent = parseJsonLocation(parts[19], verbose);

            if(dropoffEvent != null) {
                Date date = formatter.parse(parts[8].split("\\.")[0]);

                if (date.getDate() == 10 || date.getDate() == 11) {
                    dropoffEvent.eventData = new HashMap<>();
                    dropoffEvent.vehicleId = parts[21];
                    dropoffEvent.time = date.getTime();
                    dropoffEvent.eventData.put("dropoff", null);
                    list.add(dropoffEvent);
                }
                else {
                    if(verbose)
                        logger.error("Unable to parse line: " + value);

                }

            }
        }
        else {
            if(verbose)
                logger.error("Unable to parse line: " + value);

        }


        return list;
    }
}
