package io.sharedstreets.matcher.model.input.dcfhv;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import io.sharedstreets.matcher.model.events.InputEvent;
import io.sharedstreets.matcher.model.Point;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class DcfhvEventExtractor {

    static FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    static InputEvent parseJsonLocation(String value) {

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
            return null;
        }

    }

    public static List<InputEvent> extractEvents(String value) throws IOException, ParseException {

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

        if(!parts[0].equals("ID")) {

            InputEvent pickupEvent = parseJsonLocation(parts[18]);

            if(pickupEvent != null) {
                pickupEvent.vehicleId = parts[21];
                pickupEvent.time = formatter.parse(parts[5].split("\\.")[0]).getTime();
                pickupEvent.eventType = "PICKUP";
                list.add(pickupEvent);
            }

            InputEvent dropoffEvent = parseJsonLocation(parts[19]);

            if(dropoffEvent != null) {
                dropoffEvent.vehicleId = parts[21];
                dropoffEvent.time = formatter.parse(parts[8].split("\\.")[0]).getTime();
                dropoffEvent.eventType = "DROPOFF";
                list.add(dropoffEvent);
            }
        }


        return list;
    }
}
