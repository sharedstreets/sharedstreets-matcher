package io.sharedstreets.matcher.ingest.input;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class JsonDTO {

    @SerializedName("eventData")
    List<EventTypeDTO> eventData;

    public class EventTypeDTO {

        @SerializedName("vehicleId")
        public String vehicleId;

        @SerializedName("timeStamp")
        public Long timeStamp;

        @SerializedName("latitude")
        public double latitude;

        @SerializedName("longitude")
        public double longitude;

        @SerializedName("eventType")
        public String eventType;
    }
}
