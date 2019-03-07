package io.sharedstreets.matcher.ingest.model;

import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.List;

public class JsonInputObject {

    @SerializedName("eventData")
    public List<JsonEventObject> eventData;

    public class JsonEventObject {

        @SerializedName("vehicleId")
        public String vehicleId;

        @SerializedName("timeStamp")
        public Long timeStamp;

        @SerializedName("latitude")
        public double latitude;

        @SerializedName("longitude")
        public double longitude;

        @SerializedName("eventType")
        public HashMap<String, Double> eventType;
    }
}
