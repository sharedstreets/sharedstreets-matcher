package io.sharedstreets.matcher.ingest.input.gpx;

import com.google.gson.annotations.SerializedName;

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