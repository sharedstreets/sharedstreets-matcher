package io.sharedstreets.matcher.model.events;


import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polyline;
import com.jsoniter.output.JsonStream;
import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.matcher.MatcherFactory;
import io.sharedstreets.matcher.model.Point;
import io.sharedstreets.matcher.output.json.GeoJSONData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MatchOutput extends GeoJSONData {

    Long lastEdgeId = null;
    Double lastEdgeFraction = null;

    public List<SpeedEdge> speedEvents = new ArrayList<>();
    public List<SnappedEvent> snappedEvents = new ArrayList<>();
    public List<MatchFailure> matchFailures = new ArrayList<>();

    public List<Point> failedPoints = new ArrayList<>();

    //public List<InputEvent> inputEvents = new ArrayList<>();
    public List<PointEstimate> pointEstimates = new ArrayList<>();

    public MatchOutput () {
        this.type = "trace";
    }


    void addGeoJSONPointPair(JsonStream jsonStream, Point matchedPoint, Point observedPoint, String color, HashMap<String, Object> params)  throws IOException{
        jsonStream.writeObjectStart();

        jsonStream.writeObjectField("type");
        jsonStream.writeVal("Feature");
        jsonStream.writeMore();

        jsonStream.writeObjectField("properties");
        jsonStream.writeObjectStart();

        jsonStream.writeObjectField("stroke");
        jsonStream.writeVal(color);


        for(String name : params.keySet() ){
            jsonStream.writeMore();

            jsonStream.writeObjectField(name);
            jsonStream.writeVal(params.get(name));
        }


        jsonStream.writeObjectEnd();
        jsonStream.writeMore();

        jsonStream.writeObjectField("geometry");

        jsonStream.writeObjectStart();

        jsonStream.writeObjectField("type");
        jsonStream.writeVal("LineString");
        jsonStream.writeMore();

        jsonStream.writeObjectField("coordinates");
        jsonStream.writeArrayStart();

        jsonStream.writeArrayStart();
        jsonStream.writeVal(observedPoint.lon);
        jsonStream.writeMore();
        jsonStream.writeVal(observedPoint.lat);
        jsonStream.writeArrayEnd();

        jsonStream.writeMore();

        jsonStream.writeArrayStart();
        jsonStream.writeVal(matchedPoint.lon);
        jsonStream.writeMore();
        jsonStream.writeVal(matchedPoint.lat);
        jsonStream.writeArrayEnd();

        jsonStream.writeArrayEnd();
        jsonStream.writeObjectEnd();

        jsonStream.writeObjectEnd();
    }

    void addGeoJSONPoint(JsonStream jsonStream, Point point,  String color, HashMap<String, Object> params)  throws IOException{
        jsonStream.writeObjectStart();

            jsonStream.writeObjectField("type");
            jsonStream.writeVal("Feature");

            jsonStream.writeMore();

            jsonStream.writeObjectField("properties");
            jsonStream.writeObjectStart();

            jsonStream.writeObjectField("stroke");
            jsonStream.writeVal(color);


            for(String name : params.keySet() ){
                jsonStream.writeMore();

                jsonStream.writeObjectField(name);
                jsonStream.writeVal(params.get(name));
            }


        jsonStream.writeObjectEnd();
        jsonStream.writeMore();

        jsonStream.writeObjectField("geometry");

            jsonStream.writeObjectStart();

                jsonStream.writeObjectField("type");
                jsonStream.writeVal("Point");
                jsonStream.writeMore();

                jsonStream.writeObjectField("coordinates");

                    jsonStream.writeArrayStart();
                    jsonStream.writeVal(point.lon);
                    jsonStream.writeMore();
                    jsonStream.writeVal(point.lat);
                    jsonStream.writeArrayEnd();

            jsonStream.writeObjectEnd();

        jsonStream.writeObjectEnd();
    }

    void addGeoJSONEdge(JsonStream jsonStream, Polyline edge, String color, HashMap<String, Object> params)  throws IOException{
        jsonStream.writeObjectStart();

        jsonStream.writeObjectField("type");
        jsonStream.writeVal("Feature");
        jsonStream.writeMore();

        jsonStream.writeObjectField("properties");
        jsonStream.writeObjectStart();

        jsonStream.writeObjectField("stroke");
        jsonStream.writeVal(color);

        jsonStream.writeMore();

        jsonStream.writeObjectField("stroke-weight");
        jsonStream.writeVal(5.0);


        for(String name : params.keySet() ){
            jsonStream.writeMore();

            jsonStream.writeObjectField(name);
            jsonStream.writeVal(params.get(name));
        }


        jsonStream.writeObjectEnd();
        jsonStream.writeMore();

        jsonStream.writeObjectField("geometry");

        jsonStream.writeObjectStart();

        jsonStream.writeObjectField("type");
        jsonStream.writeVal("LineString");
        jsonStream.writeMore();

        jsonStream.writeObjectField("coordinates");
        jsonStream.writeArrayStart();

        boolean firstCoord = true;
        for(Point2D point : edge.getCoordinates2D()) {

            if(!firstCoord)
                jsonStream.writeMore();

            firstCoord = false;

            jsonStream.writeArrayStart();
            jsonStream.writeVal(point.x);
            jsonStream.writeMore();
            jsonStream.writeVal(point.y);
            jsonStream.writeArrayEnd();
        }

        jsonStream.writeArrayEnd();

        jsonStream.writeObjectEnd();

        jsonStream.writeObjectEnd();
    }

    @Override
    public String toGeoJSON() throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        JsonStream jsonStream = new JsonStream(outputStream, 32);

        jsonStream.writeObjectStart();

        jsonStream.writeObjectField("type");
        jsonStream.writeVal("FeatureCollection");
        jsonStream.writeMore();


        jsonStream.writeObjectField("features");
        jsonStream.writeArrayStart();

        boolean firstPoint = true;

        double lastSequenceprob = 0;

        for(PointEstimate estimate :  this.pointEstimates) {

            if(!firstPoint)
                jsonStream.writeMore();

            HashMap<String, Object> params = new HashMap();

            params.put("sequenceprob", estimate.sequenceprob);
            params.put("filterprob", estimate.filterprob);
            params.put("speed", estimate.speed);
            params.put("time", estimate.time);


            String color = "#179bf2";

            if(lastSequenceprob < estimate.sequenceprob)
                lastSequenceprob = 0;

            if((estimate.sequenceprob - lastSequenceprob) < MatcherFactory.seqProbDelta)
                color = "#ff0000";


            addGeoJSONPointPair(jsonStream, estimate.matchedPoint, estimate.observedPoint, color, params);

            lastSequenceprob = estimate.sequenceprob;
            firstPoint = false;
        }

        String colors[] = {"#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#fee090", "#fdae61", "#f46d43", "#d73027"};


        for(SpeedEdge speedEdge : this.speedEvents) {

            if(!firstPoint)
                jsonStream.writeMore();

            Polyline geom = BaseRoad.IDs.getGeom(speedEdge.edgeId);

            HashMap<String, Object> params = new HashMap();
            params.put("speed", speedEdge.speed);

            double speed = speedEdge.speed * 3.6;

            int colorBin = (int)Math.round(Math.ceil(speed / (60 / 8))) - 1;
            if (colorBin > 7)
                colorBin = 7;
            if (colorBin < 0)
                colorBin = 0;

            addGeoJSONEdge(jsonStream, geom, colors[colorBin], params);

            firstPoint = false;
        }


        for(SnappedEvent snappedEvent : this.snappedEvents) {

            if(!firstPoint)
                jsonStream.writeMore();


            HashMap<String, Object> params = new HashMap();

            for(String eventType : snappedEvent.eventData.keySet()) {
                params.put(eventType, snappedEvent.eventData.get(eventType));
            }

            params.put("speed", snappedEvent.speed);
            params.put("sequenceprob", snappedEvent.sequenceProbability);
            params.put("filterprob", snappedEvent.filterProbability);

            addGeoJSONPoint(jsonStream, snappedEvent.matchedPoint, "#0000ff", params);

            jsonStream.writeMore();

            addGeoJSONPoint(jsonStream, snappedEvent.observedPoint, "#ff00ff", params);

            firstPoint = false;
        }

        jsonStream.writeArrayEnd();
        jsonStream.writeObjectEnd();

        jsonStream.flush();


        return new String(outputStream.toByteArray());
    }


    public void updateLastEdge(Long edgeId, Double edgeFraction) {

        if(failedPoints.size() > 0) {

            MatchFailure matchFailure = new MatchFailure();
            matchFailure.failedPoints = new ArrayList<>(this.failedPoints);

            matchFailure.startEdgeId = this.lastEdgeId;
            matchFailure.startEdgeFraction = this.lastEdgeFraction;

            matchFailure.endEdgeId = edgeId;
            matchFailure.endEdgeFraction = edgeFraction;

            matchFailures.add(matchFailure);

            this.failedPoints.clear();
        }

        this.lastEdgeId = edgeId;
        this.lastEdgeFraction = edgeFraction;

    }

    public void addSpeedEvent(SpeedEdge speedEvent) {

        speedEvents.add(speedEvent);
        this.updateLastEdge(speedEvent.edgeId, speedEvent.endFraction);

    }

    public void addSnappedEvent(SnappedEvent snappedEvent) {

        snappedEvents.add(snappedEvent);
        //this.updateLastEdge(snappedEvent.edgeId, snappedEvent.edgeFraction);

    }
}
