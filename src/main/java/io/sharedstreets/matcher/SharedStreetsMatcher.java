package io.sharedstreets.matcher;


import io.sharedstreets.barefoot.matcher.Matcher;
import io.sharedstreets.barefoot.matcher.MatcherCandidate;
import io.sharedstreets.barefoot.matcher.MatcherSample;
import io.sharedstreets.barefoot.roadmap.RoadEdge;
import io.sharedstreets.barefoot.roadmap.RoadMap;
import io.sharedstreets.barefoot.roadmap.RoadPoint;
import io.sharedstreets.barefoot.spatial.Geography;
import io.sharedstreets.barefoot.spatial.SpatialOperator;
import io.sharedstreets.barefoot.topology.Cost;
import io.sharedstreets.barefoot.topology.Router;
import io.sharedstreets.barefoot.util.Stopwatch;
import io.sharedstreets.matcher.model.Point;
import io.sharedstreets.matcher.model.events.InputEvent;
import io.sharedstreets.matcher.model.events.MatchOutput;
import io.sharedstreets.matcher.model.events.PointEstimate;
import io.sharedstreets.matcher.model.events.SnappedEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.event.ItemEvent;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class SharedStreetsMatcher extends Matcher implements Serializable {

    // making matcher available as a public static as a workaround
    // for Flink serialization of matcher in speed event reducer class
    // TODO switch model serving architecture (e.g. FLIP-23: https://cwiki.apache.org/confluence/display/FLINK/FLIP-23+-+Model+Serving)

    public static SharedStreetsMatcher matcher;
    public static RoadMap map;


    public static SpatialOperator spatial = new Geography();

    static Logger logger = LoggerFactory.getLogger(SharedStreetsMatcher.class);

    /**
     * Creates a HMM map matching filter for some map, router, cost function, and spatial operator.
     *
     * @param map     {@link RoadMap} object of the map to be matched to.
     * @param router  {@link Router} object to be used for route estimation.
     * @param cost    Cost function to be used for routing.
     * @param spatial Spatial operator for spatial calculations.
     */
    public SharedStreetsMatcher(RoadMap map, Router<RoadEdge, RoadPoint> router, Cost<RoadEdge> cost, SpatialOperator spatial) {
        super(map, router, cost, spatial);
    }


    public DataSet<SnappedEvent> snapEvents(DataSet<Tuple3<Long, Long, InputEvent>> inputEvents, double radius) {

        DataSet<SnappedEvent> snappedEvents = inputEvents.flatMap(new FlatMapFunction<Tuple3<Long, Long, InputEvent>, SnappedEvent>() {
            @Override
            public void flatMap(Tuple3<Long, Long, InputEvent> value, Collector<SnappedEvent> out) throws Exception {

                InputEvent item = value.f2;

                map.loadTile(item.getTileId());

                com.esri.core.geometry.Point p = new com.esri.core.geometry.Point(item.point.lon, item.point.lat);
                Set<RoadPoint> roadPoints = map.index().radius(p, radius);

                SnappedEvent snappedEvent = null;
                double minDistance = Double.MAX_VALUE;

                for(RoadPoint roadPoint : roadPoints) {

                    double edgeNormal = spatial.azimuth(roadPoint.geometry(), p, 1.0);
                    double edgeAzimuth = roadPoint.azimuth();

                    if((edgeNormal > 90 && edgeNormal - edgeAzimuth >= 0) || ((edgeAzimuth - 360) + edgeNormal <= 90 )) {
                        double pointDistance = spatial.distance(p, roadPoint.geometry());
                        if(pointDistance < minDistance) {
                            snappedEvent = new SnappedEvent();

                            snappedEvent.observedPoint = item.point;
                            snappedEvent.matchedPoint = new Point(roadPoint.geometry().getY(), roadPoint.geometry().getX());

                            snappedEvent.edgeId = roadPoint.edge().edgeReferenceId();
                            snappedEvent.edgeFraction = roadPoint.fraction();
                            snappedEvent.time = item.time;
                            snappedEvent.eventData = item.eventData;

                            minDistance = pointDistance;
                        }
                    }
                }

                if(snappedEvent != null)
                    out.collect(snappedEvent);

            }
        });

        return snappedEvents;
    }

    public DataSet<MatchOutput> matchEvents(DataSet<Tuple3<Long, Long, InputEvent>> inputEvents, boolean debug) {

        DataSet<MatchOutput> matchOutputDataSet = inputEvents.groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GroupReduceFunction<Tuple3<Long, Long, InputEvent>, MatchOutput>() {
                    @Override
                    public void reduce(Iterable<Tuple3<Long, Long, InputEvent>> values, Collector<MatchOutput> out) throws Exception {
                        try {

                            //TraceResults
                            VehicleState state = new VehicleState();
                            MatchOutput matchOutput = new MatchOutput();

                            Stopwatch sw = new Stopwatch();
                            sw.start();
                            int inputCount = 0;
                            int sequenceCount = 0;
                            for (Tuple3<Long, Long, InputEvent> value : values) {

                                InputEvent item = value.f2;

                                map.loadTile(item.getTileId());

                                try {
                                    map.readLock();

                                    if(matchOutput.id == null)
                                        matchOutput.id = InputEvent.vehicleIdLongMap.get((item.vehicleId));

                                    inputCount++;
                                    sequenceCount++;

                                    if(item.eventData != null)
                                        state.addEventData(item.time, item.eventData);


                                    synchronized (state) {

                                        final MatcherSample sample = new MatcherSample(item.time, new com.esri.core.geometry.Point(item.point.lon, item.point.lat));


                                        if (state.kState.sample() != null) {

                                            if (sample.time() < state.kState.sample().time()) {
                                                logger.debug("received out of order sample");
                                                continue;
                                            }
                                            if (item.eventData == null && spatial.distance(sample.point(),
                                                    state.kState.sample().point()) < Math.max(0, MatcherFactory.distance)) {
                                                logger.debug("received sample below distance threshold");
                                                continue;
                                            }

                                            if (item.eventData == null && (sample.time() - state.kState.sample().time()) < Math.max(0,
                                                    MatcherFactory.minInterval)) {
                                                logger.debug("received sample below interval threshold");
                                                continue;
                                            }
                                        }

                                        final AtomicReference<Set<MatcherCandidate>> vector =
                                                new AtomicReference<>();

                                        // process sample
                                        vector.set(matcher.execute(state.kState.vector(), state.kState.sample(), sample));
                                        state.kState.update(vector.get(), sample);

                                        // limit max sequence length TODO make configurable
                                        if(sequenceCount > 1000) {
                                            state.extractEvents(matchOutput, debug);
                                            state.reset();
                                            sequenceCount = 0;
                                        }

                                    }

                                }
                                finally {
                                    map.readUnlock();
                                }

                            }

                            if(state !=null) {

                                synchronized (state) {

                                    state.extractEvents(matchOutput, debug);

                                }
                            }

                            sw.stop();

                            if(inputCount > 0)
                                logger.info("{} items in {}ms ({}ms/item): {}", inputCount, sw.ms(), ((double)Math.round((sw.us() / inputCount) /10)/100.0), matchOutput.speedEvents.size());


                            out.collect(matchOutput);
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                    }
                });

        return matchOutputDataSet;

    }

}
