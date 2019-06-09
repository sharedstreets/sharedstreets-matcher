package io.sharedstreets.matcher;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.barefoot.roadmap.Loader;
import io.sharedstreets.barefoot.roadmap.RoadMap;
import io.sharedstreets.matcher.input.Ingest;
import io.sharedstreets.matcher.model.aggregation.*;
import io.sharedstreets.matcher.model.Week;
import io.sharedstreets.matcher.model.events.*;
import io.sharedstreets.matcher.output.json.GeoJSONOutputFormat;
import io.sharedstreets.matcher.output.tiles.ProtoTileOutputFormat;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class BatchMatcher {

    static int OUTPUT_ZLEVEL = 12;

    static Logger logger = LoggerFactory.getLogger(BatchMatcher.class);

    public static void main(String[] args) throws Exception {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
//        options.addOption( OptionBuilder.withLongOpt( "map" )
//                .withDescription( "path to map tiles" )
//                .hasArg()
//                .withArgName("MAP-DIR")
//                .create() );

        options.addOption( OptionBuilder.withLongOpt( "tracker" )
                .withDescription( "tracker.properties files" )
                .hasArg()
                .withArgName("TRACKER-PATH")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "input" )
                .withDescription( "path to input files" )
                .hasArg()
                .withArgName("INPUT-DIR")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "output" )
                .withDescription( "path to output" )
                .hasArg()
                .withArgName("OUTPUT-DIR")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "debug" )
                .withDescription( "path to debug output" )
                .hasArg()
                .withArgName("DUBUG-DIR")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "tileServer" )
                .withDescription( "tile server" )
                .hasArg()
                .withArgName("TILE-SERVER")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "tileSource" )
                .withDescription( "tile source" )
                .hasArg()
                .withArgName("TILE-SOURCE")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "roadClass" )
                .withDescription( "road class" )
                .hasArg()
                .withType(PatternOptionBuilder.NUMBER_VALUE)
                .withArgName("ROAD-CLASS")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "tmpTilePath" )
                .withDescription( "tmp tile path" )
                .hasArg()
                .withArgName("TMP-TILE-PATH")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "dust" )
                .withDescription( "path to map dust output" )
                .hasArg()
                .withArgName("DUST-DIR")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "binSize" )
                .withDescription( "path to map dust output" )
                .hasArg()
                .withArgName("DUST-DIR")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "eventType" )
                .withDescription( "name for event output" )
                .hasArg()
                .withArgName("EVENT-TYPE")
                .create() );


        options.addOption("f", "fast snap method" );

        String tileServer = "https://tiles.sharedstreets.io/";

        String tileSource = "osm/planet-180430";

        Integer roadClass = 6;

        String tmpTilePath = "/tmp/shst_tiles/";

        String inputPath = "";

        String outputPath = "";

        String debugPath = "debug/";

        String dustPath = "dust/";

        String eventTypeTmp = "events";

        String trackerPath = "tracker.properties";

        boolean fastSnap = false;
        boolean debug = false;
        boolean dust = false;

        int paramEventBinSize = 10; // bin size in meters

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            // validate that block-size has been set
//            if( line.hasOption( "map" ) ) {
//                mapTilePath = line.getOptionValue( "map" );
//            }

            if( line.hasOption( "input" ) ) {
                inputPath = line.getOptionValue( "input" );
            }

            if( line.hasOption( "tracker" ) ) {
                trackerPath = line.getOptionValue( "tracker" );
            }

            if( line.hasOption( "output" ) ) {
                outputPath = line.getOptionValue( "output" );
            }

            if( line.hasOption( "tileServer" ) ) {
                tileServer = line.getOptionValue( "tileServer" );
            }

            if( line.hasOption( "tileSource" ) ) {
                tileSource = line.getOptionValue( "tileSource" );
            }

            if( line.hasOption( "roadClass" ) ) {
                roadClass = Math.toIntExact((Long) line.getParsedOptionValue("roadClass"));
            }

            if( line.hasOption( "tmpTilePath" ) ) {
                tmpTilePath = line.getOptionValue( "tmpTilePath" );
            }

            if( line.hasOption( "debug" ) ) {
                debug = true;
                debugPath = line.getOptionValue( "debug" );
            }

            if( line.hasOption( "dust" ) ) {
                dust = true;
                dustPath = line.getOptionValue( "dust" );
            }

            if( line.hasOption( "eventType" ) ) {
                eventTypeTmp = line.getOptionValue( "eventType" );
            }

            if( line.hasOption( "binSize" ) ) {
                paramEventBinSize = Integer.parseInt(line.getOptionValue( "binSize" ));
            }

            if(line.hasOption("f"))
                fastSnap = true;

        }
        catch( Exception exp ) {
            System.out.println( "Unexpected exception:" + exp.getMessage() );

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "matcher", options );
            return;
        }

        final int eventBinSize = paramEventBinSize; // bin size for event aggregation (in meters)
        final String eventType = eventTypeTmp; // eventType string

        logger.info("Setting up matching engine");

        // map and matcher are stored as static globals (for the time being)
        // as workaround related to Flink serialization of speed matching reducer function

        // dynamically load map based on point data
        SharedStreetsMatcher.map = new RoadMap(tmpTilePath, tileServer, tileSource, roadClass);

        // build match engine
        SharedStreetsMatcher.matcher = MatcherFactory.createMatcher(trackerPath, SharedStreetsMatcher.map);

        logger.info("Matcher ready!");

        // let's go...

        logger.info("Starting up streams...");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // process events

        // step 1: read strings (blocks of location events from file)

        DataSet<Tuple3<Long, Long, InputEvent>> inputEvents = env.readFile(new FileInputFormat<InputEvent>() {

            @Override
            public boolean reachedEnd() throws IOException {
                if(stream.available() > 0)
                    return false;
                else
                    return true;
            }

            @Override
            public InputEvent nextRecord(InputEvent reuse) throws IOException {
                Ingest.InputEventProto proto = Ingest.InputEventProto.parseDelimitedFrom(this.stream);
                return InputEvent.fromProto(proto);
            }
        }, inputPath).setParallelism(1).map(new MapFunction<InputEvent, Tuple3<Long, Long, InputEvent>>() {
            @Override
            public Tuple3<Long, Long, InputEvent> map(InputEvent value) throws Exception {
                return new Tuple3<Long, Long, InputEvent>(value.vehicleId, value.time, value);
            }
        }).filter(new FilterFunction<Tuple3<Long, Long, InputEvent>>() {
            @Override
            public boolean filter(Tuple3<Long, Long, InputEvent> value) throws Exception {
                return value.f1 != null;
            }
        });

        if(fastSnap) {

            // fast snap method via point projection on edge geometry -- primarily for testing
            // only matches events to edges

            int snapRadius = 10; // search radius (meters) for snapping to nearby edges.

            DataSet<SnappedEvent> snappedEvents = SharedStreetsMatcher.matcher.snapEvents(inputEvents, snapRadius);

            DataSet<SharedStreetsEventData> binnedData = snappedEvents.groupBy(new KeySelector<SnappedEvent, Long>() {

                @Override
                public Long getKey(SnappedEvent value) throws Exception {
                    return value.edgeId;
                }
            }).reduceGroup(new GroupReduceFunction<SnappedEvent, SharedStreetsEventData>() {

                @Override
                public void reduce(Iterable<SnappedEvent> values, Collector<SharedStreetsEventData> out) throws Exception {

                    WeeklyBinnedLinearEvents binnedData = null;
                    Long edgeId = null;
                    Polyline geometry = null;

                    for(SnappedEvent snappedEvent : values) {
                        if(edgeId == null) {
                            edgeId = snappedEvent.edgeId;
                            geometry = BaseRoad.IDs.getGeom(edgeId);
                            double length = SharedStreetsMatcher.spatial.length(geometry);
                            int numBins = (int)Math.floor(length / eventBinSize) + 1;

                            binnedData = new WeeklyBinnedLinearEvents(numBins, length, null);
                        }

                        PeriodicTimestamp periodicTimestamp = PeriodicTimestamp.utcPeriodTimestamp(snappedEvent.time);
                        binnedData.addEvent(periodicTimestamp.period, snappedEvent);
                    }

                    String referenceId = BaseRoad.IDs.getKey(edgeId);
                    HashSet<Point> referencePoints = new HashSet<Point>();


                    referencePoints.add(geometry.getPoint(0));
                    referencePoints.add(geometry.getPoint(geometry.getPointCount() - 1));

                    SharedStreetsEventData data = new SharedStreetsEventData(eventType, referenceId, referencePoints, binnedData);

                    out.collect(data);
                }
            });

            ProtoTileOutputFormat outputFormat = new ProtoTileOutputFormat<SharedStreetsEventData>(outputPath, OUTPUT_ZLEVEL, true);

            binnedData.output(outputFormat).setParallelism(1);
        }
        else {

            // use default HMM matcher

            // step 2:  match input events
            DataSet<MatchOutput> matchOutput = SharedStreetsMatcher.matcher.matchEvents(inputEvents, debug);

            if(debug) {

                // write debug trace output to GeoJSON
                GeoJSONOutputFormat debugOutputFormat = new GeoJSONOutputFormat<MatchOutput>(debugPath);
                matchOutput.output(debugOutputFormat).setParallelism(1);
            }

            if(dust) {

                DataSet<Tuple2<String,MatchFailure>> mappedFailures  = matchOutput.flatMap(new FlatMapFunction<MatchOutput, Tuple2<String, MatchFailure>>() {

                    @Override
                    public void flatMap(MatchOutput value, Collector<Tuple2<String, MatchFailure>> out) throws Exception {
                        for(MatchFailure matchFailure : value.matchFailures) {
                            if(matchFailure.startEdgeId != null && matchFailure.endEdgeId != null) {
                                String id = matchFailure.startEdgeId.toString();
                                out.collect(new Tuple2<>(id, matchFailure));
                            }
                        }
                    }
                });

                DataSet<MatchFailureCluster> failureCluster = mappedFailures.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, MatchFailure>, MatchFailureCluster>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, MatchFailure>> values, Collector<MatchFailureCluster> out) throws Exception {
                        MatchFailureCluster cluster = new MatchFailureCluster();

                        HashSet<Point> referencePoints = new HashSet<Point>();

                        for(Tuple2<String, MatchFailure> value : values) {

                            if(cluster.getId() == null) {
                                cluster.startEdgeId = BaseRoad.IDs.getKey(value.f1.startEdgeId);

                                Polyline geometry = BaseRoad.IDs.getGeom(value.f1.startEdgeId);
                                referencePoints.add(geometry.getPoint(0));
                                cluster.referencePoints = referencePoints;
                            }

                            cluster.failures.add(value.f1);
                        }
                        out.collect(cluster);
                    }
                });

                ProtoTileOutputFormat dustOutputFormat = new ProtoTileOutputFormat<MatchFailureCluster>(dustPath, OUTPUT_ZLEVEL, false);
                failureCluster.output(dustOutputFormat).setParallelism(1);
            }

            // extract speed events from output
            DataSet<SpeedEdge> speedEvents = matchOutput.flatMap(new FlatMapFunction<MatchOutput, SpeedEdge>() {
                @Override
                public void flatMap(MatchOutput value, Collector<SpeedEdge> out) throws Exception {
                    if(value.speedEvents != null) {
                        for (SpeedEdge speedEvent : value.speedEvents) {
                            out.collect(speedEvent);
                        }
                    }
                }
            });

            // step 3: aggregate speed events
            DataSet<SharedStreetsSpeedData> aggregatedSpeeds = speedEvents.groupBy(new KeySelector<SpeedEdge, Long>() {

                @Override
                public Long getKey(SpeedEdge value) throws Exception {
                    return value.edgeId;
                }
            }).reduceGroup(new GroupReduceFunction<SpeedEdge, SharedStreetsSpeedData>() {

                @Override
                public void reduce(Iterable<SpeedEdge> values, Collector<SharedStreetsSpeedData> out) throws Exception {
                    try {
                        HashMap<Week, WeeklySpeedCycle> weeklySpeeds = new HashMap<Week, WeeklySpeedCycle>();

                        Long edgeId = null;
                        for (SpeedEdge speedEvent : values) {

                            if (edgeId == null)
                                edgeId = speedEvent.edgeId;

                            PeriodicTimestamp periodicTimestamp = PeriodicTimestamp.utcPeriodTimestamp(speedEvent.time);

                            if (!weeklySpeeds.containsKey(periodicTimestamp.week)) {
                                weeklySpeeds.put(periodicTimestamp.week, new WeeklySpeedCycle(periodicTimestamp.week));
                            }

                            weeklySpeeds.get(periodicTimestamp.week).addSpeedEvent(speedEvent.speed, periodicTimestamp);
                        }

                        String referenceId = BaseRoad.IDs.getKey(edgeId);
                        HashSet<Point> referencePoints = new HashSet<Point>();

                        Polyline geometry = BaseRoad.IDs.getGeom(edgeId);
                        referencePoints.add(geometry.getPoint(0));
                        referencePoints.add(geometry.getPoint(geometry.getPointCount() - 1));

                        for (WeeklySpeedCycle weeklySpeedCycle : weeklySpeeds.values()) {

                            SharedStreetsSpeedData speedData = new SharedStreetsSpeedData(referenceId, referencePoints, weeklySpeedCycle);
                            out.collect(speedData);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            // extract events data from output
            DataSet<SnappedEvent> snappedEvents = matchOutput.flatMap(new FlatMapFunction<MatchOutput, SnappedEvent>() {
                @Override
                public void flatMap(MatchOutput value, Collector<SnappedEvent> out) throws Exception {
                    if(value.snappedEvents != null) {
                        for (SnappedEvent snappedEvent : value.snappedEvents) {
                            out.collect(snappedEvent);
                        }
                    }
                }
            });

            // process event data

            DataSet<SharedStreetsEventData> binnedData = snappedEvents.groupBy(new KeySelector<SnappedEvent, Long>() {

                @Override
                public Long getKey(SnappedEvent value) throws Exception {
                    return value.edgeId;
                }
            }).reduceGroup(new GroupReduceFunction<SnappedEvent, SharedStreetsEventData>() {

                @Override
                public void reduce(Iterable<SnappedEvent> values, Collector<SharedStreetsEventData> out) throws Exception {
                    WeeklyBinnedLinearEvents binnedData = null;
                    Long edgeId = null;
                    Polyline geometry = null;

                    for(SnappedEvent snappedEvent : values) {

                        PeriodicTimestamp periodicTimestamp = PeriodicTimestamp.utcPeriodTimestamp(snappedEvent.time);

                        if(edgeId == null) {
                            edgeId = snappedEvent.edgeId;

                            geometry = BaseRoad.IDs.getGeom(edgeId);
                            double length = SharedStreetsMatcher.spatial.length(geometry);
                            int numBins = (int)Math.floor(length / eventBinSize) + 1;

                            binnedData = new WeeklyBinnedLinearEvents(numBins, length, periodicTimestamp.week);
                        }

                        binnedData.addEvent(periodicTimestamp.period, snappedEvent);
                    }

                    String referenceId = BaseRoad.IDs.getKey(edgeId);
                    HashSet<Point> referencePoints = new HashSet<Point>();


                    referencePoints.add(geometry.getPoint(0));
                    referencePoints.add(geometry.getPoint(geometry.getPointCount() - 1));

                    SharedStreetsEventData data = new SharedStreetsEventData(eventType, referenceId, referencePoints, binnedData);

                    out.collect(data);
                }
            });


            // step 4: write tiles

            ProtoTileOutputFormat eventOutputFormat = new ProtoTileOutputFormat<SharedStreetsEventData>(outputPath, OUTPUT_ZLEVEL, true);
            binnedData.output(eventOutputFormat).setParallelism(1);

            ProtoTileOutputFormat speedOutputFormat = new ProtoTileOutputFormat<SharedStreetsSpeedData>(outputPath, OUTPUT_ZLEVEL, true);
            aggregatedSpeeds.output(speedOutputFormat).setParallelism(1);
        }

        // execute event processing pipeline
        env.execute("Process Event Stream");

    }
}