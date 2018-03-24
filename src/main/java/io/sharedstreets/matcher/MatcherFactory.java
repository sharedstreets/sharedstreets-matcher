package io.sharedstreets.matcher;

import io.sharedstreets.barefoot.roadmap.RoadEdge;
import io.sharedstreets.barefoot.roadmap.RoadMap;
import io.sharedstreets.barefoot.roadmap.RoadPoint;
import io.sharedstreets.barefoot.roadmap.TimePriority;
import io.sharedstreets.barefoot.spatial.Geography;
import io.sharedstreets.barefoot.spatial.SpatialOperator;
import io.sharedstreets.barefoot.topology.Dijkstra;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class MatcherFactory {

    static Logger logger = LoggerFactory.getLogger(MatcherFactory.class);

    public static Integer minInterval;
    public static Integer maxInterval;
    public static Integer distance;
    public static Double sensitive;
    public static Double seqProbDelta;

    public static SharedStreetsMatcher createMatcher(String propertiesFile, RoadMap map) throws IOException {
        SharedStreetsMatcher matcher = new SharedStreetsMatcher(map, new Dijkstra<RoadEdge, RoadPoint>(), new TimePriority(),
                new Geography());

        InputStream is = new FileInputStream(propertiesFile);
        Properties properties = new Properties();
        properties.load(is);
        is.close();

        matcher.setMaxFailure(Integer.parseInt(properties.getProperty("matcher.failure.max",
                Integer.toString(matcher.getMaxFailure()))));
        matcher.setMaxRadius(Double.parseDouble(properties.getProperty("matcher.radius.max",
                Double.toString(matcher.getMaxRadius()))));
        matcher.setMaxDistance(Double.parseDouble(properties.getProperty("matcher.distance.max",
                Double.toString(matcher.getMaxDistance()))));
        matcher.setLambda(Double.parseDouble(properties.getProperty("matcher.lambda",
                Double.toString(matcher.getLambda()))));
        matcher.setSigma(Double.parseDouble(properties.getProperty("matcher.sigma", Double.toString(matcher.getSigma()))));

        minInterval = Integer.parseInt(properties.getProperty("matcher.interval.min", "1000"));
        maxInterval = Integer.parseInt(properties.getProperty("matcher.interval.max", "60000"));
        distance = Integer.parseInt(properties.getProperty("matcher.distance.min", "0"));

        seqProbDelta = Double.parseDouble(
                properties.getProperty("tracker.seqprobdelta.min", "-5.0"));

        sensitive = Double.parseDouble(
                properties.getProperty("tracker.monitor.sensitive", "0.0"));

        logger.info("tracker.monitor.sensitive={}", sensitive);
        logger.info("matcher.failure.max={}", matcher.getMaxFailure());
        logger.info("matcher.radius.max={}", matcher.getMaxRadius());
        logger.info("matcher.distance.max={}", matcher.getMaxDistance());
        logger.info("matcher.lambda={}", matcher.getLambda());
        logger.info("matcher.sigma={}", matcher.getSigma());
        logger.info("matcher.interval.min={}", minInterval);
        logger.info("matcher.interval.max={}", maxInterval);
        logger.info("matcher.distance.min={}", distance);
        logger.info("tracker.seqprobdelta.min={}", seqProbDelta);

        return matcher;
    }
}
