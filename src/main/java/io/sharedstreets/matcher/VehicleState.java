package io.sharedstreets.matcher;


import io.sharedstreets.barefoot.matcher.MatcherCandidate;
import io.sharedstreets.barefoot.matcher.MatcherKState;
import io.sharedstreets.barefoot.spatial.Geography;
import io.sharedstreets.barefoot.spatial.SpatialOperator;
import io.sharedstreets.matcher.model.events.MatchOutput;
import io.sharedstreets.matcher.model.events.PointEstimate;
import io.sharedstreets.matcher.model.events.SnappedEvent;
import io.sharedstreets.matcher.model.Point;
import io.sharedstreets.matcher.model.events.SpeedEdge;

import java.util.HashMap;

public class VehicleState {

    private long speedEventMaxTime = 0l;

    private Long lastReset = null;

    private HashMap<Long, HashMap<String, Double> > eventData = new HashMap<>();

    public MatcherKState kState;

    public VehicleState() {
        kState = new MatcherKState();
    }

    public void reset(){
        kState = new MatcherKState();
        eventData = new HashMap<>();
    }

    // TODO handle more than one event per timestamp
    public void addEventData(long eventTime, HashMap<String, Double>  data) {

        eventData.put(eventTime, data);
    }

    public void extractEvents(MatchOutput output) {

        try {
            if (kState.estimate() != null) {

                double lastSequenceProb = 0;

                for (int i = 0; i < kState.sequence().size(); ++i) {

                    MatcherCandidate candidate = kState.sequence().get(i);

                    if(lastSequenceProb < candidate.seqprob())
                        lastSequenceProb = 0;

                    double seqProbDelta = candidate.seqprob() - lastSequenceProb;
                    lastSequenceProb = candidate.seqprob();

                    // only work with successfully matched segments
                    if (candidate.transition() != null ) {

                        // get transition stats

                        long startTime = kState.samples().get(i - 1).time();
                        long endTime = kState.samples().get(i).time();
                        com.esri.core.geometry.Point samplePoint =  kState.samples().get(i).point();
                        double length = candidate.transition().route().length();

                        // calc speed m/s for transition
                        double speed = length / ((endTime - startTime) / 1000);

                        if(lastReset == null)
                            lastReset = endTime;

                        Point matchedPoint = new Point(candidate.point().geometry().getX(), candidate.point().geometry().getY());
                        Point observedPoint = new Point(samplePoint.getX(), samplePoint.getY());
                        PointEstimate pointEstimate = new PointEstimate();

                        pointEstimate.sequenceprob = candidate.seqprob();
                        pointEstimate.filterprob = candidate.filtprob();

                        pointEstimate.speed = speed;
                        pointEstimate.time = endTime;

                        pointEstimate.matchedPoint = matchedPoint;
                        pointEstimate.observedPoint = observedPoint;

                        output.pointEstimates.add(pointEstimate);

                        if (eventData.containsKey(endTime)) {

                            double edgeFraction = candidate.transition().route().target().fraction();

                            SnappedEvent snappedEvent = new SnappedEvent();

                            snappedEvent.edgeId = candidate.transition().route().target().edge().edgeReferenceId();
                            snappedEvent.edgeFraction = edgeFraction;
                            snappedEvent.time = endTime;
                            snappedEvent.speed = speed;

                            snappedEvent.matchedPoint = matchedPoint;
                            snappedEvent.observedPoint = observedPoint;

                            snappedEvent.eventData = eventData.get(endTime);

                            output.addSnappedEvent(snappedEvent);

                            eventData.remove(endTime);

                        }

                        if(seqProbDelta > MatcherFactory.seqProbDelta) {

                            for (int j = 0; j < candidate.transition().route().path().size(); j++) {

                                if (endTime >= this.speedEventMaxTime) {

                                    SpeedEdge speedEvent = new SpeedEdge();
                                    speedEvent.edgeId = candidate.transition().route().get(j).edgeReferenceId();


                                    if(speedEvent.edgeId == candidate.transition().route().source().edge().edgeReferenceId())
                                        speedEvent.startFaction = candidate.transition().route().source().fraction();
                                    else
                                        speedEvent.startFaction = 0.0;

                                    if(speedEvent.edgeId == candidate.transition().route().target().edge().edgeReferenceId())
                                        speedEvent.endFraction = candidate.transition().route().target().fraction();
                                    else
                                        speedEvent.endFraction = 1.0;

                                    speedEvent.speed = speed;

                                    speedEvent.time = endTime;

                                    output.addSpeedEvent(speedEvent);
                                    this.speedEventMaxTime = endTime;
                                }
                            }
                        }
                        else {
                            // deal with match failures
                            output.failedPoints.add(observedPoint);
                        }
                    }

                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}



