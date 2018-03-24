package io.sharedstreets.matcher.model.events;


import io.sharedstreets.matcher.model.Point;
import io.sharedstreets.matcher.output.json.GeoJSONData;

import java.util.List;

public class MatchFailure  {

    public Long startEdgeId;
    public Double startEdgeFraction;

    public Long endEdgeId;
    public Double endEdgeFraction;

    public List<Point> failedPoints;

}
