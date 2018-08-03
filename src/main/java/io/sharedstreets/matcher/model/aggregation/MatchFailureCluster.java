package io.sharedstreets.matcher.model.aggregation;

import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.matcher.model.Point;
import io.sharedstreets.matcher.model.Week;
import io.sharedstreets.matcher.model.events.MatchFailure;
import io.sharedstreets.matcher.output.protobuf.Dust;
import io.sharedstreets.matcher.output.tiles.TilableData;
import io.sharedstreets.matcher.output.tiles.TileId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class MatchFailureCluster extends TilableData {

    public String startEdgeId;
    public Set<com.esri.core.geometry.Point> referencePoints;
    public List<MatchFailure> failures = new ArrayList<MatchFailure>();

    @Override
    public String getType() {
        return "dust";
    }


    @Override
    public Week getWeek() {
        return null;
    }


    @Override
    public byte[] toBinary() throws IOException {

        Dust.MapDust.Builder mapDust = Dust.MapDust.newBuilder();

        mapDust.setStartId(this.startEdgeId);

      for(MatchFailure failure : failures) {


            Dust.MapDustTrace.Builder dustTrace = Dust.MapDustTrace.newBuilder();
            dustTrace.addStartFraction(failure.startEdgeFraction);
            dustTrace.addEndFraction(failure.endEdgeFraction);
            dustTrace.setEndId(BaseRoad.IDs.getKey(failure.endEdgeId));

            for(Point point : failure.failedPoints) {
                dustTrace.addLonlats(point.lon);
                dustTrace.addLonlats(point.lat);
            }

            mapDust.addTraceData(dustTrace);
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        mapDust.build().writeDelimitedTo(bytes);

        return bytes.toByteArray();

    }

    @Override
    public String getId() {
        return startEdgeId;
    }


    @Override
    public Set<TileId> getTileKeys(int zLevel) {

        HashSet<TileId> tileIds = new HashSet<TileId>();

        for(com.esri.core.geometry.Point point : referencePoints) {
            TileId tileId = TileId.lonLatToTileId(zLevel,point.getX(), point.getY());
            tileIds.add(tileId);
        }

        return tileIds;
    }
}
