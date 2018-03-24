package io.sharedstreets.matcher.model.aggregation;


import io.sharedstreets.matcher.model.Week;
import io.sharedstreets.matcher.output.protobuf.SharedStreetsLinearReferencesProto;
import io.sharedstreets.matcher.output.tiles.TilableData;
import io.sharedstreets.matcher.output.tiles.TileId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SharedStreetsEventData extends TilableData {


    String referenceId;
    Set<com.esri.core.geometry.Point> referencePoints;
    WeeklyBinnedLinearEvents binnedEvents;

    public SharedStreetsEventData(String referenceId, Set<com.esri.core.geometry.Point> referencePoints, WeeklyBinnedLinearEvents binnedEvents) {

        this.referenceId = referenceId;
        this.referencePoints = referencePoints;
        this.binnedEvents = binnedEvents;
    }

    @Override
    public String getType() {
        return "events";
    }

    @Override
    public byte[] toBinary() throws IOException {

        SharedStreetsLinearReferencesProto.SharedStreetsWeeklyBinnedLinearReferences speedDataProto = binnedEvents.getProtobuf(referenceId);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        speedDataProto.writeDelimitedTo(bytes);

        return bytes.toByteArray();
    }

    @Override
    public Week getWeek() {
        return this.binnedEvents.week;
    }

    @Override
    public String getId() {
        return referenceId;
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
