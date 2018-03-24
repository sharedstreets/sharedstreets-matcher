package io.sharedstreets.matcher.output.tiles;


import io.sharedstreets.matcher.model.Week;

import java.io.IOException;
import java.util.Set;

public abstract class TilableData {

    public abstract String getType();

    public abstract Week getWeek();

    public abstract byte[] toBinary() throws IOException;

    public abstract String getId();

    public abstract Set<TileId> getTileKeys(int zLevel);

}
