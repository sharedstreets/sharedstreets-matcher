package io.sharedstreets.matcher.output.tiles;

/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import org.apache.flink.annotation.Public;

import java.io.IOException;

/**
 * The abstract base class for all Rich output formats that are file based. Contains the logic to
 * open/close the target
 * file streams.
 */
@Public
public class ProtoTileOutputFormat<IT extends  TilableData> extends TiledNIOFileOutputFormat<IT> {

    int zLevel = 10;
    boolean saveWeekly;

    public ProtoTileOutputFormat(String outputPath, int zLevel, boolean saveWeekly) {
        super(outputPath, "pbf");
        this.saveWeekly = saveWeekly;
        this.zLevel = zLevel;
    }

    @Override
    public void writeRecord(IT record) throws IOException {

        for(TileId tileId :record.getTileKeys(zLevel)) {

            this.writeRecord(tileId, record.getWeek(), record.getType(), record.toBinary());

        }

    }
}