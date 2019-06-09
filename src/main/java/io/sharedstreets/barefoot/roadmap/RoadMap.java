/*
 * Copyright (C) 2015, BMW Car IT GmbH
 *
 * Author: Sebastian Mattheis <sebastian.mattheis@bmw-carit.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.sharedstreets.barefoot.roadmap;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.barefoot.road.RoadReader;
import io.sharedstreets.barefoot.spatial.RTreeIndex;
import io.sharedstreets.barefoot.spatial.SpatialIndex;
import io.sharedstreets.barefoot.topology.Graph;
import io.sharedstreets.barefoot.util.SourceException;
import io.sharedstreets.matcher.input.SharedStreetsReader;
import io.sharedstreets.matcher.output.tiles.TileId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Executable;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of a road map with (directed) roads, i.e. {@link RoadEdge} objects. It provides a road
 * network for routing that is derived from {@link Graph} and spatial search of roads with a
 * {@link SpatialIndex}.
 * <p>
 * <b>Note:</b> Since {@link RoadEdge} objects are directed representations of {@link BaseRoad} objects,
 * identifiers have a special mapping, see {@link RoadEdge}.
 */
public class RoadMap extends Graph<RoadEdge> implements Serializable {

    private final String tileServer;
    private final String tileSource;
    private final Integer roadClass;

    private final String tmpTilePath;

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(RoadMap.class);
    private transient Index index = null;

    private transient ArrayList<BaseRoad> roads;
    private ReentrantReadWriteLock mapLock;
    private Set<TileId> loadedTiles;

    static Collection<RoadEdge> split(BaseRoad base) {
        List<RoadEdge> roads = base.getEdges();
        return roads;
    }

    public RoadMap(String tmpTilePath, String tileServer, String tileSource, Integer roadClass) {

        this.tmpTilePath = tmpTilePath;

        this.tileServer = tileServer;
        this.tileSource = tileSource;
        this.roadClass = roadClass;

        index = new Index();
        roads = new ArrayList<>();
        mapLock = new ReentrantReadWriteLock();
        loadedTiles = new HashSet<TileId>();
    }

    public class Index  {
        private static final long serialVersionUID = 1L;
        public final RTreeIndex index;

        public Index() {
            index = new RTreeIndex();
        }

        public void put(BaseRoad road) {
            index.add(road);
        }

        public Set<RoadPoint> nearest(Point c) {
            return index.nearest(c);
        }

        public Set<RoadPoint> radius(Point c, double r) {
            return index.radius(c, r);
        }

        public Set<RoadPoint> knearest(Point c, int k) {
            return index.knearest(c, k);
        }
    }

    public Index index() {
        return this.index;
    }

    public void readLock() {
        this.mapLock.readLock().lock();
    }

    public void readUnlock() {
        this.mapLock.readLock().unlock();
    }

    public void loadTile(TileId tileId) throws IOException {

        if(loadedTiles.contains(tileId))
            return;

        try {

            mapLock.writeLock().lock();

            // check cache status again...
            if(loadedTiles.contains(tileId))
                return;

            SharedStreetsReader sharedStreetsReader = new SharedStreetsReader(this.tmpTilePath, this.tileServer, this.tileSource, this.roadClass, tileId);

            sharedStreetsReader.open();

            BaseRoad newRoad = null;
            ArrayList<BaseRoad> tileRoads = new ArrayList<>();

            while ((newRoad = sharedStreetsReader.next()) != null) {
                roads.add(newRoad);
                index.put(newRoad);
                for (RoadEdge edge : split(newRoad)) {
                    this.add(edge);
                }
            }

            sharedStreetsReader.close();

            loadedTiles.add(tileId);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            mapLock.writeLock().unlock();
        }

    }

    /**
     * Loads and creates a {@link RoadMap} object from {@link BaseRoad} objects loaded with a
     * {@link RoadReader}.
     *
     * @param reader {@link RoadReader} to load {@link BaseRoad} objects.
     * @return {@link RoadMap} object.
     * @throws SourceException thrown if error occurs while loading roads.
     */
    public static RoadMap Load(RoadReader reader) throws SourceException {

        long memory = 0;

        System.gc();
        memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        if (!reader.isOpen()) {
            reader.open();
        }

        logger.info("inserting roads ...");

        RoadMap roadmap = new RoadMap(null, null, null, null);


        int geometryCount = 0, counter = 0;
        BaseRoad baseRoad = null;
        while ((baseRoad = reader.next()) != null) {
            geometryCount += 1;

            roadmap.roads.add(baseRoad);

            if (geometryCount % 100000 == 0) {
                logger.info("inserted {} ({}) roads", geometryCount, counter);
            }
        }

        logger.info("inserted {} ({}) roads and finished", geometryCount, counter);

        reader.close();

        System.gc();
        memory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) - memory;
        logger.info("~{} megabytes used for road data (estimate)",
                Math.max(0, Math.round(memory / 1E6)));

        return roadmap;
    }

    /**
     * Constructs road network topology and spatial index.
     */
    @Override
    public RoadMap construct() {
        long memory = 0;

        System.gc();
        memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        logger.info("index and topology constructing ...");

        for(BaseRoad baseRoad : this.roads) {

            index.put(baseRoad);

            for (RoadEdge edge : split(baseRoad)) {
                this.add(edge);
            }
        }

        super.construct();

        logger.info("index and topology constructed");

        System.gc();
        memory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) - memory;
        logger.info("~{} megabytes used for spatial index (estimate)",
                Math.max(0, Math.round(memory / 1E6)));

        return this;
    }

    /**
     * Gets {@link RoadReader} of roads in this {@link RoadMap}.
     *
     * @return {@link RoadReader} object.
     */
    public RoadReader reader() {
        return new RoadReader() {
            Iterator<RoadEdge> iterator = null;
            HashSet<Short> exclusions = null;
            Polygon polygon = null;

            @Override
            public boolean isOpen() {
                return (iterator != null);
            }

            @Override
            public void open() throws SourceException {
                open(null, null);
            }

            @Override
            public void open(Polygon polygon, HashSet<Short> exclusions) throws SourceException {
                iterator = edges.values().iterator();
                this.exclusions = exclusions;
                this.polygon = polygon;
            }

            @Override
            public void close() throws SourceException {
                iterator = null;
            }

            @Override
            public BaseRoad next() throws SourceException {
                BaseRoad road = null;
                do {
                    if (!iterator.hasNext()) {
                        return null;
                    }

                    RoadEdge _road = iterator.next();

                    if (_road.id() % 2 == 1) {
                        continue;
                    }

                    road = _road.base();
                } while (road == null || exclusions != null && exclusions.contains(road.type())
                        || polygon != null
                                && !GeometryEngine.contains(polygon, road.geometry(),
                                        SpatialReference.create(4326))
                                && !GeometryEngine.overlaps(polygon, road.geometry(),
                                        SpatialReference.create(4326)));
                return road;
            }
        };
    }
}
