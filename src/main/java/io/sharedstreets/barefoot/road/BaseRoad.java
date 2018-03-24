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

package io.sharedstreets.barefoot.road;

import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.*;
import io.sharedstreets.barefoot.roadmap.RoadEdge;


import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * RoadEdge data structure for a road segment.
 *
 * Provides topological information, i.e. {@link BaseRoad#source()} and {@link BaseRoad#target()}),
 * road type information, i.e. {@link BaseRoad#oneway()}, {@link BaseRoad#type()},
 * {@link BaseRoad#priority()} and {@link BaseRoad#maxSpeed(Heading)}), and geometrical information
 * (e.g. {@link BaseRoad#length()} and {@link BaseRoad#geometry()}).
 */
public class BaseRoad implements Serializable {

    public static IdGen IDs = new IdGen();

    public static class IdGen {

        HashMap<String, Long> keyMap = new HashMap<>();
        HashMap<Long, String> idMap = new HashMap<>();
        HashMap<Long, Polyline> geomMap = new HashMap<>();
        Long nextId = 100000l;

        private Long getNextId() {
            nextId++;
            return nextId;
        }

        public Long getId(String key) {

            if (!keyMap.containsKey(key)) {
                long id = getNextId();
                keyMap.put(key, id);
                idMap.put(id, key);
            }

            return keyMap.get(key);
        }

        public Polyline getGeom(long id) { return geomMap.get(id); }
        public String getKey(long id){
            return idMap.get(id);
        }
    }

    private static final long serialVersionUID = 1L;
    private final long id;
    private final long source;
    private final long target;
    private final long forwardId;
    private final long backId;
    private final short type;
    private final float priority;
    private final float maxSpeedForward;
    private final float maxSpeedBackward;
    private final float length;
    private final Polyline geometry;

    private final ArrayList<RoadEdge> edges;


    public BaseRoad(String geometryRefId, String startIntersectionId, String endIntersectionId, String forwardRefId, String backRefId, short type,
            float priority, float maxSpeed, float length,
            Polyline geometry) {


        this.id = IDs.getId(geometryRefId);;
        this.source = IDs.getId(startIntersectionId);
        this.target = IDs.getId(endIntersectionId);
        this.forwardId = IDs.getId(forwardRefId);

        IDs.geomMap.put(this.forwardId, geometry);

        if(backRefId != null && backRefId != "") {
            this.backId = IDs.getId(backRefId);

            IDs.geomMap.put(this.backId, geometry);
        }
        else
            this.backId = -1;

        this.type = type;
        this.priority = priority;
        this.maxSpeedForward = maxSpeed;
        this.maxSpeedBackward = maxSpeed;
        this.length = length;
        this.geometry = geometry;

        this.edges = new ArrayList();

        edges.add(new RoadEdge(this, Heading.forward));

        if(backId != -1)
            edges.add(new RoadEdge(this, Heading.backward));

    }

    public List<RoadEdge> getEdges(){

        return edges;
    }

    /**
     * Gets unique road identifier.
     *
     * @return Unique road identifier.
     */
    public long id() {
        return id;
    }

    /**
     * Gets source vertex identifier.
     *
     * @return Source vertex identifier.
     */
    public long source() {
        return source;
    }

    /**
     * Gets target vertex identifier.
     *
     * @return Target vertex identifier.
     */
    public long target() {
        return target;
    }

    public long forward() {
        return forwardId;
    }

    public long back() {
        return backId;
    }


    /**
     * Gets a boolean if this is a one-way.
     *
     * @return True if this road is a one-way road, false otherwise.
     */
    public boolean oneway() {
        return backId == -1 ? true : false;
    }

    /**
     * Gets road's type identifier.
     *
     * @return RoadEdge type identifier.
     */
    public short type() {
        return type;
    }

    /**
     * Gets road's priority factor, i.e. an additional cost factor for routing, and must be greater
     * or equal to one. Higher priority factor means higher costs.
     *
     * @return RoadEdge's priority factor.
     */
    public float priority() {
        return priority;
    }

    /**
     * Gets road's maximum speed for respective heading in kilometers per hour.
     *
     * @param heading {@link Heading} for which maximum speed must be returned.
     * @return Maximum speed in kilometers per hour.
     */
    public float maxSpeed(Heading heading) {
        return heading == Heading.forward ? maxSpeedForward : maxSpeedBackward;
    }

    /**
     * Gets road length in meters.
     *
     * @return RoadEdge length in meters.
     */
    public float length() {
        return length;
    }

    /**
     * Gets road's geometry as a {@link Polyline} from the road's source to its target.
     *
     * @return RoadEdge's geometry as {@link Polyline} from source to target.
     */
    public Polyline geometry() {
        return this.geometry;
    }

}
