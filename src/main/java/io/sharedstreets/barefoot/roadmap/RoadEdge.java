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

import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.barefoot.road.Heading;
import io.sharedstreets.barefoot.topology.AbstractEdge;
import com.esri.core.geometry.Polyline;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;

/**
 * Directed road wrapper of {@link BaseRoad} objects in a directed road map ({@link RoadMap}). *
 * <p>
 * <b>Note:</b> Since {@link RoadEdge} objects are directional representations of {@link BaseRoad}
 * objects, each {@link BaseRoad} is split into two {@link RoadEdge} objects. For that purpose, it uses
 * the identifier <i>i</i> of each {@link BaseRoad} to define identifiers of the respective
 * {@link RoadEdge} objects, where <i>i * 2</i> is the identifier of the forward directed {@link RoadEdge}
 * and <i>i * 2 + 1</i> of the backward directed {@link RoadEdge}.
 */
public class RoadEdge extends AbstractEdge<RoadEdge> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final BaseRoad base;
    private final Heading heading;

    static Polyline invert(Polyline geometry) {
        Polyline reverse = new Polyline();
        int last = geometry.getPointCount() - 1;
        reverse.startPath(geometry.getPoint(last));

        for (int i = last - 1; i >= 0; --i) {
            reverse.lineTo(geometry.getPoint(i));
        }

        return reverse;
    }

    /**
     * Constructs {@link RoadEdge} object.
     *
     * @param base {@link BaseRoad} object to be referred to.
     * @param heading {@link Heading} of the directed {@link RoadEdge}.
     */
    public RoadEdge(BaseRoad base, Heading heading) {
        this.base = base;
        this.heading = heading;
    }

    public RoadPoint getRoadPoint(double f) {
        return new RoadPoint(this, f);
    }

    @Override
    public long id() {

        // TODO this hacky Barefoot ID management scheme needs to be replaced with actual back/forward refs
        // but ESRI's Quadtreet index only supports int keys which means we can't directly use long ids
        // let's replace the ESRI quadtree with a better index and update id scheme at the same time
        // alternative: https://github.com/tzaeschke/tinspin-indexes/
        return heading == Heading.forward ? base.id() * 2 : base.id() * 2 + 1;
    }

    @Override
    public long source() {
        return heading == Heading.forward ? base.source() : base.target();
    }

    @Override
    public long target() {
        return heading == Heading.forward ? base.target() : base.source();
    }


    // get internal edge reference id -- moving data out of barefoot graph and linking back to SR references
    public long edgeReferenceId() {
        return heading == Heading.forward ? base.forward() : base.back();
    }

    // get internal edge reference id -- moving data out of barefoot graph and linking back to SR references
    public long geometryId() {
        return base.id();
    }


    /**
     * Gets road's type identifier.
     *
     * @return RoadEdge type identifier.
     */
    public short type() {
        return base.type();
    }

    /**
     * Gets road's priority factor, i.e. an additional cost factor for routing, and must be greater
     * or equal to one. Higher priority factor means higher costs.
     *
     * @return RoadEdge's priority factor.
     */
    public float priority() {
        return base.priority();
    }

    /**
     * Gets road's maximum speed in kilometers per hour.
     *
     * @return Maximum speed in kilometers per hour.
     */
    public float maxSpeed() {
        return base.maxSpeed(heading);
    }

    /**
     * Gets road length in meters.
     *
     * @return RoadEdge length in meters.
     */
    public float length() {
        return base.length();
    }

    /**
     * Gets road {@link Heading} relative to its {@link BaseRoad}.
     *
     * @return RoadEdge's {@link Heading} relative to its {@link BaseRoad}.
     */
    public Heading heading() {
        return heading;
    }

    /**
     * Gets road's geometry as a {@link Polyline} from the road's source to its target.
     *
     * @return RoadEdge's geometry as {@link Polyline} from source to target.
     */
    public Polyline geometry() {
        return heading == Heading.forward ? base.geometry() : invert(base.geometry());
    }

    /**
     * Gets referred {@link BaseRoad} object.
     *
     * @return {@link BaseRoad} object.
     */
    public BaseRoad base() {
        return base;
    }

    /**
     * Gets a JSON representation of the {@link RoadEdge}.
     *
     * @return {@link JSONObject} object.
     * @throws JSONException thrown on JSON extraction or parsing error.
     */
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("road", base().id());
        json.put("heading", heading());
        return json;
    }

    /**
     * Creates a {@link Route} object from its JSON representation.
     *
     * @param json JSON representation of the {@link Route}.
     * @param map {@link RoadMap} object as the reference of {@link RoadPoint}s and {@link RoadEdge}s.
     * @return {@link RoadEdge} object.
     * @throws JSONException thrown on JSON extraction or parsing error.
     */
    public static RoadEdge fromJSON(JSONObject json, RoadMap map) throws JSONException {
        long baseid = json.getLong("road");
        RoadEdge road = map.get(Heading.valueOf(json.getString("heading")) == Heading.forward
                ? baseid * 2 : baseid * 2 + 1);
        if (road == null) {
            throw new JSONException("road id " + json.getLong("road") + " not found");
        }
        return road;
    }
}
