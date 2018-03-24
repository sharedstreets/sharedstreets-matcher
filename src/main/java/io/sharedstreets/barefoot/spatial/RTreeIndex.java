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

package io.sharedstreets.barefoot.spatial;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.barefoot.road.Heading;
import io.sharedstreets.barefoot.roadmap.RoadEdge;
import io.sharedstreets.barefoot.roadmap.RoadPoint;
import io.sharedstreets.barefoot.util.Tuple;

import java.util.*;

public class RTreeIndex  {
    private final SpatialOperator spatial;
    private final static int height = 16;
    private transient RTree<BaseRoad, Rectangle> index = null;

    public RTreeIndex() {
        spatial = new Geography();
        index =  RTree.star().maxChildren(6).create();
    }

    public void add(BaseRoad road ) {
        Envelope env = new Envelope();
        road.geometry().queryEnvelope(env);

        Rectangle rect = Geometries.rectangleGeographic(env.getXMin(), env.getYMin(), env.getXMax(), env.getYMax());

        index = index.add(road, rect);
    }


    public Set<RoadPoint> nearest(Point c) {
        Set<RoadPoint> nearest = new HashSet<>();
        double radius = 100, min = Double.MAX_VALUE;

        if (index.size() == 0) {
            return nearest;
        }

        do {
            Rectangle rect = spatial.envelope(c, radius);


            Iterator<Entry<BaseRoad, Rectangle>> it = index.search(rect).toBlocking().getIterator();

            while(it.hasNext()) {
                Entry<BaseRoad, Rectangle> entry = it.next();

                double f = spatial.intercept(entry.value().geometry(), c);
                Point p = spatial.interpolate(entry.value().geometry(), entry.value().length(), f);
                double d = spatial.distance(p, c);

                if (d > min) {
                    continue;
                }

                if (d < min) {
                    min = d;
                    nearest.clear();
                }

                for(RoadEdge roadEdge : entry.value().getEdges()) {
                    if(roadEdge.heading() == Heading.forward)
                        nearest.add(new RoadPoint(roadEdge, f));
                    else
                        nearest.add(new RoadPoint(roadEdge, 1.0 - f));
                }
            }

            radius *= 2;

        } while(nearest.isEmpty());

        return nearest;
    }

    public Set<RoadPoint> radius(Point c, double radius) {
        Set<RoadPoint> neighbors = new HashSet<>();

        if (index.size() == 0) {
            return neighbors;
        }

        Rectangle rect = spatial.envelope(c, radius);

        Iterator<Entry<BaseRoad, Rectangle>> it = index.search(rect).toBlocking().getIterator();

        while(it.hasNext()) {
            Entry<BaseRoad, Rectangle> entry = it.next();

            double f = spatial.intercept(entry.value().geometry(), c);
            Point p = spatial.interpolate(entry.value().geometry(), entry.value().length(), f);
            double d = spatial.distance(p, c);

            if (d > radius) {
                continue;
            }

            for(RoadEdge roadEdge : entry.value().getEdges()) {
                if(roadEdge.heading() == Heading.forward)
                    neighbors.add(new RoadPoint(roadEdge, f));
                else
                    neighbors.add(new RoadPoint(roadEdge, 1.0 - f));
            }


        }

        return neighbors;
    }


    public Set<RoadPoint> knearest(Point c, int k) {
        Set<RoadPoint> neighbors = new HashSet<>();

        if (index.size() == 0) {
            return neighbors;
        }

        Set<Long> visited = new HashSet<>();

        PriorityQueue<Tuple<RoadPoint, Double>> queue =
                new PriorityQueue<>(k,
                        new Comparator<Tuple<RoadPoint, Double>>() {
                            @Override
                            public int compare(Tuple<RoadPoint, Double> left,
                                               Tuple<RoadPoint, Double> right) {
                                return left.two() < right.two() ? -1
                                        : left.two() > right.two() ? +1 : 0;
                            }
                        });

        double radius = 100;

        do {
            Rectangle rect = spatial.envelope(c, radius);


            Iterator<Entry<BaseRoad, Rectangle>> it = index.search(rect).toBlocking().getIterator();

            while(it.hasNext()) {
                Entry<BaseRoad, Rectangle> entry = it.next();

                if (visited.contains(entry.value().id())) {
                    continue;
                }

                double f = spatial.intercept(entry.value().geometry(), c);
                Point p = spatial.interpolate(entry.value().geometry(), entry.value().length(), f);
                double d = spatial.distance(p, c);


                if (d < radius) { // Only within radius, we can be sure that we have semantically
                                  // correct k-nearest neighbors.

                    for(RoadEdge roadEdge : entry.value().getEdges()) {
                        if(roadEdge.heading() == Heading.forward)
                            queue.add(new Tuple<RoadPoint, Double>(new RoadPoint(roadEdge, f), d));
                        else
                            queue.add(new Tuple<RoadPoint, Double>(new RoadPoint(roadEdge, 1.0 - f), d));
                    }

                    visited.add(entry.value().id());
                }
            }

            radius *= 2;

        } while (queue.size() < k);

        Set<RoadPoint> result = new HashSet<>();

        while (result.size() < k) {
            Tuple<RoadPoint, Double> e = queue.poll();
            result.add(e.one());
        }

        return result;
    }
}
