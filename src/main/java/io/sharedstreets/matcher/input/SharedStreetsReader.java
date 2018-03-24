package io.sharedstreets.matcher.input;

import com.esri.core.geometry.Polyline;
import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.barefoot.road.RoadReader;
import io.sharedstreets.barefoot.spatial.Geography;
import io.sharedstreets.barefoot.spatial.SpatialOperator;
import io.sharedstreets.barefoot.util.SourceException;
import com.esri.core.geometry.Polygon;
import io.sharedstreets.matcher.input.model.SharedStreetGeometry;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;


public class SharedStreetsReader implements RoadReader {

    private final static SpatialOperator spatial = new Geography();

    private Path tilePath;
    private Iterator<Path> fileIterator;
    private SharedStreetsGeometryIterator currentTileIterator = null;

    class SharedStreetsGeometryIterator {

        InputStream is;

        SharedStreetsGeometryIterator(InputStream is) {
            this.is = is;
        }

        void close() throws IOException {
            this.is.close();
        }

        boolean hasNext() throws IOException {
            if(is.available() > 0)
                return true;
            else
                return false;
        }

        SharedStreetGeometry next() throws IOException {

            SharedStreetGeometry geometry = null;

            SharedStreetsProto.SharedStreetsGeometry protobuf = SharedStreetsProto.SharedStreetsGeometry.parseDelimitedFrom(is);

            if(protobuf != null) {
                geometry = new SharedStreetGeometry();
                geometry.id = protobuf.getId();
                geometry.forwardReferenceId = protobuf.getForwardReferenceId();
                geometry.backReferenceId = protobuf.getBackReferenceId();
                geometry.startIntersectionId = protobuf.getFromIntersectionId();
                geometry.endIntersectionId = protobuf.getToIntersectionId();
                geometry.roadClass = SharedStreetGeometry.ROAD_CLASS.forInt(protobuf.getRoadClassValue());

                Double lat = null, lon = null;
                Polyline polyline = new Polyline();
                boolean firstPoint = true;

                for(double lonLat : protobuf.getLonlatsList()) {

                    if(lon == null)
                        lon = lonLat;
                    else if(lat == null)
                        lat = lonLat;

                    if(lat != null && lon !=null) {
                        if(firstPoint)
                            polyline.startPath(lon, lat);
                        else
                            polyline.lineTo(lon, lat);

                        firstPoint = false;

                        lat = null;
                        lon = null;
                    }
                }

                geometry.geometry = polyline;
            }

            return geometry;
        }

    }

    public SharedStreetsReader(Path tilePath) {

        this.tilePath = tilePath;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void open() throws SourceException {
        try {

            Stream<Path> fileStream = Files.list(tilePath);
            fileIterator = fileStream.iterator();

            openNextTile();

        } catch (IOException ex) {
            throw new SourceException("Could not load tiles: " + ex.getLocalizedMessage());
        }

    }

    private void openNextTile() {

        try {

            if(this.currentTileIterator != null)
                this.currentTileIterator.close();

            this.currentTileIterator = null;

            if( fileIterator.hasNext() ) {
                Path nextFile = fileIterator.next();

                while(!nextFile.getFileName().toString().endsWith("geometry.pbf") && fileIterator.hasNext())
                    nextFile = fileIterator.next();

                if(!nextFile.getFileName().toString().endsWith("geometry.pbf") )
                    nextFile = null;

                if (nextFile != null) {
                    InputStream is = new FileInputStream(nextFile.toFile());

                    this.currentTileIterator = new SharedStreetsGeometryIterator(is);
                }
            }

        }
        catch (IOException ex) {
            throw new SourceException("Could not load tiles: " + ex.getLocalizedMessage());
        }
    }

    @Override
    public void open(Polygon polygon, HashSet<Short> exclusion) throws SourceException {
        this.open();
    }

    @Override
    public void close() throws SourceException {

    }

    @Override
    public BaseRoad next() throws SourceException {
        try {
            BaseRoad baseRoad = null;

            while (currentTileIterator != null) {

                while(currentTileIterator.hasNext()){

                    SharedStreetGeometry geom = currentTileIterator.next();

                    int speed = geom.getSpeed();
                    float priority = geom.getPriority();

                    float length = (float) spatial.length(geom.geometry);

                    baseRoad = new BaseRoad(geom.id, geom.startIntersectionId, geom.endIntersectionId, geom.forwardReferenceId, geom.backReferenceId,
                            (short)geom.roadClass.getValue(), priority, speed, length, geom.geometry);

                    return baseRoad;
                }

                openNextTile();
            }

        }
        catch (IOException ex) {
            throw new SourceException("Could not load tiles: " + ex.getLocalizedMessage());
        }

        return null;
    }
}
