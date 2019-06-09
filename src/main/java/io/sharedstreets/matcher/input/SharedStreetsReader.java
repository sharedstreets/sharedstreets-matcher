package io.sharedstreets.matcher.input;

import com.esri.core.geometry.Polyline;
import io.sharedstreets.barefoot.road.BaseRoad;
import io.sharedstreets.barefoot.road.RoadReader;
import io.sharedstreets.barefoot.roadmap.RoadMap;
import io.sharedstreets.barefoot.spatial.Geography;
import io.sharedstreets.barefoot.spatial.SpatialOperator;
import io.sharedstreets.barefoot.util.SourceException;
import com.esri.core.geometry.Polygon;
import io.sharedstreets.matcher.input.model.SharedStreetGeometry;
import io.sharedstreets.matcher.output.tiles.TileId;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;


public class SharedStreetsReader implements RoadReader {

    private static final Logger logger = LoggerFactory.getLogger(SharedStreetsReader.class);

    private final static SpatialOperator spatial = new Geography();

    private Path tilePath;
    private Iterator<Path> fileIterator;
    private SharedStreetsGeometryIterator currentTileIterator = null;
    private String currentTileName  = "";

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

    public SharedStreetsReader(String tmpTilePath, String tileServer, String tileSource, Integer roadClass, TileId tileId) throws IOException {

        String tileFileName = tileId.toString() + ".geometry."+ roadClass +".pbf";

        File tileTempFile = new File(tmpTilePath, tileFileName);

        if(!tileTempFile.exists()) {

            new File(tmpTilePath).mkdirs();

            logger.info("loaded tile:  {} {}", tileSource, tileFileName);

            URL tileUrl = new URL(tileServer + tileSource + "/" + tileFileName);
            FileUtils.copyURLToFile(tileUrl, tileTempFile);

        }

        this.tilePath = tileTempFile.toPath();
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void open() throws SourceException {
        try {

            if(tilePath.toFile().isDirectory()) {
                Stream<Path> fileStream = Files.list(tilePath);
                fileIterator = fileStream.iterator();
            }
            else {
                ArrayList<Path> files = new ArrayList();
                files.add(tilePath);
                fileIterator  = files.iterator();
            }

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

                while(!nextFile.getFileName().toString().contains(".geometry.") && fileIterator.hasNext())
                    nextFile = fileIterator.next();

                if(!nextFile.getFileName().toString().contains(".geometry.") )
                    nextFile = null;

                if (nextFile != null) {
                    currentTileName = nextFile.getFileName().toString();
                    InputStream is = new FileInputStream(nextFile.toFile());


                    this.currentTileIterator = new SharedStreetsGeometryIterator(is);
                }
            }

        }
        catch (IOException ex) {
            System.err.print("Could not load tiles: " + ex.getLocalizedMessage());
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

        BaseRoad baseRoad = null;

        while (currentTileIterator != null) {

            try {
                while(currentTileIterator.hasNext()){


                    SharedStreetGeometry geom = currentTileIterator.next();

                    // only return unindexed roads
                    if(!BaseRoad.IDs.hasId(geom.id)) {
                        int speed = geom.getSpeed();
                        float priority = geom.getPriority();

                        float length = (float) spatial.length(geom.geometry);

                        baseRoad = new BaseRoad(geom.id, geom.startIntersectionId, geom.endIntersectionId, geom.forwardReferenceId, geom.backReferenceId,
                                (short)geom.roadClass.getValue(), priority, speed, length, geom.geometry);

                        return baseRoad;
                    }

                }
            }
            catch (IOException ex) {
                System.err.print("Could not load tiles: " + ex.getLocalizedMessage() + " (" + this.currentTileName + ")\n");
            }

            openNextTile();
        }

        return null;
    }
}
