package io.sharedstreets.matcher.ingest.input.gpx;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

import io.sharedstreets.matcher.ingest.model.InputEvent;
import io.sharedstreets.matcher.ingest.model.Point;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.*;
import org.apache.flink.core.fs.local.LocalFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GpxInputFormat extends FileInputFormat<InputEvent> {

    static Logger logger = LoggerFactory.getLogger(GpxInputFormat.class);

    boolean gpsSpeed;
    boolean verbose;

    public GpxInputFormat(String path, boolean gpsSpeed, boolean verbose) {
        super();
        this.filePath = new Path(path);
        this.gpsSpeed = gpsSpeed;
        this.verbose = verbose;
    }

    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {

        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        // take the desired number of splits into account
        minNumSplits = Math.max(minNumSplits, this.numSplits);

        final Path path = this.filePath;

        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

        final FileSystem fs = this.filePath.getFileSystem();
        int splitNum = 0;
        File dir = new File(path.getPath());

        for (File f : dir.listFiles()) {

            FileStatus file =  new LocalFileStatus(f, fs);
            FileInputSplit split = new FileInputSplit(splitNum++, file.getPath(), 0, file.getLen(),
                    null);
            inputSplits.add(split);

        }
        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);

    }

    GpxContentHandler gpxParser;
    Integer totalPoints = 0;
    Integer currentPoint = 0;
    String id;

    @Override
    public void open(FileInputSplit fileSplit) {
        try {
            super.open(fileSplit);
            // TODO move to stream processing of XML
            String fileParts[] = fileSplit.getPath().getName().split("\\.");

            if(fileParts.length == 2)
                id = fileParts[0];
            else
                id = UUID.randomUUID().toString();

            gpxParser = new GpxContentHandler();
            GpxParser.parseGpx(this.stream, gpxParser);
            totalPoints = gpxParser.getPointLists().size();

            currentPoint = 0;
            logger.info(fileSplit.toString() + " (" + totalPoints + ")");
        }
        catch(Exception e) {
            logger.error("unable to parse: " + fileSplit.toString());
        }
    }



    @Override
    public boolean reachedEnd() throws IOException {
        return gpxParser == null || currentPoint + 1 >= totalPoints ;
    }

    @Override
    public InputEvent nextRecord(InputEvent reuse) throws IOException {
        LatLon latLon = gpxParser.getPointLists().get(currentPoint);
        reuse.point = new Point(latLon.getLon(), latLon.getLat());
        reuse.time = latLon.getTime();
        reuse.vehicleId = id;

        if(latLon.getName() != null || latLon.getSpeed() != null) {
            reuse.eventData = new HashMap<>();

            if(latLon.getName() != null)
                reuse.eventData.put(latLon.getName(), null);

            if(gpsSpeed && latLon.getSpeed() != null)
                reuse.eventData.put("gpsSpeed", latLon.getSpeed());
        }

        currentPoint++;

        return reuse;
    }



}