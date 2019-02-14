package io.sharedstreets.matcher.ingest.input.json;

import io.sharedstreets.matcher.ingest.model.InputEvent;
import io.sharedstreets.matcher.ingest.model.JsonInputObject;
import io.sharedstreets.matcher.ingest.model.Point;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileStatus;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonInputFormat extends FileInputFormat<InputEvent> {

    private final AtomicInteger numEvents = new AtomicInteger(0);
    private final AtomicInteger currentEventIndex = new AtomicInteger(0);
    private static JsonInputObject parsedFileContents = new JsonInputObject();

    public JsonInputFormat(Path filePath) {
        super(filePath);
    }

    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        // take the desired number of splits into account
        minNumSplits = Math.max(minNumSplits, this.numSplits);

        final List<FileInputSplit> inputSplits = new ArrayList<>(minNumSplits);
        final FileSystem fs = filePath.getFileSystem();
        File dir = new File(filePath.getPath());
        File[] files = dir.listFiles();

        if (files != null) {
            int splitNum = 0;

            for (File f : files) {
                FileStatus file = new LocalFileStatus(f, fs);
                FileInputSplit split = new FileInputSplit(splitNum++, file.getPath(), 0, file.getLen(), null);
                inputSplits.add(split);
            }
        }

        return inputSplits.toArray(new FileInputSplit[0]);
    }

    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        super.open(fileSplit);
        String pathToFile = filePath + "/" + fileSplit.getPath().getName();

        JsonInputObject result = JsonParser.parseJson(pathToFile);

        if (result != null) {
            parsedFileContents = result;
            numEvents.compareAndSet(0, result.eventData.size());
        }
    }

    @Override
    public boolean reachedEnd() {
        return currentEventIndex.get() >= numEvents.get();
    }

    @Override
    public InputEvent nextRecord(InputEvent reuse) {
        int index = incrementEventIndex();

        reuse.vehicleId = parsedFileContents.eventData.get(index).vehicleId;
        reuse.time = parsedFileContents.eventData.get(index).timeStamp;
        reuse.point = new Point(parsedFileContents.eventData.get(index).longitude, parsedFileContents.eventData.get(index).latitude);

        if (parsedFileContents.eventData.get(index).eventType != null) {
            reuse.eventData = parsedFileContents.eventData.get(index).eventType;
        }

        return reuse;
    }

    private synchronized int incrementEventIndex() {
        return currentEventIndex.getAndIncrement();
    }
}
