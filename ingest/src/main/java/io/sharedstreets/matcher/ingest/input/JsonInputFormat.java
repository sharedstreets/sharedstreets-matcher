package io.sharedstreets.matcher.ingest.input;

import io.sharedstreets.matcher.ingest.model.InputEvent;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class JsonInputFormat extends FileInputFormat<InputEvent> {

    private final AtomicInteger numFiles = new AtomicInteger(0);
    private final AtomicInteger currentFile = new AtomicInteger(0);
    private static AtomicReference<JsonDTO> fileContents = new AtomicReference<>(new JsonDTO());

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

        final Path path = this.filePath;

        final List<FileInputSplit> inputSplits = new ArrayList<>(minNumSplits);

        final FileSystem fs = this.filePath.getFileSystem();
        int splitNum = 0;
        File dir = new File(path.getPath());
        File[] files = dir.listFiles();

        if (files != null) {
            for (File f : files) {

                FileStatus file = new LocalFileStatus(f, fs);
                FileInputSplit split = new FileInputSplit(splitNum++, file.getPath(), 0, file.getLen(),
                        null);
                inputSplits.add(split);

            }
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }

    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        super.open(fileSplit);
        String pathToFile = filePath + "/" + fileSplit.getPath().getName();

        JsonDTO result = JsonParser.parseJson(pathToFile);
        fileContents = new AtomicReference<>(result);
        numFiles.compareAndSet(0, result.eventData.size());
    }

    @Override
    public boolean reachedEnd() {
        return currentFile.get() >= numFiles.get();
    }

    @Override
    public InputEvent nextRecord(InputEvent reuse) {
        int index = foo();

        reuse.vehicleId = fileContents.get().eventData.get(index).vehicleId;
        reuse.time = fileContents.get().eventData.get(index).timeStamp;
        reuse.point = new Point(fileContents.get().eventData.get(index).longitude, fileContents.get().eventData.get(index).latitude);

        if (fileContents.get().eventData.get(index).eventType != null) {
            HashMap<String, Double> event = new HashMap<>();
            event.put(fileContents.get().eventData.get(index).eventType, 0.0);
            reuse.eventData = event;
        }

        return reuse;
    }

    synchronized int foo() {
        return currentFile.getAndIncrement();
    }
}
