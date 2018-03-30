package io.sharedstreets.matcher.output.tiles;

import io.sharedstreets.matcher.model.Week;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class TiledNIOFileOutputFormat<IT> extends RichOutputFormat<IT> {

    static Logger LOG = LoggerFactory.getLogger(TiledNIOFileOutputFormat.class);

    static int MAX_FILES = 1024;

    protected class NIOFileObject {

        FileOutputStream stream = null;
        FileChannel channel = null;

        NIOFileObject(String filePath) throws IOException {

            try {
                stream = new FileOutputStream(filePath, true);
                channel = stream.getChannel();
            } catch(Exception ex) {

                LOG.error(ex.getLocalizedMessage());

                if (channel != null)
                    channel.close();
                if (stream != null)
                    stream.close();
            }
        }

        void write(byte[] data) throws IOException {
            stream.write(data);
            stream.flush();
        }

        void close() throws IOException {


            if (channel != null)
                channel.close();
            if (stream != null)
                stream.close();


        }

    }

    private class LRUFileCache<K extends String, V extends NIOFileObject> extends LinkedHashMap<String, NIOFileObject> {

        private int cacheSize;
        Path filePath;

        public LRUFileCache(Path filePath, int cacheSize) {
            super(16, 0.75f, true);
            this.filePath = filePath;
            this.cacheSize = 100;
        }


        @Override
        public NIOFileObject get(Object key) {
            synchronized (this) {
                if(!this.containsKey(key)) {
                    try {
                        NIOFileObject file = new NIOFileObject((String)key);
                        this.put((String) key, file);
                    } catch(Exception ex){
                        LOG.error(ex.getLocalizedMessage());
                    }
                }

                return super.get(key);
            }
        }


        @Override
        protected boolean removeEldestEntry(Map.Entry<String, NIOFileObject> eldest) {
            synchronized (this) {

                if (size() >= cacheSize) {
                    try {
                        eldest.getValue().close();
                    } catch (Exception ex) {
                        LOG.error(ex.getLocalizedMessage());
                    }

                    return true;
                } else
                    return false;
            }
        }
    }

    LRUFileCache fileCache;

    String fileType;

    String outputFilePath;

    public TiledNIOFileOutputFormat(String outputPath, String fileType) {
        this.outputFilePath = outputPath;
        this.fileType = fileType;
    }


    @Override
    public void configure(Configuration parameters) {

        // no-op

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        // init filesystem

        Path outputPath = FileSystems.getDefault().getPath(this.outputFilePath);

        if(!outputPath.toFile().exists()){
            outputPath.toFile().mkdirs();
        }

        this.fileCache = new LRUFileCache(outputPath, MAX_FILES / numTasks);
    }

    public void writeRecord(TileId tileId, Week week, String recordType, byte[] data) throws IOException {

        String weekString = "";
        if(week != null)
            weekString = week.toString();

        Path directoryPath = FileSystems.getDefault().getPath(this.outputFilePath,  weekString, recordType);

        if(!directoryPath.toFile().exists()){
            directoryPath.toFile().mkdirs();
        }

        Path filePath = Paths.get(directoryPath.toString(), (String)tileId.toString()  + "." + recordType + "." + fileType);

        String key = filePath.toAbsolutePath().toString();

        fileCache.get(key).write(data);
    }


    @Override
    public void close() throws IOException {

        for(Object file : fileCache.values()) {

            ((NIOFileObject)file).close();

        }
    }
}
