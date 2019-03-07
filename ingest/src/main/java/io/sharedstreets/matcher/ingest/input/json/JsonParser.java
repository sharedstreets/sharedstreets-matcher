package io.sharedstreets.matcher.ingest.input.json;

import com.google.gson.Gson;
import io.sharedstreets.matcher.ingest.model.JsonInputObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

class JsonParser {

    private static Logger logger = LoggerFactory.getLogger(JsonParser.class);

    static JsonInputObject parseJson(final String pathToFile) {
        if (pathToFile == null || pathToFile.isEmpty()) {
            logger.error("Path is empty or null when parsing Json File");
            return null;
        }
        JsonInputObject result = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(new File(pathToFile)))) {

            Gson gson = new Gson();
            StringBuilder builder = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            reader.close();

            result = gson.fromJson(builder.toString(), JsonInputObject.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }
}
