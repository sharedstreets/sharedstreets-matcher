package io.sharedstreets.matcher.ingest.input;

import com.google.gson.Gson;

import java.io.*;

public class JsonParser {

    static JsonDTO parseJson(final String pathToFile) {
        JsonDTO result = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(pathToFile)))) {

            Gson gson = new Gson();
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            reader.close();

            result = gson.fromJson(builder.toString(), JsonDTO.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }
}
