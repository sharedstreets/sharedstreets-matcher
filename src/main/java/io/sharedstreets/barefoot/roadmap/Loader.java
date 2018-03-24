/*
 * Copyright (C) 2016, BMW Car IT GmbH
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

import io.sharedstreets.barefoot.util.SourceException;
import io.sharedstreets.barefoot.util.Tuple;
import io.sharedstreets.matcher.input.SharedStreetsReader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Standard map loader that loads road map from database connection or file buffer.
 */
public class Loader {
    private static Logger logger = LoggerFactory.getLogger(Loader.class);


    public static RoadMap roadmap(String tilePath) throws SourceException {

        File file = new File(tilePath);

        if(!file.exists() && !file.isDirectory())
            throw new SourceException("tile path invalid: " + tilePath);

        RoadMap map = RoadMap.Load(new SharedStreetsReader(file.toPath()));

        return map;
    }

    /**
     * Reads road type configuration from file.
     *
     * @param path Path of the road type configuration file.
     * @return Mapping of road class identifiers to priority factor and default maximum speed.
     * @throws JSONException thrown on JSON extraction or parsing error.
     * @throws IOException thrown on file reading error.
     */
    public static Map<Short, Tuple<Double, Integer>> read(String path)
            throws JSONException, IOException {
        BufferedReader file = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

        String line = null, json = new String();
        while ((line = file.readLine()) != null) {
            json += line;
        }
        file.close();

        return roadtypes(new JSONObject(json));
    }

    /**
     * Reads road type configuration from JSON representation.
     *
     * @param jsonconfig JSON representation of the road type configuration.
     * @return Mapping of road class identifiers to priority factor and default maximum speed.
     * @throws JSONException thrown on JSON extraction or parsing error.
     */
    public static Map<Short, Tuple<Double, Integer>> roadtypes(JSONObject jsonconfig)
            throws JSONException {

        Map<Short, Tuple<Double, Integer>> config = new HashMap<>();

        JSONArray jsontags = jsonconfig.getJSONArray("tags");
        for (int i = 0; i < jsontags.length(); ++i) {
            JSONObject jsontag = jsontags.getJSONObject(i);
            JSONArray jsonvalues = jsontag.getJSONArray("values");
            for (int j = 0; j < jsonvalues.length(); ++j) {
                JSONObject jsonvalue = jsonvalues.getJSONObject(j);
                config.put((short) jsonvalue.getInt("id"),
                        new Tuple<>(jsonvalue.getDouble("priority"), jsonvalue.getInt("maxspeed")));
            }
        }

        return config;
    }

}
