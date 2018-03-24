package io.sharedstreets.barefoot.road;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Polyline;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;

import java.io.IOException;


public class GeometryJSONEncoder implements Encoder {

    @Override
    public void encode(Object obj, JsonStream stream) throws IOException {
        if(obj.getClass().equals(Polyline.class)){
            Polyline geom = (Polyline)obj;

            stream.writeObjectStart();

            stream.writeObjectField("type");
                stream.writeVal("Feature");
                stream.writeMore();

                stream.writeObjectField("properties");
                stream.writeObjectStart();

                    stream.writeObjectField("id");
                    stream.writeVal("test");

                stream.writeObjectEnd();
                stream.writeMore();

                stream.writeObjectField("geometry");
                stream.writeObjectStart();

                    stream.writeObjectField("type");
                    stream.writeVal("LineString");
                    stream.writeMore();

                    stream.writeObjectField("coordinates");
                    stream.writeArrayStart();
                        int pointCount = ((Polyline)geom).getPointCount();
                        for(int i = 0; i < pointCount; i++) {
                            com.esri.core.geometry.Point point = ((Polyline)geom).getPoint(i);
                            stream.writeArrayStart();
                                stream.writeVal(point.getX());
                                stream.writeMore();
                                stream.writeVal(point.getY());
                            stream.writeArrayEnd();
                            if(i + 1 < pointCount)
                                stream.writeMore();
                        }

                    stream.writeArrayEnd();
                stream.writeObjectEnd();
            stream.writeObjectEnd();

        }
    }
}