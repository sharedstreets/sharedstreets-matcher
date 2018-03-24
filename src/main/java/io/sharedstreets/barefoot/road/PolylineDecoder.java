package io.sharedstreets.barefoot.road;

import com.esri.core.geometry.Polyline;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.Decoder;

import java.io.IOException;


public class PolylineDecoder implements Decoder {
    @Override
    public Object decode(JsonIterator iter) throws IOException {
        Polyline geometry = new Polyline();

        boolean firstPoint = true;
        for (String field = iter.readObject(); field != null; field = iter.readObject()) {
            if (field.equals("coordinates")) {
                while (iter.readArray()) {
                    iter.readArray();
                    if (firstPoint) {
                        double x = iter.readDouble();
                        iter.readArray();
                        double y = iter.readDouble();
                        geometry.startPath(x, y);
                    }
                    else {
                        double x = iter.readDouble();
                        iter.readArray();
                        double y = iter.readDouble();
                        geometry.lineTo(x, y);
                    }
                    firstPoint = false;
                    iter.readArray();
                }
            } else
                iter.read();
        }

        return geometry;
    }
}
