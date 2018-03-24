package io.sharedstreets.matcher.ingest.input.gpx;

/*
 *  Copyright 2013 Martin Ždila, Freemap Slovakia
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

class GpxParser {

    static void parseGpx(final InputStream stream, final GpxContentHandler dh) throws IOException {
        final SAXParser saxParser;
        try {
            saxParser = SAXParserFactory.newInstance().newSAXParser();
        } catch (final ParserConfigurationException e) {
            throw new RuntimeException("can't create XML parser", e);
        } catch (final SAXException e) {
            throw new RuntimeException("can't create XML parser", e);
        }

        try {
            try {

                try {
                    saxParser.parse(stream, dh);
                } catch (final SAXException e) {
                    throw new IOException("error parsing input GPX file", e);
                } catch (final RuntimeException e) {
                    throw new RuntimeException("internal error when parsing GPX file", e);
                } finally {

                }
            } finally {

            }
        } catch (final IOException e) {
            throw new IOException("error reading input file", e);
        }
    }



}