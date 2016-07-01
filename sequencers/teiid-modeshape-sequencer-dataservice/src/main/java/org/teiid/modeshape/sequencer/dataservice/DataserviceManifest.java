/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.modeshape.sequencer.dataservice;

import java.io.InputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;
import org.modeshape.jcr.api.sequencer.Sequencer.Context;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;

/**
 * The POJO for the Dataservice manifest file.
 */
public class DataserviceManifest implements Comparable<DataserviceManifest> {

    static final Logger LOGGER = Logger.getLogger(DataserviceManifest.class);

	private String serviceVdbName;

	public static DataserviceManifest read( final InputStream stream,
                                    final Context context ) throws Exception {

        return new Reader().read(stream, context);
    }

    public String getServiceVdbName() {
		return serviceVdbName;
	}

	public void setServiceVdbName(String serviceVdbName) {
		this.serviceVdbName = serviceVdbName;
	}

    /**
     * Constructor
     */
    public DataserviceManifest( ) {
    }

    /**
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo( final DataserviceManifest that ) {
        CheckArg.isNotNull(that, "that");

        if (this == that) {
            return 0;
        }

        return 0;
    }

    protected static class Reader {
        private DataserviceManifest parseDataservice( final XMLStreamReader streamReader ) throws Exception {
            assert DataVirtLexicon.ManifestIds.DATASERVICE.equals(streamReader.getLocalName());

            // collect VDB attributes
            final DataserviceManifest manifest = new DataserviceManifest();
            assert (manifest != null) : "manifest is null";

            // collect children
            while (streamReader.hasNext()) {
                streamReader.next(); // point to next element

                if (streamReader.isStartElement()) {
                    final String elementName = streamReader.getLocalName();

                    if (DataVirtLexicon.ManifestIds.SERVICE_VDB.equals(elementName)) {
                        final String serviceVdb = streamReader.getElementText();
                        manifest.setServiceVdbName(serviceVdb);
                    } else {
                        LOGGER.debug("**** unexpected Dataservice element={0}", elementName);
                    }
                } else if (streamReader.isEndElement() && DataVirtLexicon.ManifestIds.DATASERVICE.equals(streamReader.getLocalName())) {
                    break;
                }
            }

            return manifest;
        }

        public DataserviceManifest read( final InputStream stream,
                                 final Context context ) throws Exception {
            DataserviceManifest manifest = null;
            final XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader streamReader = null;

            try {
                streamReader = factory.createXMLStreamReader(stream);

                if (streamReader.hasNext()) {
                    if (streamReader.next() == XMLStreamConstants.START_ELEMENT) {
                        final String elementName = streamReader.getLocalName();

                        if (DataVirtLexicon.ManifestIds.DATASERVICE.equals(elementName)) {
                            manifest = parseDataservice(streamReader);
                            assert (manifest != null) : "manifest is null";
                        } else {
                            LOGGER.debug("**** unhandled vdb read element ****");
                        }
                    }
                }
            } finally {
                if (streamReader != null)
                    streamReader.close();
            }

            return manifest;
        }
    }
}
