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
 * The POJO for the Datasource file.
 * 
 * Format of the datasource xml :
 *  <dataSourceSet>
 *       <dataSource name="mySource" jdbc="false">
 *           <property name="prop2">prop2Value</property>
 *           <property name="preview">true</property>
 *           <property name="profileName">dsProfileName</property>
 *           <property name="prop1">prop1Value</property>
 *           <property name="jndiName">java:/jndiName</property>
 *           <property name="driverName">dsDriver</property>
 *           <property name="className">dsClassname</property>
 *       </dataSource>
 *   </dataSourceSet>
 */
public class DataserviceDatasource implements Comparable<DataserviceDatasource> {

    static final Logger LOGGER = Logger.getLogger(DataserviceDatasource.class);

    public static DataserviceDatasource read( final InputStream stream,
                                    final Context context ) throws Exception {

        return new Reader().read(stream, context);
    }

    public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isJdbc() {
		return jdbc;
	}

	public void setJdbc(boolean jdbc) {
		this.jdbc = jdbc;
	}


	private String name;
    private boolean jdbc;

    /**
     * Constructor
     */
    public DataserviceDatasource( ) {
    }

    /**
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo( final DataserviceDatasource that ) {
        CheckArg.isNotNull(that, "that");

        if (this == that) {
            return 0;
        }

        return 0;
    }

    protected static class Reader {
        private DataserviceDatasource parseDatasource( final XMLStreamReader streamReader ) throws Exception {
            assert DataVirtLexicon.ManifestIds.DATASOURCE_SET.equals(streamReader.getLocalName());

            // collect VDB attributes
            final DataserviceDatasource ds = new DataserviceDatasource();
            assert (ds != null) : "datasource is null";

            // collect children
            while (streamReader.hasNext()) {
                streamReader.next(); // point to next element

                if (streamReader.isStartElement()) {
                    final String elementName = streamReader.getLocalName();

                    if (DataVirtLexicon.ManifestIds.DATASOURCE.equals(elementName)) {
                    	String dsName = "temp";
                    	ds.setName(dsName);
                    	boolean isJdbc = true;
                    	ds.setJdbc(isJdbc);
                    	continue;
                    } else if (DataVirtLexicon.ManifestIds.PROPERTY.equals(elementName)) {
                        final String propValue = streamReader.getElementText();
                        //manifest.setServiceVdbName(serviceVdb);
                    } else {
                        LOGGER.debug("**** unexpected Dataservice element={0}", elementName);
                    }
                } else if (streamReader.isEndElement() && DataVirtLexicon.ManifestIds.DATASERVICE.equals(streamReader.getLocalName())) {
                    break;
                }
            }

            return ds;
        }

        public DataserviceDatasource read( final InputStream stream,
                                final Context context ) throws Exception {
            DataserviceDatasource datasource = null;
            final XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader streamReader = null;

            try {
                streamReader = factory.createXMLStreamReader(stream);

                if (streamReader.hasNext()) {
                    if (streamReader.next() == XMLStreamConstants.START_ELEMENT) {
                        final String elementName = streamReader.getLocalName();

                        if (DataVirtLexicon.ManifestIds.DATASOURCE_SET.equals(elementName)) {
                        	datasource = parseDatasource(streamReader);
                            assert (datasource != null) : "datasource is null";
                        } else {
                            LOGGER.debug("**** unhandled datasource read element ****");
                        }
                    }
                }
            } finally {
                if (streamReader != null)
                    streamReader.close();
            }

            return datasource;
        }
    }
}
