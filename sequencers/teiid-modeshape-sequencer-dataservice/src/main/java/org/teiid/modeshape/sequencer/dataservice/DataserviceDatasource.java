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

import java.util.Properties;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;

/**
 * The POJO for the Datasource file.
 * 
 * Format of the datasource xml :
 *  <dataSourceSet>
 *       <dataSource name="mySource" type="RESOURCE">
 *           <property name="prop2">prop2Value</property>
 *           <property name="prop1">prop1Value</property>
 *           <property name="jndiName">java:/jndiName</property>
 *           <property name="driverName">dsDriver</property>
 *           <property name="className">dsClassname</property>
 *       </dataSource>
 *   </dataSourceSet>
 */
public class DataserviceDatasource implements Comparable<DataserviceDatasource> {

    static final Logger LOGGER = Logger.getLogger(DataserviceDatasource.class);

    /**
     * Datasource types
     */
    public enum Type {
        /**
         * JDBC
         */
        JDBC,

        /**
         * ResourceAdapter
         */
        RESOURCE
    }

    private String className;
    private String driverName;
    private String jndiName;
	private String name;
    private Type type;
    private Properties props = new Properties();

    /**
     * Constructor
     */
    public DataserviceDatasource( ) {
    	this.props.clear();
    }

    /**
     * @return the data source name (can be <code>null</code> or empty)
     */
    public String getName() {
    	return this.name;
    }

    public void setName( final String name ) {
    	this.name = name;
    }

    /**
     * @return the JNDI name (can be <code>null</code> or empty)
     */
    public String getJndiName() {
        return this.jndiName;
    }

    /**
     * @param jndiName Sets jndiName to the specified value.
     */
    public void setJndiName( final String jndiName ) {
        this.jndiName = jndiName;
    }

    /**
     * @return the driver class name (can be <code>null</code> or empty)
     */
    public String getClassName() {
        return this.className;
    }

    /**
     * @param className Sets className to the specified value.
     */
    public void setClassName( final String className ) {
        this.className = className;
    }

    /**
     * @return driverName
     */
    public String getDriverName() {
        return driverName;
    }

    /**
     * @param driverName Sets driverName to the specified value.
     */
    public void setDriverName( String driverName ) {
        this.driverName = driverName;
    }

    public Type getType() {
    	return type;
    }

    public void setType(Type type) {
    	this.type = type;
    }

    public void setProperty(String propName, String propValue) {
    	this.props.put(propName, propValue);
    }

    public boolean hasProperty(String propName) {
    	return this.props.containsKey(propName);
    }

    public String getPropertyValue(String propName) {
    	return this.props.getProperty(propName);
    }
    
    /**
     * @return the custom properties (never <code>null</code> but can be empty)
     */
    public Properties getProperties() {
    	return this.props;
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
//
//    protected static class Reader {
//        private DataserviceDatasource parseDatasource( final XMLStreamReader streamReader ) throws Exception {
//            assert DataVirtLexicon.DatasourceXml.DATASOURCE_SET.equals(streamReader.getLocalName());
//
//            final DataserviceDatasource ds = new DataserviceDatasource();
//            assert (ds != null) : "datasource is null";
//
//            // collect children
//            while (streamReader.hasNext()) {
//                streamReader.next(); // point to next element
//
//                if (streamReader.isStartElement()) {
//                    final String elementName = streamReader.getLocalName();
//
//                    if (DataVirtLexicon.DatasourceXml.DATASOURCE.equals(elementName)) {
//                        String dsName = streamReader.getAttributeValue(null, DataVirtLexicon.DatasourceXml.NAME_ATTR);
//                    	ds.setName(dsName);
//                        String typeValue = streamReader.getAttributeValue(null, DataVirtLexicon.DatasourceXml.TYPE_ATTR);
//                    	ds.setType(Type.valueOf(typeValue));
//                    } else if (DataVirtLexicon.DatasourceXml.PROPERTY.equals(elementName)) {
//                        String propName = streamReader.getAttributeValue(null, DataVirtLexicon.DatasourceXml.NAME_ATTR);
//                        final String propValue = streamReader.getElementText();
//                        if(propName!=null && !propName.trim().isEmpty()) {
//                        	ds.setProperty(propName,propValue);
//                        }
//                    } else {
//                        LOGGER.debug("**** unexpected Dataservice element={0}", elementName);
//                    }
//                } else if (streamReader.isEndElement() && DataVirtLexicon.DatasourceXml.DATASOURCE.equals(streamReader.getLocalName())) {
//                    break;
//                }
//            }
//
//            return ds;
//        }
//
//        public DataserviceDatasource read( final InputStream stream,
//                                final Context context ) throws Exception {
//            DataserviceDatasource datasource = null;
//            final XMLInputFactory factory = XMLInputFactory.newInstance();
//            XMLStreamReader streamReader = null;
//
//            try {
//                streamReader = factory.createXMLStreamReader(stream);
//
//                if (streamReader.hasNext()) {
//                    if (streamReader.next() == XMLStreamConstants.START_ELEMENT) {
//                        final String elementName = streamReader.getLocalName();
//
//                        if (DataVirtLexicon.DatasourceXml.DATASOURCE_SET.equals(elementName)) {
//                        	datasource = parseDatasource(streamReader);
//                            assert (datasource != null) : "datasource is null";
//                        } else {
//                            LOGGER.debug("**** unhandled datasource read element ****");
//                        }
//                    }
//                }
//            } finally {
//                if (streamReader != null)
//                    streamReader.close();
//            }
//
//            return datasource;
//        }
//    }
}