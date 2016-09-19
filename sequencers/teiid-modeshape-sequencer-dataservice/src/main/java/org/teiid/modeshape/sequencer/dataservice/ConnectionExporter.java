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

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.StringUtil;
import org.teiid.modeshape.sequencer.dataservice.Connection.Type;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * An exporter for {@link Connection connections}.
 */
public class ConnectionExporter {

    private static final int DEFAULT_INDENT_AMOUNT = 4;
    private static final boolean DEFAULT_PRETTY_PRINT = true;

    /**
     * A default property filter that filters out <code>jcr</code>, <code>mix</code>, <code>nt</code>, <code>dv</code>, and
     * <code>mode</code> prefixed properties.
     *
     * @see PropertyFilter
     */
    public static PropertyFilter DEFAULT_PROPERTY_FILTER = propertyName -> !propertyName.startsWith( "jcr:" )
                                                                           && !propertyName.startsWith( "mix:" )
                                                                           && !propertyName.startsWith( "nt:" )
                                                                           && !propertyName.startsWith( "dv:" )
                                                                           && !propertyName.startsWith( "mode:" );
    private static final Logger LOGGER = Logger.getLogger( ConnectionExporter.class );

    public static Map< String, Object > getDefaultOptions() {
        final Map< String, Object > options = new HashMap<>();
        options.put( OptionName.INDENT_AMOUNT, DEFAULT_INDENT_AMOUNT );
        options.put( OptionName.PRETTY_PRINT_XML, DEFAULT_PRETTY_PRINT );
        return options;
    }

    private Connection constructConnection( final Node connectionNode,
                                            final Map< String, Object > options ) throws Exception {
        final Connection connection = new Connection();
        connection.setName( connectionNode.getName() );

        { // type is required
            if ( !connectionNode.hasProperty( DataVirtLexicon.Connection.TYPE ) ) {
                throw new Exception( TeiidI18n.missingConnectionTypeProperty.text( connectionNode.getPath() ) );
            }

            final String type = connectionNode.getProperty( DataVirtLexicon.Connection.TYPE ).getString();
            connection.setType( Type.valueOf( type ) );
        }

        { // jndi name is required
            if ( !connectionNode.hasProperty( DataVirtLexicon.Connection.JNDI_NAME ) ) {
                throw new Exception( TeiidI18n.missingConnectionJndiNameProperty.text( connectionNode.getPath() ) );
            }

            final String jndiName = connectionNode.getProperty( DataVirtLexicon.Connection.JNDI_NAME ).getString();
            connection.setJndiName( jndiName );
        }

        { // driver name is required
            if ( !connectionNode.hasProperty( DataVirtLexicon.Connection.DRIVER_NAME ) ) {
                throw new Exception( TeiidI18n.missingConnectionDriverNameProperty.text( connectionNode.getPath() ) );
            }

            final String driverName = connectionNode.getProperty( DataVirtLexicon.Connection.DRIVER_NAME ).getString();
            connection.setDriverName( driverName );
        }

        // description is optional
        if ( connectionNode.hasProperty( DataVirtLexicon.Connection.DESCRIPTION ) ) {
            final String description = connectionNode.getProperty( DataVirtLexicon.Connection.DESCRIPTION ).getString();
            connection.setDescription( description );
        }

        // class name is required for resource adapters
        if ( connection.getType() == Type.RESOURCE ) {
            if ( !connectionNode.hasProperty( DataVirtLexicon.Connection.CLASS_NAME ) ) {
                throw new Exception( TeiidI18n.missingConnectionClassNameProperty.text( connectionNode.getPath() ) );
            }

            final String className = connectionNode.getProperty( DataVirtLexicon.Connection.CLASS_NAME ).getString();
            connection.setClassName( className );
        }

        { // properties are optional
            final PropertyIterator itr = connectionNode.getProperties();

            if ( itr.hasNext() ) {
                final PropertyFilter filter = getPropertyFilter( options );

                while ( itr.hasNext() ) {
                    final Property property = itr.nextProperty();

                    if ( filter.accept( property.getName() ) ) {
                        final String value = property.getValue().getString();
                        connection.setProperty( property.getName(), value );
                    }
                }
            }
        }

        return connection;
    }

    public String execute( final Node connectionNode,
                           Map< String, Object > options ) throws Exception {
        Objects.requireNonNull( connectionNode, "connectionNode" );

        if ( ( options == null ) || options.isEmpty() ) {
            options = getDefaultOptions();
        }

        final Connection connection = constructConnection( connectionNode, options );
        final StringWriter stringWriter = new StringWriter();
        XMLStreamWriter xmlWriter = null;

        try {
            final XMLOutputFactory xof = XMLOutputFactory.newInstance();
            xmlWriter = xof.createXMLStreamWriter( stringWriter );
            xmlWriter.writeStartDocument( "UTF-8", "1.0" );

            // root element
            if ( connection.getType() == Type.JDBC ) {
                xmlWriter.writeStartElement( DataVirtLexicon.ConnectionXmlId.JDBC_CONNECTION );
            } else if ( connection.getType() == Type.RESOURCE ) {
                xmlWriter.writeStartElement( DataVirtLexicon.ConnectionXmlId.RESOURCE_CONNECTION );
            } else {
                assert ( connection.getType() != null ); // should be checked in constructConnection method
                throw new Exception( TeiidI18n.unhandledConnectionType.text( connectionNode.getPath(),
                                                                             connection.getType().name() ) );
            }

            xmlWriter.writeAttribute( DataVirtLexicon.ConnectionXmlId.NAME_ATTR, connection.getName() );

            // description element
            if ( !StringUtil.isBlank( connection.getDescription() ) ) {
                xmlWriter.writeStartElement( DataVirtLexicon.ConnectionXmlId.DESCRIPTION );
                xmlWriter.writeCharacters( connection.getDescription() );
                xmlWriter.writeEndElement();
            }

            // JNDI name element
            xmlWriter.writeStartElement( DataVirtLexicon.ConnectionXmlId.JNDI_NAME );
            xmlWriter.writeCharacters( connection.getJndiName() );
            xmlWriter.writeEndElement();

            // driver name element
            xmlWriter.writeStartElement( DataVirtLexicon.ConnectionXmlId.DRIVER_NAME );
            xmlWriter.writeCharacters( connection.getDriverName() );
            xmlWriter.writeEndElement();

            { // properties
                final Properties props = connection.getProperties(); // already filtered

                for ( final String propName : props.stringPropertyNames() ) {
                    final Object value = props.get( propName );
                    writePropertyElement( xmlWriter, propName, value.toString() );
                }
            }

            // class name element
            if ( connection.getType() == Type.RESOURCE ) {
                xmlWriter.writeStartElement( DataVirtLexicon.ConnectionXmlId.CLASSNAME );
                xmlWriter.writeCharacters( connection.getClassName() );
                xmlWriter.writeEndElement();
            }

            xmlWriter.writeEndElement();
            xmlWriter.writeEndDocument();

            final String xml = stringWriter.toString().trim();
            LOGGER.debug( "Connection {0} xml: \n{1}", connection.getName(), prettyPrint( xml, options ) );
            return ( isPrettyPrint( options ) ? prettyPrint( xml, options ) : xml );
        } finally {
            if ( xmlWriter != null ) {
                try {
                    xmlWriter.close();
                } catch ( final Exception e ) {
                    // do nothing
                }
            }
        }
    }

    private String getIndentAmount( final Map< String, Object > options ) {
        final Object temp = options.get( OptionName.INDENT_AMOUNT );

        if ( ( temp == null ) || !( temp instanceof Integer ) ) {
            return Integer.toString( DEFAULT_INDENT_AMOUNT );
        }

        return Integer.toString( ( Integer )temp );
    }

    private PropertyFilter getPropertyFilter( final Map< String, Object > options ) {
        final Object temp = options.get( OptionName.PROPERTY_FILTER );

        if ( ( temp == null ) || !( temp instanceof PropertyFilter ) ) {
            return DEFAULT_PROPERTY_FILTER;
        }

        return ( PropertyFilter )temp;
    }

    private boolean isPrettyPrint( final Map< String, Object > options ) {
        final Object temp = options.get( OptionName.PRETTY_PRINT_XML );

        if ( ( temp == null ) || !( temp instanceof Boolean ) ) {
            return DEFAULT_PRETTY_PRINT;
        }

        return ( Boolean )temp;
    }

    private Document parseXmlFile( final String xml ) throws Exception {
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder db = dbf.newDocumentBuilder();
        final InputSource is = new InputSource( new StringReader( xml ) );
        return db.parse( is );
    }

    private String prettyPrint( final String xml,
                                final Map< String, Object > options ) throws Exception {
        final Document document = parseXmlFile( xml );
        final TransformerFactory factory = TransformerFactory.newInstance();

        final Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty( OutputKeys.ENCODING, "UTF-8" );
        transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );
        transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
        transformer.setOutputProperty( "{http://xml.apache.org/xslt}indent-amount", getIndentAmount( options ) );

        final DOMSource source = new DOMSource( document );
        final StringWriter output = new StringWriter();
        final StreamResult result = new StreamResult( output );
        transformer.transform( source, result );
        return output.toString();
    }

    private void writePropertyElement( final XMLStreamWriter writer,
                                       final String propName,
                                       final String propValue ) throws XMLStreamException {
        writer.writeStartElement( DataVirtLexicon.DataServiceManifestId.PROPERTY );
        writer.writeAttribute( DataVirtLexicon.DataServiceManifestId.NAME, propName );
        writer.writeCharacters( propValue );
        writer.writeEndElement();
    }

    /**
     * The names of the known options.
     */
    public interface OptionName {

        /**
         * The number of spaces for each indent level. Default value is <code>4</code>.
         */
        public String INDENT_AMOUNT = "export.indent_amount";

        /**
         * Indicates if pretty printing of the XML manifest should be done. Default value is <code>true</code>.
         */
        public String PRETTY_PRINT_XML = "export.pretty_print_xml";

        /**
         * The {@link PropertyFilter} to use for filtering data service properties.
         *
         * @see DataServiceExporter#DEFAULT_PROPERTY_FILTER
         */
        public String PROPERTY_FILTER = "export.property_filter";

    }

    /**
     * Filters the properties that will be exported.
     */
    @FunctionalInterface
    public interface PropertyFilter {

        /**
         * @param propertyName the name of the property being checked (cannot be <code>null</code> or empty)
         * @return <code>true</code> if the property should be exported
         */
        boolean accept( final String propertyName );

    }

}
