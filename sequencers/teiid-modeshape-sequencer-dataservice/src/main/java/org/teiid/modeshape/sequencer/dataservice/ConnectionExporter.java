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

import java.io.StringWriter;
import java.util.Properties;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.StringUtil;
import org.teiid.modeshape.sequencer.Options;
import org.teiid.modeshape.sequencer.dataservice.Connection.Type;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.internal.AbstractExporter;

/**
 * An exporter for {@link Connection connections}.
 */
public class ConnectionExporter extends AbstractExporter {

    private static final Logger LOGGER = Logger.getLogger( ConnectionExporter.class );

    private Connection constructConnection( final Node connectionNode,
                                            final Options options ) throws Exception {
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
                final Options.PropertyFilter filter = getPropertyFilter( options );

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

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.internal.AbstractExporter#doExport(javax.jcr.Node,
     *      org.teiid.modeshape.sequencer.Options, org.teiid.modeshape.sequencer.internal.AbstractExporter.ResultImpl)
     */
    @Override
    protected void doExport( final Node connectionNode,
                             final Options options,
                             final ResultImpl result ) {
        XMLStreamWriter xmlWriter = null;

        try {
            final Connection connection = constructConnection( connectionNode, options );
            final StringWriter stringWriter = new StringWriter();

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

            final String pretty = ( isPrettyPrint( options ) ? prettyPrint( xml, options ) : xml );
            result.setOutcome( pretty, String.class );
        } catch ( final Exception e ) {
            result.setError( TeiidI18n.errorExportingConnection.text(), e );
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

    private void writePropertyElement( final XMLStreamWriter writer,
                                       final String propName,
                                       final String propValue ) throws XMLStreamException {
        writer.writeStartElement( DataVirtLexicon.DataServiceManifestId.PROPERTY );
        writer.writeAttribute( DataVirtLexicon.DataServiceManifestId.NAME, propName );
        writer.writeCharacters( propValue );
        writer.writeEndElement();
    }

}
