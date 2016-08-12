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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Stack;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.modeshape.common.logging.Logger;
import org.teiid.modeshape.sequencer.dataservice.Connection.Type;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A reader for connection files.
 */
public final class ConnectionReader extends DefaultHandler {

    /**
     * The valid connection file extensions. Value is {@value}.
     */
    public static final String[] FILE_EXTENSIONS = { "-connection.xml" }; //$NON-NLS-1$

    private static final String DATA_SOURCE_SCHEMA_FILE = "org/teiid/modeshape/sequencer/dataService/connection.xsd"; //$NON-NLS-1$
    private static final Logger LOGGER = Logger.getLogger( ConnectionReader.class );

    private final StringBuilder className;
    private Connection dataSource;
    private final StringBuilder description;
    private final StringBuilder driverName;
    private final Stack< String > elements;
    private final List< String > errors;
    private final List< String > fatals;
    private final List< String > infos;
    private final StringBuilder jndiName;
    private SAXParser parser;
    private String propertyName;
    private final StringBuilder propertyValue;
    private File schemaFile = null;
    private final List< String > warnings;

    /**
     * @throws Exception if there is an error constructing the parser
     */
    public ConnectionReader() throws Exception {
        this.className = new StringBuilder();
        this.description = new StringBuilder();
        this.driverName = new StringBuilder();
        this.elements = new Stack<>();
        this.errors = new ArrayList<>();
        this.fatals = new ArrayList<>();
        this.infos = new ArrayList<>();
        this.jndiName = new StringBuilder();
        this.propertyValue = new StringBuilder();
        this.warnings = new ArrayList<>();

        initParser();
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
     */
    @Override
    public void characters( final char[] ch,
                            final int start,
                            final int length ) throws SAXException {
        final String value = new String( ch, start, length );

        if ( DataVirtLexicon.ConnectionXmlId.PROPERTY.equals( getCurrentElement() ) ) {
            this.propertyValue.append( value );
        } else if ( DataVirtLexicon.ConnectionXmlId.CLASSNAME.equals( getCurrentElement() ) ) {
            this.className.append( value );
        } else if ( DataVirtLexicon.ConnectionXmlId.DESCRIPTION.equals( getCurrentElement() ) ) {
            this.description.append( value );
        } else if ( DataVirtLexicon.ConnectionXmlId.DRIVER_NAME.equals( getCurrentElement() ) ) {
            this.driverName.append( value );
        } else if ( DataVirtLexicon.ConnectionXmlId.JNDI_NAME.equals( getCurrentElement() ) ) {
            this.jndiName.append( value );
        }

        super.characters( ch, start, length );
    }

    private void clearDataSourceState() {
        this.dataSource = new Connection();
        this.className.setLength( 0 );
        this.description.setLength( 0 );
        this.driverName.setLength( 0 );
        this.jndiName.setLength( 0 );
        this.propertyName = null;
        this.propertyValue.setLength( 0 );
        LOGGER.debug( "cleared connection instance state" ); //$NON-NLS-1$
    }

    private void clearState() {
        this.elements.clear();
        this.errors.clear();
        this.fatals.clear();
        this.infos.clear();
        this.warnings.clear();
        clearDataSourceState();
        LOGGER.debug( "cleared all connection reader state" ); //$NON-NLS-1$
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void endElement( final String uri,
                            final String localName,
                            final String qName ) throws SAXException {
        if ( DataVirtLexicon.ConnectionXmlId.JDBC_CONNECTION.equals( localName ) ) {
            // done
        } else if ( DataVirtLexicon.ConnectionXmlId.RESOURCE_CONNECTION.equals( localName ) ) {
            // done
        } else if ( DataVirtLexicon.ConnectionXmlId.DESCRIPTION.equals( localName ) ) {
            this.dataSource.setDescription( this.description.toString() );
        } else if ( DataVirtLexicon.ConnectionXmlId.JNDI_NAME.equals( localName ) ) {
            if ( this.jndiName.length() != 0 ) {
                this.dataSource.setJndiName( this.jndiName.toString() );
                this.jndiName.setLength( 0 );
            }
        } else if ( DataVirtLexicon.ConnectionXmlId.DRIVER_NAME.equals( localName ) ) {
            if ( this.driverName.length() != 0 ) {
                this.dataSource.setDriverName( this.driverName.toString() );
                this.driverName.setLength( 0 );
            }
        } else if ( DataVirtLexicon.ConnectionXmlId.CLASSNAME.equals( localName ) ) {
            if ( this.className.length() != 0 ) {
                this.dataSource.setClassName( this.className.toString() );
                this.className.setLength( 0 );
            }
        } else if ( DataVirtLexicon.ConnectionXmlId.PROPERTY.equals( localName ) ) {
            this.dataSource.setProperty( this.propertyName, this.propertyValue.toString() );
            this.propertyName = null;
            this.propertyValue.setLength( 0 );
        } else {
            throw new SAXException( TeiidI18n.unhandledDatasoureEndElement.text( localName ) );
        }

        this.elements.pop();
        super.endElement( uri, localName, qName );
    }

    /**
     * {@inheritDoc}}
     *
     * @see org.xml.sax.helpers.DefaultHandler#error(org.xml.sax.SAXParseException)
     */
    @Override
    public void error( final SAXParseException e ) {
        this.errors.add( e.getLocalizedMessage() );
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#fatalError(org.xml.sax.SAXParseException)
     */
    @Override
    public void fatalError( final SAXParseException e ) {
        this.fatals.add( e.getLocalizedMessage() );
    }

    /**
     * @return the element currently being parsed
     */
    private String getCurrentElement() {
        if ( this.elements.empty() ) {
            return null;
        }

        return this.elements.peek();
    }

    /**
     * @return the connection defined in the input stream (<code>null</code> until {@link #read(InputStream)} is called)
     */
    public Connection getDatasource() {
        return this.dataSource;
    }

    /**
     * @return the error messages output from the last parse operation (never <code>null</code> but can be empty)
     */
    public List< String > getErrors() {
        return this.errors;
    }

    /**
     * @return the fatal error messages output from the last parse operation (never <code>null</code> but can be empty)
     */
    public List< String > getFatalErrors() {
        return this.fatals;
    }

    /**
     * @return the information messages output from the last parse operation (never <code>null</code> but can be empty)
     */
    public List< String > getInfos() {
        return this.infos;
    }

    /**
     * @return the warning messages output from the last parse operation (never <code>null</code> but can be empty)
     */
    public List< String > getWarnings() {
        return this.warnings;
    }

    private void initParser() throws Exception {
        final InputStream schemaStream = getClass().getClassLoader().getResourceAsStream( DATA_SOURCE_SCHEMA_FILE );

        try {
            this.schemaFile = File.createTempFile( "dataSourceSchemaFile", ".xsd" ); //$NON-NLS-1$ //$NON-NLS-2$
            Files.copy( schemaStream, this.schemaFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
            this.schemaFile.deleteOnExit();
            LOGGER.debug( "connection schema file loaded" ); //$NON-NLS-1$
        } catch ( final IOException e ) {
            throw new Exception( TeiidI18n.dataSourceSchemaError.text( DATA_SOURCE_SCHEMA_FILE ), e );
        }

        // create parser
        final SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware( true );
        factory.setValidating( true );

        try {
            this.parser = factory.newSAXParser();
            this.parser.setProperty( "http://java.sun.com/xml/jaxp/properties/schemaLanguage", //$NON-NLS-1$
                                     "http://www.w3.org/2001/XMLSchema" ); //$NON-NLS-1$
            this.parser.setProperty( "http://java.sun.com/xml/jaxp/properties/schemaSource", this.schemaFile ); //$NON-NLS-1$
            LOGGER.debug( "connection reader parser created" ); //$NON-NLS-1$
        } catch ( final Exception e ) {
            throw new Exception( TeiidI18n.dataSourceSchemaError.text(), e );
        }
    }

    /**
     * @param connectionStream the input stream being processed (cannot be <code>null</code>)
     * @return the connection defined in the stream (never <code>null</code>)
     * @throws Exception if an error occurs
     */
    public Connection read( final InputStream connectionStream ) throws Exception {
        LOGGER.debug( "start connection read" ); //$NON-NLS-1$
        clearState(); // make sure state is clear if read is called multiple times

        // read in stream because it will be used twice
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] buf = new byte[ 1024 ];

        int n = 0;
        while ( ( n = Objects.requireNonNull( connectionStream, "connectionStream" ).read( buf ) ) >= 0 ) {
            baos.write( buf, 0, n );
        }

        // use this to create new streams
        final byte[] content = baos.toByteArray();

        // validate XML
        validateXml( new ByteArrayInputStream( content ) );

        // parse
        this.parser.parse( new ByteArrayInputStream( content ), this );
        LOGGER.debug( "finished connection read" ); //$NON-NLS-1$
        return this.dataSource;
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#skippedEntity(java.lang.String)
     */
    @Override
    public void skippedEntity( final String name ) {
        this.infos.add( TeiidI18n.dataSourceXmlEntitySkipped.text( name ) );
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String,
     *      org.xml.sax.Attributes)
     */
    @Override
    public void startElement( final String uri,
                              final String localName,
                              final String qName,
                              final Attributes attributes ) throws SAXException {
        this.elements.push( localName );

        if ( DataVirtLexicon.ConnectionXmlId.JDBC_CONNECTION.equals( localName ) ) {
            clearDataSourceState();
            this.dataSource.setType( Type.JDBC );
            final String dsName = attributes.getValue( DataVirtLexicon.ConnectionXmlId.NAME_ATTR );
            this.dataSource.setName( dsName );
        } else if ( DataVirtLexicon.ConnectionXmlId.RESOURCE_CONNECTION.equals( localName ) ) {
            clearDataSourceState();
            this.dataSource.setType( Type.RESOURCE );
            final String dsName = attributes.getValue( DataVirtLexicon.ConnectionXmlId.NAME_ATTR );
            this.dataSource.setName( dsName );
        } else if ( DataVirtLexicon.ConnectionXmlId.DESCRIPTION.equals( localName ) ) {
            // nothing to do
        } else if ( DataVirtLexicon.ConnectionXmlId.JNDI_NAME.equals( localName )
                    || DataVirtLexicon.ConnectionXmlId.DRIVER_NAME.equals( localName )
                    || DataVirtLexicon.ConnectionXmlId.CLASSNAME.equals( localName ) ) {
            // nothing to do
        } else if ( DataVirtLexicon.ConnectionXmlId.PROPERTY.equals( localName ) ) {
            this.propertyName = attributes.getValue( DataVirtLexicon.ConnectionXmlId.NAME_ATTR );
        } else {
            throw new SAXException( TeiidI18n.unhandledDatasoureStartElement.text( localName ) );
        }

        super.startElement( uri, localName, qName, attributes );
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#unparsedEntityDecl(java.lang.String, java.lang.String, java.lang.String,
     *      java.lang.String)
     */
    @Override
    public void unparsedEntityDecl( final String name,
                                    final String publicId,
                                    final String systemId,
                                    final String notationName ) {
        this.infos.add( TeiidI18n.dataSourceXmlDeclarationNotParsed.text( name ) );
    }

    private void validateXml( final InputStream stream ) throws Exception {
        final SchemaFactory factory = SchemaFactory.newInstance( "http://www.w3.org/2001/XMLSchema" ); //$NON-NLS-1$
        final Schema schema = factory.newSchema( this.schemaFile );
        final Validator validator = schema.newValidator();
        validator.validate( new StreamSource( stream ) );
        LOGGER.debug( "connection XML file validated" ); //$NON-NLS-1$
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#warning(org.xml.sax.SAXParseException)
     */
    @Override
    public void warning( final SAXParseException e ) {
        this.warnings.add( e.getLocalizedMessage() );
    }

}
