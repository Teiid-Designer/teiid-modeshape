/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of 
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 * 
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
import javax.xml.XMLConstants;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.modeshape.common.logging.Logger;
import org.teiid.modeshape.sequencer.dataservice.DataserviceDatasource.Type;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

public final class DataSourceReader extends DefaultHandler {

    private static final String DATASOURCE_SCHEMA_FILE = "org/teiid/modeshape/sequencer/dataservice/datasource.xsd";
    private static final Logger LOGGER = Logger.getLogger( DataSourceReader.class );

    private final StringBuilder className;
    private DataserviceDatasource dataSource;
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

    public DataSourceReader() throws Exception {
        this.className = new StringBuilder();
        this.driverName = new StringBuilder();
        this.elements = new Stack< String >();
        this.errors = new ArrayList< String >();
        this.fatals = new ArrayList< String >();
        this.infos = new ArrayList< String >();
        this.jndiName = new StringBuilder();
        this.propertyValue = new StringBuilder();
        this.warnings = new ArrayList< String >();

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

        if ( DataVirtLexicon.DatasourceXml.PROPERTY.equals( getCurrentElement() ) ) {
            this.propertyValue.append( value );
        } else if ( DataVirtLexicon.DatasourceXml.CLASSNAME.equals( getCurrentElement() ) ) {
            this.className.append( value );
        } else if ( DataVirtLexicon.DatasourceXml.DRIVER_NAME.equals( getCurrentElement() ) ) {
            this.driverName.append( value );
        } else if ( DataVirtLexicon.DatasourceXml.JNDI_NAME.equals( getCurrentElement() ) ) {
            this.jndiName.append( value );
        }

        super.characters( ch, start, length );
    }

    private void clearDataSourceState() {
        this.dataSource = new DataserviceDatasource();
        this.className.setLength( 0 );
        this.driverName.setLength( 0 );
        this.jndiName.setLength( 0 );
        this.propertyName = null;
        this.propertyValue.setLength( 0 );
        LOGGER.debug( "cleared data source instance state" );
    }

    private void clearState() {
        this.elements.clear();
        this.errors.clear();
        this.fatals.clear();
        this.infos.clear();
        this.warnings.clear();
        clearDataSourceState();
        LOGGER.debug( "cleared all data source reader state" );
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
        if ( DataVirtLexicon.DatasourceXml.JDBC_DATA_SOURCE.equals( localName ) ) {
            // done
        } else if ( DataVirtLexicon.DatasourceXml.RESOURCE_DATA_SOURCE.equals( localName ) ) {
            // done
        } else if ( DataVirtLexicon.DatasourceXml.JNDI_NAME.equals( localName ) ) {
            if ( this.jndiName.length() != 0 ) {
                this.dataSource.setJndiName( this.jndiName.toString() );
                this.jndiName.setLength( 0 );
            }
        } else if ( DataVirtLexicon.DatasourceXml.DRIVER_NAME.equals( localName ) ) {
            if ( this.driverName.length() != 0 ) {
                this.dataSource.setDriverName( this.driverName.toString() );
                this.driverName.setLength( 0 );
            }
        } else if ( DataVirtLexicon.DatasourceXml.CLASSNAME.equals( localName ) ) {
            if ( this.className.length() != 0 ) {
                this.dataSource.setClassName( this.className.toString() );
                this.className.setLength( 0 );
            }
        } else if ( DataVirtLexicon.DatasourceXml.PROPERTY.equals( localName ) ) {
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
     * @return the data source defined in the input stream (<code>null</code> until {@link #read(InputStream)} is called)
     */
    public DataserviceDatasource getDatasource() {
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
        final InputStream schemaStream = getClass().getClassLoader().getResourceAsStream( DATASOURCE_SCHEMA_FILE );

        try {
            this.schemaFile = File.createTempFile( "datasourceSchemaFile", ".xsd" ); //$NON-NLS-1$ //$NON-NLS-2$
            Files.copy( schemaStream, this.schemaFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
            this.schemaFile.deleteOnExit();
            LOGGER.debug( "data source schema file loaded" );
        } catch ( final IOException e ) {
            throw new Exception( TeiidI18n.dataSourceSchemaError.text( DATASOURCE_SCHEMA_FILE ), e );
        }

        // create parser
        final SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware( true );
        factory.setValidating( true );

        try {
            this.parser = factory.newSAXParser();
            this.parser.setProperty( "http://java.sun.com/xml/jaxp/properties/schemaLanguage", //$NON-NLS-1$
                                     "http://www.w3.org/2001/XMLSchema" ); //$NON-NLS-1$
            this.parser.setProperty( "http://java.sun.com/xml/jaxp/properties/schemaSource", schemaFile ); //$NON-NLS-1$
            LOGGER.debug( "data source reader parser created" );
        } catch ( final Exception e ) {
            throw new Exception( TeiidI18n.dataSourceSchemaError.text(), e );
        }
    }

    /**
     * @param datasourcesStream the input stream being processed (cannot be <code>null</code>)
     * @return the data source defined in the stream (never <code>null</code>)
     * @throws Exception if an error occurs
     */
    public DataserviceDatasource read( final InputStream datasourcesStream ) throws Exception {
        LOGGER.debug( "start data sources read" );
        clearState(); // make sure state is clear if read is called multiple times

        // read in stream because it will be used twice
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] buf = new byte[ 1024 ];

        int n = 0;
        while ( ( n = Objects.requireNonNull( datasourcesStream ).read( buf ) ) >= 0 ) {
            baos.write( buf, 0, n );
        }

        // use this to create new streams
        final byte[] content = baos.toByteArray();

        // validate XML
        validateXml( new ByteArrayInputStream( content ) );

        // parse
        this.parser.parse( new ByteArrayInputStream( content ), this );
        LOGGER.debug( "finished data sources read" );
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

        if ( DataVirtLexicon.DatasourceXml.JDBC_DATA_SOURCE.equals( localName ) ) {
            clearDataSourceState();
            this.dataSource.setType( Type.JDBC );
            final String dsName = attributes.getValue( DataVirtLexicon.DatasourceXml.NAME_ATTR );
            this.dataSource.setName( dsName );
        } else if ( DataVirtLexicon.DatasourceXml.RESOURCE_DATA_SOURCE.equals( localName ) ) {
            clearDataSourceState();
            this.dataSource.setType( Type.RESOURCE );
            final String dsName = attributes.getValue( DataVirtLexicon.DatasourceXml.NAME_ATTR );
            this.dataSource.setName( dsName );
        } else if ( DataVirtLexicon.DatasourceXml.JNDI_NAME.equals( localName )
                    || DataVirtLexicon.DatasourceXml.DRIVER_NAME.equals( localName )
                    || DataVirtLexicon.DatasourceXml.CLASSNAME.equals( localName ) ) {
            // nothing to do
        } else if ( DataVirtLexicon.DatasourceXml.PROPERTY.equals( localName ) ) {
            this.propertyName = attributes.getValue( DataVirtLexicon.DatasourceXml.NAME_ATTR );
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
        final SchemaFactory factory = SchemaFactory.newInstance( XMLConstants.W3C_XML_SCHEMA_NS_URI );
        final Schema schema = factory.newSchema( this.schemaFile );
        final Validator validator = schema.newValidator();
        validator.validate( new StreamSource( stream ) );
        LOGGER.debug( "data source XML file validated" );
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
