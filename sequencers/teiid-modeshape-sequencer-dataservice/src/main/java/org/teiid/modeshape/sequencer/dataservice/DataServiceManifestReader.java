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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
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
import org.teiid.modeshape.sequencer.dataservice.DataServiceEntry.PublishPolicy;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A {@link DataServiceManifest data service manifest} reader.
 */
public final class DataServiceManifestReader extends DefaultHandler {

    private enum VdbParent {

        MANIFEST,
        SERVICE_VDB,
        UNKNOWN

    }

    private static final String DATA_SERVICE_SCHEMA_FILE = "org/teiid/modeshape/sequencer/dataService/dataService.xsd"; //$NON-NLS-1$
    private static final Logger LOGGER = Logger.getLogger( DataServiceManifestReader.class );

    private ConnectionEntry dataSource;
    private final Collection< ConnectionEntry > dataSources = new ArrayList<>();
    private DataServiceEntry ddl;
    private final StringBuilder description = new StringBuilder();
    private DataServiceEntry driver;
    private final Collection< DataServiceEntry > drivers = new ArrayList<>();
    private final Stack< String > elements = new Stack<>();
    private final List< String > errors = new ArrayList<>();
    private final List< String > fatals = new ArrayList<>();
    private final Collection< VdbEntry > importVdbs = new ArrayList<>();
    private final List< String > infos = new ArrayList<>();
    private final StringBuilder lastModified = new StringBuilder();
    private DataServiceManifest manifest;
    private final Collection< DataServiceEntry > metadata = new ArrayList<>();
    private final StringBuilder modifiedBy = new StringBuilder();
    private SAXParser parser;
    private String propertyName;
    private final StringBuilder propertyValue = new StringBuilder();
    private DataServiceEntry resource;
    private final Collection< DataServiceEntry > resources = new ArrayList<>();
    private File schemaFile = null;
    private ServiceVdbEntry serviceVdb;
    private DataServiceEntry udf;
    private final Collection< DataServiceEntry > udfs = new ArrayList<>();
    private VdbEntry vdb;
    private VdbParent vdbContainer = VdbParent.UNKNOWN;
    private final Collection< DataServiceEntry > vdbs = new ArrayList<>();
    private final List< String > warnings = new ArrayList<>();

    /**
     * @throws Exception if there is an error constructing the parser
     */
    public DataServiceManifestReader() throws Exception {
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

        if ( DataVirtLexicon.DataServiceManifestId.DESCRIPTION.equals( getCurrentElement() ) ) {
            this.description.append( value );
        } else if ( DataVirtLexicon.DataServiceManifestId.LAST_MODIFIED.equals( getCurrentElement() ) ) {
            this.lastModified.append( value );
        } else if ( DataVirtLexicon.DataServiceManifestId.MODIFIED_BY.equals( getCurrentElement() ) ) {
            this.modifiedBy.append( value );
        } else if ( DataVirtLexicon.DataServiceManifestId.PROPERTY.equals( getCurrentElement() ) ) {
            this.propertyValue.append( value );
        } else {
            LOGGER.debug( "characters unhandled: current element={0}, value={1}", getCurrentElement(), value ); //$NON-NLS-1$
        }

        super.characters( ch, start, length );
    }

    private void clearState() {
        this.dataSource = null;
        this.dataSources.clear();
        this.description.setLength( 0 );
        this.driver = null;
        this.drivers.clear();
        this.elements.clear();
        this.errors.clear();
        this.fatals.clear();
        this.importVdbs.clear();
        this.infos.clear();
        this.lastModified.setLength( 0 );
        this.manifest = new DataServiceManifest();
        this.ddl = null;
        this.metadata.clear();
        this.modifiedBy.setLength( 0 );
        this.propertyName = null;
        this.propertyValue.setLength( 0 );
        this.resource = null;
        this.resources.clear();
        this.serviceVdb = null;
        this.udf = null;
        this.udfs.clear();
        this.vdb = null;
        this.vdbContainer = VdbParent.UNKNOWN;
        this.vdbs.clear();
        this.warnings.clear();
        LOGGER.debug( "cleared all Data Service reader state" ); //$NON-NLS-1$
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
        if ( DataVirtLexicon.DataServiceManifestId.DATASERVICE.equals( localName ) ) {
            // done
        } else if ( DataVirtLexicon.DataServiceManifestId.DESCRIPTION.equals( localName ) ) {
            this.manifest.setDescription( this.description.toString() );
        } else if ( DataVirtLexicon.DataServiceManifestId.LAST_MODIFIED.equals( localName ) ) {
            final LocalDateTime lastModifiedDate = DataServiceManifest.parse( this.lastModified.toString() );
            this.manifest.setLastModified( lastModifiedDate );
        } else if ( DataVirtLexicon.DataServiceManifestId.MODIFIED_BY.equals( localName ) ) {
            this.manifest.setModifiedBy( this.modifiedBy.toString() );
        } else if ( DataVirtLexicon.DataServiceManifestId.PROPERTY.equals( localName ) ) {
            this.manifest.setProperty( this.propertyName, this.propertyValue.toString() );
            this.propertyName = null;
            this.propertyValue.setLength( 0 );
        } else if ( DataVirtLexicon.DataServiceManifestId.SERVICE_VDB.equals( localName ) ) {
            this.manifest.setServiceVdb( this.serviceVdb );
            this.vdbContainer = VdbParent.UNKNOWN;
        } else if ( DataVirtLexicon.DataServiceManifestId.METADATA.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.CONNECTIONS.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.DRIVERS.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.UDFS.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.RESOURCES.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.DEPENDENCIES.equals( localName ) ) {
            // nothing to do
        } else if ( DataVirtLexicon.DataServiceManifestId.VDBS.equals( localName ) ) {
            this.vdbContainer = VdbParent.UNKNOWN;
        } else if ( DataVirtLexicon.DataServiceManifestId.VDB_FILE.equals( localName ) ) {
            // add to parent
            if ( this.vdbContainer == VdbParent.MANIFEST ) {
                this.manifest.addVdb( this.vdb );
            } else if ( this.vdbContainer == VdbParent.SERVICE_VDB ) {
                this.serviceVdb.addVdb( this.vdb );
            } else {
                throw new SAXException( TeiidI18n.unhandledVdbFile.text( this.vdb.getPath() ) );
            }

            this.vdb = null;
        } else if ( DataVirtLexicon.DataServiceManifestId.DDL_FILE.equals( localName ) ) {
            this.manifest.addMetadata( this.ddl );
            this.ddl = null;
        } else if ( DataVirtLexicon.DataServiceManifestId.DRIVER_FILE.equals( localName ) ) {
            this.manifest.addDriver( this.driver );
            this.driver = null;
        } else if ( DataVirtLexicon.DataServiceManifestId.CONNECTION_FILE.equals( localName ) ) {
            this.manifest.addDataSource( this.dataSource );
            this.dataSource = null;
        } else if ( DataVirtLexicon.DataServiceManifestId.UDF_FILE.equals( localName ) ) {
            this.manifest.addUdf( this.udf );
            this.udf = null;
        } else if ( DataVirtLexicon.DataServiceManifestId.RESOURCE_FILE.equals( localName ) ) {
            this.manifest.addResource( this.resource );
            this.resource = null;
        } else {
            throw new SAXException( TeiidI18n.unhandledDataServiceEndElement.text( localName ) );
        }

        final String popped = this.elements.pop();
        LOGGER.debug( '-' + popped + ", size=" + this.elements.size() ); //$NON-NLS-1$
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
     * @return the data service manifest defined in the input stream (can be <code>null</code> if stream has not been read)
     * @see #read(InputStream)
     */
    public DataServiceManifest getManifest() {
        return this.manifest;
    }

    /**
     * @return the warning messages output from the last parse operation (never <code>null</code> but can be empty)
     */
    public List< String > getWarnings() {
        return this.warnings;
    }

    private void initParser() throws Exception {
        final InputStream schemaStream = getClass().getClassLoader().getResourceAsStream( DATA_SERVICE_SCHEMA_FILE );

        try {
            this.schemaFile = File.createTempFile( "dataServiceSchemaFile", ".xsd" ); //$NON-NLS-1$ //$NON-NLS-2$
            Files.copy( schemaStream, this.schemaFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
            this.schemaFile.deleteOnExit();
            LOGGER.debug( "Data Service schema file loaded" ); //$NON-NLS-1$
        } catch ( final IOException e ) {
            throw new Exception( TeiidI18n.dataServiceSchemaError.text( DATA_SERVICE_SCHEMA_FILE ), e );
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
            LOGGER.debug( "Data Service reader parser created" ); //$NON-NLS-1$
        } catch ( final Exception e ) {
            throw new Exception( TeiidI18n.dataServiceSchemaError.text(), e );
        }
    }

    /**
     * @param stream the input stream being processed (cannot be <code>null</code>)
     * @return the data service manifest defined in the stream (never <code>null</code>)
     * @throws Exception if an error occurs
     */
    public DataServiceManifest read( final InputStream stream ) throws Exception {
        LOGGER.debug( "start Data Service read" ); //$NON-NLS-1$
        clearState(); // make sure state is clear if read is called multiple times

        // read in stream because it will be used twice
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] buf = new byte[ 1024 ];

        int n = 0;
        while ( ( n = Objects.requireNonNull( stream ).read( buf ) ) >= 0 ) {
            baos.write( buf, 0, n );
        }

        // use this to create new streams
        final byte[] content = baos.toByteArray();

        // validate XML
        validateXml( new ByteArrayInputStream( content ) );

        // parse
        this.parser.parse( new ByteArrayInputStream( content ), this );
        LOGGER.debug( "finished Data Service read" ); //$NON-NLS-1$
        return this.manifest;
    }

    private void setEntryAttributes( final DataServiceEntry entry,
                                     final Attributes attributes ) {
        // path
        final String path = attributes.getValue( DataVirtLexicon.DataServiceManifestId.PATH );
        entry.setPath( path );

        // deploy policy
        if ( attributes.getValue( DataVirtLexicon.DataServiceManifestId.PUBLISH ) != null ) {
            final String xmlPolicy = attributes.getValue( DataVirtLexicon.DataServiceManifestId.PUBLISH );
            entry.setPublishPolicy( PublishPolicy.fromXml( xmlPolicy ) );
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see org.xml.sax.helpers.DefaultHandler#skippedEntity(java.lang.String)
     */
    @Override
    public void skippedEntity( final String name ) {
        this.infos.add( TeiidI18n.dataServiceXmlEntitySkipped.text( name ) );
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
        final String pushed = this.elements.push( localName );
        LOGGER.debug( '+' + pushed + ", size=" + this.elements.size() ); //$NON-NLS-1$

        if ( DataVirtLexicon.DataServiceManifestId.DATASERVICE.equals( localName ) ) {
            final String name = attributes.getValue( DataVirtLexicon.DataServiceManifestId.NAME );
            this.manifest.setName( name );
        } else if ( DataVirtLexicon.DataServiceManifestId.DESCRIPTION.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.LAST_MODIFIED.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.MODIFIED_BY.equals( localName ) ) {
            // nothing to do
        } else if ( DataVirtLexicon.DataServiceManifestId.PROPERTY.equals( localName ) ) {
            this.propertyName = attributes.getValue( DataVirtLexicon.ConnectionXmlId.NAME_ATTR );
        } else if ( DataVirtLexicon.DataServiceManifestId.SERVICE_VDB.equals( localName ) ) {
            this.serviceVdb = new ServiceVdbEntry();
            setEntryAttributes( this.serviceVdb, attributes );

            // VDB name
            final String vdbName = attributes.getValue( DataVirtLexicon.DataServiceManifestId.VDB_NAME );
            this.serviceVdb.setVdbName( vdbName );

            // VDB version
            final String version = attributes.getValue( DataVirtLexicon.DataServiceManifestId.VDB_VERSION );
            this.serviceVdb.setVdbVersion( version );

            this.vdbContainer = VdbParent.SERVICE_VDB;
        } else if ( DataVirtLexicon.DataServiceManifestId.METADATA.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.CONNECTIONS.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.DRIVERS.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.UDFS.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.RESOURCES.equals( localName )
                    || DataVirtLexicon.DataServiceManifestId.DEPENDENCIES.equals( localName ) ) {
            // nothing to do
        } else if ( DataVirtLexicon.DataServiceManifestId.VDBS.equals( localName ) ) {
            this.vdbContainer = VdbParent.MANIFEST;
        } else if ( DataVirtLexicon.DataServiceManifestId.VDB_FILE.equals( localName ) ) {
            this.vdb = new VdbEntry();
            setEntryAttributes( this.vdb, attributes );

            // VDB name
            final String vdbName = attributes.getValue( DataVirtLexicon.DataServiceManifestId.VDB_NAME );
            this.vdb.setVdbName( vdbName );

            // VDB version
            final String version = attributes.getValue( DataVirtLexicon.DataServiceManifestId.VDB_VERSION );
            this.vdb.setVdbVersion( version );
        } else if ( DataVirtLexicon.DataServiceManifestId.DDL_FILE.equals( localName ) ) {
            this.ddl = new DataServiceEntry();
            setEntryAttributes( this.ddl, attributes );
        } else if ( DataVirtLexicon.DataServiceManifestId.DRIVER_FILE.equals( localName ) ) {
            this.driver = new DataServiceEntry();
            setEntryAttributes( this.driver, attributes );
        } else if ( DataVirtLexicon.DataServiceManifestId.CONNECTION_FILE.equals( localName ) ) {
            this.dataSource = new ConnectionEntry();
            setEntryAttributes( this.dataSource, attributes );

            // JNDI name
            final String jndiName = attributes.getValue( DataVirtLexicon.DataServiceManifestId.JNDI_NAME );
            this.dataSource.setJndiName( jndiName );
        } else if ( DataVirtLexicon.DataServiceManifestId.UDF_FILE.equals( localName ) ) {
            this.udf = new DataServiceEntry();
            setEntryAttributes( this.udf, attributes );
        } else if ( DataVirtLexicon.DataServiceManifestId.RESOURCE_FILE.equals( localName ) ) {
            this.resource = new DataServiceEntry();
            setEntryAttributes( this.resource, attributes );
        } else {
            throw new SAXException( TeiidI18n.unhandledDataServiceStartElement.text( localName ) );
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
        this.infos.add( TeiidI18n.dataServiceXmlDeclarationNotParsed.text( name ) );
    }

    private void validateXml( final InputStream stream ) throws Exception {
        final SchemaFactory factory = SchemaFactory.newInstance( "http://www.w3.org/2001/XMLSchema" ); //$NON-NLS-1$
        final Schema schema = factory.newSchema( this.schemaFile );
        final Validator validator = schema.newValidator();
        validator.validate( new StreamSource( stream ) );
        LOGGER.debug( "Data Service XML file validated" ); //$NON-NLS-1$
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
