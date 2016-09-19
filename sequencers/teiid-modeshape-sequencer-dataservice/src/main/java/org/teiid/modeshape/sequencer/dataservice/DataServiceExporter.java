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

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
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
import org.modeshape.jcr.api.JcrConstants;
import org.teiid.modeshape.sequencer.dataservice.DataServiceEntry.PublishPolicy;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.vdb.VdbExporter;
import org.teiid.modeshape.sequencer.vdb.VdbManifest;
import org.teiid.modeshape.sequencer.vdb.lexicon.VdbLexicon;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * An exporter for data services.
 */
public class DataServiceExporter {

    private static final SimpleDateFormat DEFAULT_DATE_FORMATTER = new SimpleDateFormat( DataServiceManifest.DATE_PATTERN );
    private static final int DEFAULT_INDENT_AMOUNT = 4;

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

    /**
     * Pass in the data service node path.
     */
    private static final String FIND_SERVICE_VDB_PATTERN = "SELECT [jcr:path] FROM [" + DataVirtLexicon.ServiceVdbEntry.NODE_TYPE
                                                           + "] WHERE ISDESCENDANTNODE('%s')";

    private static final Logger LOGGER = Logger.getLogger( DataServiceExporter.class );
    private static final boolean DEFAULT_PRETTY_PRINT = true;

    /**
     * @return the default exporter options (never <code>null</code>)
     */
    public static Map< String, Object > getDefaultOptions() {
        final Map< String, Object > options = new HashMap<>();
        options.put( OptionName.DATE_FORMATTER, DEFAULT_DATE_FORMATTER );
        options.put( OptionName.EXPORT_ARTIFACT, ExportArtifact.DEFAULT );
        options.put( OptionName.PROPERTY_FILTER, DEFAULT_PROPERTY_FILTER );
        options.put( OptionName.CONNECTIONS_FOLDER, "connections/" );
        options.put( OptionName.DRIVERS_EXPORT_FOLDER, "drivers/" );
        options.put( OptionName.INDENT_AMOUNT, DEFAULT_INDENT_AMOUNT );
        options.put( OptionName.METADATA_FOLDER, "metadata/" );
        options.put( OptionName.PRETTY_PRINT_XML, DEFAULT_PRETTY_PRINT );
        options.put( OptionName.RESOURCES_FOLDER, "resources/" );
        options.put( OptionName.UDFS_FOLDER, "udfs/" );
        options.put( OptionName.VDBS_FOLDER, "vdbs/" );
        return options;
    }

    private DataServiceManifest constructManifest( final Node dataService,
                                                   final Map< String, Object > options,
                                                   final Map< DataServiceEntry, Node > entryNodeMap ) throws Exception {
        final DataServiceManifest manifest = new DataServiceManifest();

        { // name is required
            String name = null;

            if ( dataService.hasProperty( DataVirtLexicon.DataService.NAME ) ) {
                name = dataService.getProperty( DataVirtLexicon.DataService.NAME ).getString();
            } else {
                name = dataService.getName();
            }

            manifest.setName( name );
        }

        // description is optional
        if ( dataService.hasProperty( DataVirtLexicon.DataService.DESCRIPTION ) ) {
            final String description = dataService.getProperty( DataVirtLexicon.DataService.DESCRIPTION ).getString();
            manifest.setDescription( description );
        }

        // lastModified is optional
        if ( dataService.hasProperty( DataVirtLexicon.DataService.LAST_MODIFIED ) ) {
            final Date date = dataService.getProperty( DataVirtLexicon.DataService.LAST_MODIFIED ).getDate().getTime();
            final LocalDateTime lastModified = date.toInstant().atZone( ZoneId.systemDefault() ).toLocalDateTime();
            manifest.setLastModified( lastModified );
        }

        // modifiedBy is optional
        if ( dataService.hasProperty( DataVirtLexicon.DataService.MODIFIED_BY ) ) {
            final String modifiedBy = dataService.getProperty( DataVirtLexicon.DataService.MODIFIED_BY ).getString();
            manifest.setModifiedBy( modifiedBy );
        }

        { // properties are optional
            final PropertyIterator itr = dataService.getProperties();

            if ( itr.hasNext() ) {
                final PropertyFilter filter = getPropertyFilter( options );

                while ( itr.hasNext() ) {
                    final Property property = itr.nextProperty();

                    if ( filter.accept( property.getName() ) ) {
                        final String value = property.getValue().getString();
                        manifest.setProperty( property.getName(), value );
                    }
                }
            }
        }

        final NodeIterator itr = dataService.getNodes();

        while ( itr.hasNext() ) {
            final Node node = itr.nextNode();

            if ( node.hasProperty( DataVirtLexicon.DataServiceEntry.PUBLISH_POLICY ) ) {
                final String value = node.getProperty( DataVirtLexicon.DataServiceEntry.PUBLISH_POLICY ).getString();
                final PublishPolicy policy = PublishPolicy.valueOf( value );

                // don't export
                if ( policy == PublishPolicy.NEVER ) {
                    continue;
                }
            }

            final String primaryType = node.getPrimaryNodeType().getName();

            switch ( primaryType ) {
                case DataVirtLexicon.ServiceVdbEntry.NODE_TYPE: {
                    final ServiceVdbEntry entry = new ServiceVdbEntry();
                    final Node reference = findReference( node );
                    entryNodeMap.put( entry, reference );

                    setEntryProperties( node, entry, null );
                    setVdbProperties( node, entry, reference );

                    // dependencies
                    final NodeIterator dependencyItr = node.getNodes();

                    while ( dependencyItr.hasNext() ) {
                        final Node dependency = dependencyItr.nextNode();

                        if ( DataVirtLexicon.VdbEntry.NODE_TYPE.equals( dependency.getPrimaryNodeType().getName() ) ) {
                            final VdbEntry dependencyEntry = new VdbEntry();
                            final Node vdb = findReference( dependency );

                            entryNodeMap.put( dependencyEntry, vdb );
                            setEntryProperties( dependency, dependencyEntry, ( String )options.get( OptionName.VDBS_FOLDER ) );
                            setVdbProperties( dependency, dependencyEntry, vdb );
                            entry.addVdb( dependencyEntry );
                        } else {
                            LOGGER.info( TeiidI18n.serviceVdbEntryUnknownChildType,
                                         dependency.getName(),
                                         node.getPath(),
                                         dependency.getPrimaryNodeType().getName() );
                        }
                    }

                    manifest.setServiceVdb( entry );
                    LOGGER.debug( "Added service VDB {0} to manifest", node.getPath() );
                    break;
                }
                case DataVirtLexicon.ResourceEntry.NODE_TYPE: {
                    final DataServiceEntry entry = new DataServiceEntry();
                    final Node reference = findReference( node );
                    entryNodeMap.put( entry, reference );

                    setEntryProperties( node, entry, ( String )options.get( OptionName.RESOURCES_FOLDER ) );
                    manifest.addResource( entry );
                    LOGGER.debug( "Added resource {0} to manifest", node.getPath() );
                    break;
                }
                case DataVirtLexicon.ResourceEntry.DDL_ENTRY_NODE_TYPE: {
                    final DataServiceEntry entry = new DataServiceEntry();
                    final Node reference = findReference( node );
                    entryNodeMap.put( entry, reference );

                    setEntryProperties( node, entry, ( String )options.get( OptionName.METADATA_FOLDER ) );
                    manifest.addMetadata( entry );
                    LOGGER.debug( "Added metadata {0} to manifest", node.getPath() );
                    break;
                }
                case DataVirtLexicon.ResourceEntry.UDF_ENTRY_NODE_TYPE: {
                    final DataServiceEntry entry = new DataServiceEntry();
                    final Node reference = findReference( node );
                    entryNodeMap.put( entry, reference );

                    setEntryProperties( node, entry, ( String )options.get( OptionName.UDFS_FOLDER ) );
                    manifest.addUdf( entry );
                    LOGGER.debug( "Added UDF {0} to manifest", node.getPath() );
                    break;
                }
                case DataVirtLexicon.ResourceEntry.DRIVER_ENTRY_NODE_TYPE: {
                    final DataServiceEntry entry = new DataServiceEntry();
                    final Node reference = findReference( node );
                    entryNodeMap.put( entry, reference );

                    setEntryProperties( node, entry, ( String )options.get( OptionName.DRIVERS_EXPORT_FOLDER ) );
                    manifest.addDriver( entry );
                    LOGGER.debug( "Added driver {0} to manifest", node.getPath() );
                    break;
                }
                case DataVirtLexicon.ConnectionEntry.NODE_TYPE: {
                    final ConnectionEntry entry = new ConnectionEntry();

                    // JNDI name is required
                    if ( node.hasProperty( DataVirtLexicon.ConnectionEntry.JDNI_NAME ) ) {
                        final String jndiName = node.getProperty( DataVirtLexicon.ConnectionEntry.JDNI_NAME ).getString();
                        entry.setJndiName( jndiName );
                    } else {
                        LOGGER.info( TeiidI18n.missingConnectionEntryJndiName, node.getName(), dataService.getPath() );
                        continue;
                    }

                    final Node reference = findReference( node );
                    entryNodeMap.put( entry, reference );
                    setEntryProperties( node, entry, ( String )options.get( OptionName.CONNECTIONS_FOLDER ) );
                    manifest.addConnection( entry );
                    LOGGER.debug( "Added connection {0} to manifest", node.getPath() );
                    break;
                }
                case DataVirtLexicon.VdbEntry.NODE_TYPE: {
                    final VdbEntry entry = new VdbEntry();
                    final Node reference = findReference( node );
                    entryNodeMap.put( entry, reference );

                    setEntryProperties( node, entry, ( String )options.get( OptionName.VDBS_FOLDER ) );
                    setVdbProperties( node, entry, reference );
                    manifest.addVdb( entry );
                    LOGGER.debug( "Added VDB {0} to manifest", node.getPath() );
                    break;
                }
                default: {
                    LOGGER.info( TeiidI18n.dataServiceUnknownChildType, dataService.getPath(), node.getName(), primaryType );
                    break;
                }
            }
        }

        return manifest;
    }

    /**
     * @param dataServiceNode the data service node being exported (cannot be <code>null</code>)
     * @param options the exporter options (can be <code>null</code> or empty if default options should be used)
     * @return the exported artifact (never <code>null</code>)
     * @throws Exception if an error occurs
     * @see #getDefaultOptions()
     */
    public Object execute( final Node dataServiceNode,
                           Map< String, Object > options ) throws Exception {
        Objects.requireNonNull( dataServiceNode, "dataServiceNode" );

        if ( ( options == null ) || options.isEmpty() ) {
            options = getDefaultOptions();
        }

        final Map< DataServiceEntry, Node > entryNodeMap = new HashMap<>();

        switch ( getExportArtifact( options ) ) {
            case DATA_SERVICE_AS_ZIP:
                return exportAsZip( dataServiceNode, options, entryNodeMap );
            case MANIFEST_AS_XML:
                return exportManifest( dataServiceNode, options, entryNodeMap );
            case SERVICE_VDB_AS_XML:
                return exportServiceVdb( dataServiceNode, options, entryNodeMap );
            default:
                break;
        }

        // should not happen
        throw new Exception( TeiidI18n.unhandledDataServiceExportArtifactType.text( dataServiceNode.getPath() ) );
    }

    private byte[] exportAsZip( final Node dataService,
                                final Map< String, Object > options,
                                final Map< DataServiceEntry, Node > entryNodeMap ) throws Exception {
        try ( final ByteArrayOutputStream bos = new ByteArrayOutputStream();
              final ZipOutputStream zipStream = new ZipOutputStream( bos ); ) {
            final NodeIterator itr = dataService.getNodes();

            if ( !itr.hasNext() ) {
                LOGGER.info( TeiidI18n.missingDataServiceEntries, dataService.getPath() );
                return bos.toByteArray();
            }

            // export the data service manifest
            final String manifestXml = exportManifest( dataService, options, entryNodeMap );
            final ZipEntry manZipEntry = new ZipEntry( DataServiceSequencer.MANIFEST_FILE );
            zipStream.putNextEntry( manZipEntry );
            zipStream.write( manifestXml.getBytes() );
            zipStream.closeEntry();
            LOGGER.debug( "Added manifest zip entry {0}", manZipEntry.getName() );

            // export the data service entries
            final DataServiceManifest manifest = constructManifest( dataService, options, entryNodeMap );
            LOGGER.debug( "Manifest constructed" );

            { // export VDBs
                final List< VdbEntry > entries = new ArrayList<>();

                if ( manifest.getServiceVdb() != null ) {
                    entries.add( manifest.getServiceVdb() );

                    if ( manifest.getServiceVdb().getVdbs().length != 0 ) {
                        entries.addAll( Arrays.asList( manifest.getServiceVdb().getVdbs() ) );
                    }
                }

                entries.addAll( Arrays.asList( manifest.getVdbs() ) );

                if ( !entries.isEmpty() ) {
                    for ( final VdbEntry entry : entries ) {
                        final VdbExporter exporter = new VdbExporter();
                        final Node vdb = entryNodeMap.get( entry );
                        final String xml = exporter.execute( vdb, options ).toString();
                        final byte[] data = xml.getBytes();

                        final ZipEntry zipEntry = new ZipEntry( entry.getPath() );
                        zipStream.putNextEntry( zipEntry );
                        zipStream.write( data );
                        zipStream.flush();
                        zipStream.closeEntry();
                        LOGGER.debug( "Added VDB zip entry: {0}", entry.getPath() );
                    }
                } else {
                    LOGGER.debug( "No VDBs found to export" );
                }
            }

            { // export connections
                final ConnectionEntry[] connections = manifest.getConnections();

                if ( connections.length != 0 ) {
                    for ( final ConnectionEntry entry : connections ) {
                        final ConnectionExporter exporter = new ConnectionExporter();
                        final Node connection = entryNodeMap.get( entry );
                        final String xml = exporter.execute( connection, options );
                        final byte[] data = xml.getBytes();

                        final ZipEntry zipEntry = new ZipEntry( entry.getPath() );
                        zipStream.putNextEntry( zipEntry );
                        zipStream.write( data );
                        zipStream.flush();
                        zipStream.closeEntry();
                        LOGGER.debug( "Added connection zip entry: {0}", entry.getPath() );
                    }
                } else {
                    LOGGER.debug( "No connections found to export" );
                }
            }

            { // export drivers, DDLs, UDFs, and resources
                final List< DataServiceEntry > entries = new ArrayList<>();
                entries.addAll( Arrays.asList( manifest.getMetadata() ) );
                entries.addAll( Arrays.asList( manifest.getDrivers() ) );
                entries.addAll( Arrays.asList( manifest.getUdfs() ) );
                entries.addAll( Arrays.asList( manifest.getResources() ) );

                if ( !entries.isEmpty() ) {
                    for ( final DataServiceEntry entry : entries ) {
                        final Node reference = entryNodeMap.get( entry );

                        if ( reference.hasNode( JcrConstants.JCR_CONTENT ) ) {
                            final Node content = reference.getNode( JcrConstants.JCR_CONTENT );

                            if ( content.hasProperty( JcrConstants.JCR_DATA ) ) {
                                final Binary value = content.getProperty( JcrConstants.JCR_DATA ).getBinary();
                                final byte[] data = new byte[ ( int )value.getSize() ];
                                value.read( data, 0 );

                                final ZipEntry zipEntry = new ZipEntry( entry.getPath() );
                                zipStream.putNextEntry( zipEntry );
                                zipStream.write( data );
                                zipStream.flush();
                                zipStream.closeEntry();
                                LOGGER.debug( "Added zip entry: {0}", entry.getPath() );
                            } else {
                                LOGGER.info( TeiidI18n.missingDataServiceReferenceDataProperty,
                                             dataService.getPath(),
                                             entry.getEntryName(),
                                             reference.getPath() );
                            }
                        } else {
                            LOGGER.info( TeiidI18n.missingDataServiceReferenceContent,
                                         dataService.getPath(),
                                         entry.getEntryName(),
                                         reference.getPath() );
                        }
                    }
                } else {
                    LOGGER.debug( "No drivers, metadata files, UDFs, or resource files found to export" );
                }
            }

            // close output stream
            zipStream.flush();
            zipStream.finish();
            zipStream.close();

            return bos.toByteArray();
        }
    }

    private String exportManifest( final Node dataService,
                                   final Map< String, Object > options,
                                   final Map< DataServiceEntry, Node > entryNodeMap ) throws Exception {
        final DataServiceManifest manifest = constructManifest( dataService, options, entryNodeMap );
        final StringWriter stringWriter = new StringWriter();
        XMLStreamWriter xmlWriter = null;

        try {
            final XMLOutputFactory xof = XMLOutputFactory.newInstance();
            xmlWriter = xof.createXMLStreamWriter( stringWriter );
            xmlWriter.writeStartDocument( "UTF-8", "1.0" );

            // root element
            xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.DATASERVICE );
            xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.NAME, manifest.getName() );

            // data service description element
            if ( !StringUtil.isBlank( manifest.getDescription() ) ) {
                xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.DESCRIPTION );
                xmlWriter.writeCharacters( manifest.getDescription() );
                xmlWriter.writeEndElement();
            }

            // data service lastModified element
            if ( manifest.getLastModified() != null ) {
                xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.LAST_MODIFIED );

                final LocalDateTime modifiedDate = manifest.getLastModified();
                final Calendar calendar = GregorianCalendar.from( modifiedDate.atZone( ZoneId.systemDefault() ) );
                xmlWriter.writeCharacters( getDateFormatter( options ).format( calendar.getTime() ) );
                xmlWriter.writeEndElement();
            }

            // data service modifiedBy element
            if ( !StringUtil.isBlank( manifest.getModifiedBy() ) ) {
                xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.MODIFIED_BY );
                xmlWriter.writeCharacters( manifest.getModifiedBy() );
                xmlWriter.writeEndElement();
            }

            { // properties
                final Properties props = manifest.getProperties(); // already filtered

                for ( final String propName : props.stringPropertyNames() ) {
                    final Object value = props.get( propName );
                    writePropertyElement( xmlWriter, propName, value.toString() );
                }
            }

            { // service VDB entry
                final ServiceVdbEntry entry = manifest.getServiceVdb();

                if ( entry != null ) {
                    xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.SERVICE_VDB );

                    // path attribute
                    final String path = entry.getPath();
                    xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, path );

                    // publish policy attribute
                    final PublishPolicy policy = entry.getPublishPolicy();

                    if ( policy != null ) {
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH, policy.toXml() );
                    }

                    // VDB name attribute
                    xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.VDB_NAME, entry.getVdbName() );

                    // VDB version attribute
                    xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.VDB_VERSION, entry.getVdbVersion() );

                    { // dependencies
                        final VdbEntry[] dependencies = entry.getVdbs();

                        if ( dependencies.length != 0 ) {
                            xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.DEPENDENCIES );

                            for ( final VdbEntry dependency : dependencies ) {
                                xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.VDB_FILE );
                                xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, dependency.getPath() );
                                xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH,
                                                          dependency.getPublishPolicy().toXml() );
                                xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.VDB_NAME,
                                                          dependency.getVdbName() );
                                xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.VDB_VERSION,
                                                          dependency.getVdbVersion() );
                                xmlWriter.writeEndElement();
                            }

                            xmlWriter.writeEndElement();
                        }
                    }

                    xmlWriter.writeEndElement();
                }
            }

            { // metadata entries
                final DataServiceEntry[] entries = manifest.getMetadata();

                if ( entries.length != 0 ) {
                    xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.METADATA );

                    for ( final DataServiceEntry entry : entries ) {
                        xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.DDL_FILE );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, entry.getPath() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH,
                                                  entry.getPublishPolicy().toXml() );
                        xmlWriter.writeEndElement();
                    }

                    xmlWriter.writeEndElement();
                }
            }

            { // connection entries
                final ConnectionEntry[] entries = manifest.getConnections();

                if ( entries.length != 0 ) {
                    xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.CONNECTIONS );

                    for ( final ConnectionEntry entry : entries ) {
                        xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.CONNECTION_FILE );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, entry.getPath() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH,
                                                  entry.getPublishPolicy().toXml() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.JNDI_NAME, entry.getJndiName() );
                        xmlWriter.writeEndElement();
                    }

                    xmlWriter.writeEndElement();
                }
            }

            { // driver entries
                final DataServiceEntry[] entries = manifest.getDrivers();

                if ( entries.length != 0 ) {
                    xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.DRIVERS );

                    for ( final DataServiceEntry entry : entries ) {
                        xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.DRIVER_FILE );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, entry.getPath() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH,
                                                  entry.getPublishPolicy().toXml() );
                        xmlWriter.writeEndElement();
                    }

                    xmlWriter.writeEndElement();
                }
            }

            { // udf entries
                final DataServiceEntry[] entries = manifest.getUdfs();

                if ( entries.length != 0 ) {
                    xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.UDFS );

                    for ( final DataServiceEntry entry : entries ) {
                        xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.UDF_FILE );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, entry.getPath() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH,
                                                  entry.getPublishPolicy().toXml() );
                        xmlWriter.writeEndElement();
                    }

                    xmlWriter.writeEndElement();
                }
            }

            { // vdb entries
                final VdbEntry[] entries = manifest.getVdbs();

                if ( entries.length != 0 ) {
                    xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.VDBS );

                    for ( final VdbEntry entry : entries ) {
                        xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.VDB_FILE );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, entry.getPath() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH,
                                                  entry.getPublishPolicy().toXml() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.VDB_NAME, entry.getVdbName() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.VDB_VERSION, entry.getVdbVersion() );
                        xmlWriter.writeEndElement();
                    }

                    xmlWriter.writeEndElement();
                }
            }

            { // resource entries
                final DataServiceEntry[] entries = manifest.getResources();

                if ( entries.length != 0 ) {
                    xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.RESOURCES );

                    for ( final DataServiceEntry entry : entries ) {
                        xmlWriter.writeStartElement( DataVirtLexicon.DataServiceManifestId.RESOURCE_FILE );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PATH, entry.getPath() );
                        xmlWriter.writeAttribute( DataVirtLexicon.DataServiceManifestId.PUBLISH,
                                                  entry.getPublishPolicy().toXml() );
                        xmlWriter.writeEndElement();
                    }

                    xmlWriter.writeEndElement();
                }
            }

            xmlWriter.writeEndElement();
            xmlWriter.writeEndDocument();

            final String xml = stringWriter.toString().trim();
            LOGGER.debug( "Data service {0} manifest: \n{1}", dataService.getPath(), prettyPrint( xml, options ) );
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

    private String exportServiceVdb( final Node dataService,
                                     final Map< String, Object > options,
                                     final Map< DataServiceEntry, Node > entryNodeMap ) throws Exception {
        final String queryText = String.format( FIND_SERVICE_VDB_PATTERN, dataService.getPath() );
        final Session session = dataService.getSession();
        final QueryManager queryMgr = session.getWorkspace().getQueryManager();
        final Query query = queryMgr.createQuery( queryText, Query.JCR_SQL2 );
        final QueryResult result = query.execute();
        final NodeIterator itr = result.getNodes();

        if ( itr.getSize() == 0 ) {
            return null;
        }

        final Node serviceVdbEntry = itr.nextNode();
        final Node vdb = findReference( serviceVdbEntry );
        final VdbExporter exporter = new VdbExporter();
        return exporter.execute( vdb, options ).toString();
    }

    private Node findReference( final Node node ) throws Exception {
        Node reference = null;

        if ( node.hasProperty( DataVirtLexicon.DataServiceEntry.SOURCE_RESOURCE ) ) {
            final String refId = node.getProperty( DataVirtLexicon.DataServiceEntry.SOURCE_RESOURCE ).getString();
            reference = node.getSession().getNodeByIdentifier( refId );
        }

        if ( reference == null ) {
            throw new Exception( TeiidI18n.missingDataServiceEntryReference.text( node.getName() ) );
        }

        return reference;
    }

    private DateFormat getDateFormatter( final Map< String, Object > options ) {
        final Object temp = options.get( OptionName.DATE_FORMATTER );

        if ( ( temp == null ) || !( temp instanceof DateFormat ) ) {
            return DEFAULT_DATE_FORMATTER;
        }

        return ( DateFormat )temp;
    }

    private ExportArtifact getExportArtifact( final Map< String, Object > options ) {
        final Object temp = options.get( OptionName.EXPORT_ARTIFACT );

        if ( ( temp == null ) || !( temp instanceof ExportArtifact ) ) {
            return ExportArtifact.DEFAULT;
        }

        return ( ExportArtifact )temp;
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

    private void setEntryProperties( final Node node,
                                     final DataServiceEntry entry,
                                     final String entryFolder ) throws Exception {
        { // path is required
            String path = null;

            if ( node.hasProperty( DataVirtLexicon.DataServiceEntry.PATH ) ) {
                path = node.getProperty( DataVirtLexicon.DataServiceEntry.PATH ).getString();
            } else if ( StringUtil.isBlank( entryFolder ) ) {
                path = node.getName();
            } else {
                path = ( entryFolder + node.getName() );
            }

            entry.setPath( path );
        }

        { // publish policy is optional
            if ( node.hasProperty( DataVirtLexicon.DataServiceEntry.PUBLISH_POLICY ) ) {
                final String value = node.getProperty( DataVirtLexicon.DataServiceEntry.PUBLISH_POLICY ).getString();
                final PublishPolicy policy = PublishPolicy.valueOf( value );
                entry.setPublishPolicy( policy );
            }
        }
    }

    private void setVdbProperties( final Node node,
                                   final VdbEntry entry,
                                   final Node reference ) throws Exception {
        { // VDB name is required
            String vdbName = null;

            if ( node.hasProperty( DataVirtLexicon.VdbEntry.VDB_NAME ) ) {
                vdbName = node.getProperty( DataVirtLexicon.VdbEntry.VDB_NAME ).getString();
            } else if ( reference.hasProperty( VdbLexicon.Vdb.NAME ) ) {
                vdbName = node.getProperty( VdbLexicon.Vdb.NAME ).getString();
            } else {
                vdbName = reference.getName();
            }

            entry.setVdbName( vdbName );
        }

        { // VDB version is required
            String vdbVersion = null;

            if ( node.hasProperty( DataVirtLexicon.VdbEntry.VDB_VERSION ) ) {
                vdbVersion = node.getProperty( DataVirtLexicon.VdbEntry.VDB_VERSION ).getString();
            } else if ( reference.hasProperty( VdbLexicon.Vdb.VERSION ) ) {
                vdbVersion = node.getProperty( VdbLexicon.Vdb.VERSION ).getString();
            } else {
                entry.setVdbVersion( VdbManifest.DEFAULT_VERSION );
            }

            entry.setVdbVersion( vdbVersion );
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

    public enum ExportArtifact {

        /**
         * Export data service as the <code>byte</code> array of a zip file.
         */
        DATA_SERVICE_AS_ZIP,

        /**
         * Export data service XML manifest as a string.
         */
        MANIFEST_AS_XML,

        /**
         * Export the service VDB manifest as an XML string.
         */
        SERVICE_VDB_AS_XML;

        /**
         * The default export artifact. Value is {@value}.
         */
        public static final ExportArtifact DEFAULT = DATA_SERVICE_AS_ZIP;

    }

    /**
     * The names of the known options.
     */
    public interface OptionName {

        /**
         * Property whose value is the zip entry folder path for connection files (like *-connection.xml). Default value is
         * <code>connections/</code>.
         */
        public String CONNECTIONS_FOLDER = "export.connections_folder";

        /**
         * The {@link DateFormat formatter} to use for dates.
         *
         * @see DataServiceExporter#DEFAULT_DATE_FORMATTER
         */
        public String DATE_FORMATTER = "export.date_formatter";

        /**
         * Property whose value is the zip entry folder path for the drivers. Default value is <code>drivers/</code>.
         */
        public String DRIVERS_EXPORT_FOLDER = "export.drivers_folder";

        /**
         * The name of the artifact to export. Default value is {@link ExportArtifact#DATA_SERVICE_AS_ZIP}.
         *
         * @see ExportArtifact
         */
        public String EXPORT_ARTIFACT = "export.artifact";

        /**
         * The number of spaces for each indent level. Default value is <code>4</code>.
         */
        public String INDENT_AMOUNT = "export.indent_amount";

        /**
         * Property whose value is the zip entry folder path for metadata files (*.ddl). Default value is <code>metadata/</code>.
         */
        public String METADATA_FOLDER = "export.metadata_folder";

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

        /**
         * Property whose value is the zip entry folder path for resource files. Default value is <code>resources/</code>.
         */
        public String RESOURCES_FOLDER = "export.resources_folder";

        /**
         * Property whose value is the zip entry folder path for UDF files. Default value is <code>udfs/</code>.
         */
        public String UDFS_FOLDER = "export.udfs_folder";

        /**
         * Property whose value is the zip entry folder path for VDB files (*-vdb.xml). Default value is <code>vdbs/</code>.
         */
        public String VDBS_FOLDER = "export.vdbs_folder";

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
