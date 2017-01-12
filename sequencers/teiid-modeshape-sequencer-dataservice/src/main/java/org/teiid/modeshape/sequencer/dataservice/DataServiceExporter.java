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

import static org.teiid.modeshape.sequencer.dataservice.DataServiceManifest.MANIFEST_ZIP_PATH;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.StringUtil;
import org.modeshape.jcr.api.JcrConstants;
import org.teiid.modeshape.sequencer.Options;
import org.teiid.modeshape.sequencer.Result;
import org.teiid.modeshape.sequencer.dataservice.DataServiceEntry.PublishPolicy;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.internal.AbstractExporter;
import org.teiid.modeshape.sequencer.vdb.VdbExporter;
import org.teiid.modeshape.sequencer.vdb.VdbManifest;
import org.teiid.modeshape.sequencer.vdb.lexicon.VdbLexicon;

/**
 * An exporter for data services.
 */
public class DataServiceExporter extends AbstractExporter {

    /**
     * The result data key whose value is a <code>String[]</code> of the file entry paths of the result outcome. Used only when
     * exporting file contents of the data service.
     *
     * @see Result#getData(String)
     * @see Result#getOutcome()
     */
    public static final String RESULT_ENTRY_PATHS = "data-service-exporter.result-entry-paths";

    private static final String DEFAULT_CONNECTIONS_FOLDER = "connections/";
    private static final String DEFAULT_DRIVERS_EXPORT_FOLDER = "drivers/";
    private static final String DEFAULT_METADATA_FOLDER = "metadata/";
    private static final String DEFAULT_RESOURCES_FOLDER = "resources/";
    private static final String DEFAULT_UDFS_FOLDER = "udfs/";
    private static final String DEFAULT_VDBS_FOLDER = "vdbs/";

    /**
     * Pass in the data service node path.
     */
    private static final String FIND_SERVICE_VDB_PATTERN = "SELECT [jcr:path] FROM ["
                                                           + DataVirtLexicon.ServiceVdbEntry.NODE_TYPE
                                                           + "] WHERE ISDESCENDANTNODE('%s')";

    private static final Logger LOGGER = Logger.getLogger( DataServiceExporter.class );

    private DataServiceManifest constructManifest( final Node dataService,
                                                   final Options options,
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
            final Date lastModified = dataService.getProperty( DataVirtLexicon.DataService.LAST_MODIFIED ).getDate().getTime();
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
                final Options.PropertyFilter filter = getPropertyFilter( options );

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

            if ( DataVirtLexicon.ServiceVdbEntry.NODE_TYPE.equals( primaryType ) ) {
                final ServiceVdbEntry entry = new ServiceVdbEntry();
                final Node reference = findReference(node);
                entryNodeMap.put(entry, reference);

                setEntryProperties(node, entry, null);
                setVdbProperties(node, entry, reference);

                // dependencies
                final NodeIterator dependencyItr = node.getNodes();

                while (dependencyItr.hasNext()) {
                    final Node dependency = dependencyItr.nextNode();

                    if (DataVirtLexicon.VdbEntry.NODE_TYPE.equals(dependency.getPrimaryNodeType().getName())) {
                        final VdbEntry dependencyEntry = new VdbEntry();
                        final Node vdb = findReference(dependency);

                        entryNodeMap.put(dependencyEntry, vdb);
                        setEntryProperties(dependency, dependencyEntry,
                                           (String)options.get(OptionName.VDBS_FOLDER, DEFAULT_VDBS_FOLDER));
                        setVdbProperties(dependency, dependencyEntry, vdb);
                        entry.addVdb(dependencyEntry);
                    } else {
                        LOGGER.info(TeiidI18n.serviceVdbEntryUnknownChildType, dependency.getName(), node.getPath(),
                                    dependency.getPrimaryNodeType().getName());
                    }
                }

                manifest.setServiceVdb(entry);
                LOGGER.debug("Added service VDB {0} to manifest", node.getPath());
            } else if (DataVirtLexicon.ResourceEntry.NODE_TYPE.equals(primaryType)) {
                final DataServiceEntry entry = new DataServiceEntry();
                final Node reference = findReference(node);
                entryNodeMap.put(entry, reference);

                setEntryProperties(node, entry,
                                   (String) options.get(OptionName.RESOURCES_FOLDER, DEFAULT_RESOURCES_FOLDER));
                manifest.addResource(entry);
                LOGGER.debug("Added resource {0} to manifest", node.getPath());
            } else if (DataVirtLexicon.ResourceEntry.DDL_ENTRY_NODE_TYPE.equals(primaryType)) {
                final DataServiceEntry entry = new DataServiceEntry();
                final Node reference = findReference(node);
                entryNodeMap.put(entry, reference);

                setEntryProperties(node, entry,
                                   (String) options.get(OptionName.METADATA_FOLDER, DEFAULT_METADATA_FOLDER));
                manifest.addMetadata(entry);
                LOGGER.debug("Added metadata {0} to manifest", node.getPath());
            } else if (DataVirtLexicon.ResourceEntry.UDF_ENTRY_NODE_TYPE.equals(primaryType)) {
                final DataServiceEntry entry = new DataServiceEntry();
                final Node reference = findReference(node);
                entryNodeMap.put(entry, reference);

                setEntryProperties(node, entry, (String) options.get(OptionName.UDFS_FOLDER, DEFAULT_UDFS_FOLDER));
                manifest.addUdf(entry);
                LOGGER.debug("Added UDF {0} to manifest", node.getPath());
            } else if (DataVirtLexicon.ResourceEntry.DRIVER_ENTRY_NODE_TYPE.equals(primaryType)) {
                final DataServiceEntry entry = new DataServiceEntry();
                final Node reference = findReference(node);
                entryNodeMap.put(entry, reference);

                setEntryProperties(node, entry,
                                   (String) options.get(OptionName.DRIVERS_EXPORT_FOLDER, DEFAULT_DRIVERS_EXPORT_FOLDER));
                manifest.addDriver(entry);
                LOGGER.debug("Added driver {0} to manifest", node.getPath());
            } else if (DataVirtLexicon.ConnectionEntry.NODE_TYPE.equals(primaryType)) {
                final ConnectionEntry entry = new ConnectionEntry();

                // JNDI name is required
                if (node.hasProperty(DataVirtLexicon.ConnectionEntry.JDNI_NAME)) {
                    final String jndiName = node.getProperty(DataVirtLexicon.ConnectionEntry.JDNI_NAME).getString();
                    entry.setJndiName(jndiName);
                } else {
                    LOGGER.info(TeiidI18n.missingConnectionEntryJndiName, node.getName(), dataService.getPath());
                    continue;
                }

                final Node reference = findReference(node);
                entryNodeMap.put(entry, reference);
                setEntryProperties(node, entry,
                                   (String) options.get(OptionName.CONNECTIONS_FOLDER, DEFAULT_CONNECTIONS_FOLDER));
                manifest.addConnection(entry);
                LOGGER.debug("Added connection {0} to manifest", node.getPath());
            } else if (DataVirtLexicon.VdbEntry.NODE_TYPE.equals(primaryType)) {
                final VdbEntry entry = new VdbEntry();
                final Node reference = findReference(node);
                entryNodeMap.put(entry, reference);

                setEntryProperties(node, entry, (String) options.get(OptionName.VDBS_FOLDER, DEFAULT_VDBS_FOLDER));
                setVdbProperties(node, entry, reference);
                manifest.addVdb(entry);
                LOGGER.debug("Added VDB {0} to manifest", node.getPath());
            } else {
            		LOGGER.info(TeiidI18n.dataServiceUnknownChildType, dataService.getPath(), node.getName(), primaryType);
            }
        }

        return manifest;
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.internal.AbstractExporter#doExport(javax.jcr.Node,
     *      org.teiid.modeshape.sequencer.Options, org.teiid.modeshape.sequencer.internal.AbstractExporter.ResultImpl)
     */
    @Override
    protected void doExport( final Node dataServiceNode,
                             final Options options,
                             final ResultImpl result ) {
        final Map< DataServiceEntry, Node > entryNodeMap = new HashMap< DataServiceEntry, Node >();

        try {
            switch ( getExportArtifact( options ) ) {
                case DATA_SERVICE_AS_ZIP: {
                    LOGGER.debug( "Exporting data service {0} as zip", dataServiceNode.getPath() );
                    exportAsZip( dataServiceNode, options, entryNodeMap, result );
                    break;
                }
                case MANIFEST_AS_XML: {
                    LOGGER.debug( "Exporting data service {0} manifest as XML", dataServiceNode.getPath() );
                    exportManifest( dataServiceNode, options, entryNodeMap, result );
                    break;
                }
                case SERVICE_VDB_AS_XML: {
                    LOGGER.debug( "Exporting data service {0} service VDB manifest as XML", dataServiceNode.getPath() );
                    exportServiceVdb( dataServiceNode, result );
                    break;
                }
                case DATA_SERVICE_AS_FILES: {
                    LOGGER.debug( "Exporting data service {0} as a collection of files", dataServiceNode.getPath() );
                    exportAsFiles( dataServiceNode, options, entryNodeMap, result );
                    break;
                }
                default:
                    result.setError( TeiidI18n.unhandledDataServiceExportArtifactType.text( dataServiceNode.getPath() ), null );
                    break;
            }
        } catch ( final Exception e ) {
            result.setError( TeiidI18n.unhandledErrorDuringDataServiceExport.text(), e );
        }
    }

    private void exportAsFiles( final Node dataService,
                                final Options options,
                                final Map< DataServiceEntry, Node > entryNodeMap,
                                final ResultImpl result ) throws Exception {
        final List< String > entryPaths = new ArrayList< String >();
        final List< byte[] > contents = new ArrayList< byte[] >();

        try {
            final NodeIterator itr = dataService.getNodes();

            if ( !itr.hasNext() ) {
                result.setError( TeiidI18n.missingDataServiceEntries.text( dataService.getPath() ), null );
                return;
            }

            // export the data service manifest
            final DataServiceManifest manifest = constructManifest( dataService, options, entryNodeMap );
            exportManifest( dataService, options, manifest, result );

            final byte[] manifestBytes = ( ( String )result.getOutcome() ).getBytes();
            entryPaths.add( MANIFEST_ZIP_PATH );
            contents.add( manifestBytes );
            LOGGER.debug( "Added {0} to exported data service files", MANIFEST_ZIP_PATH );

            if ( !result.wasSuccessful() ) {
                return;
            }

            { // export VDBs
                final List< VdbEntry > entries = new ArrayList< VdbEntry >();

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
                        final Result vdbResult = exporter.execute( vdb, options );

                        if ( vdbResult.wasSuccessful() ) {
                            final String xml = ( String )vdbResult.getOutcome();
                            final byte[] data = xml.getBytes();
                            entryPaths.add( entry.getPath() );
                            contents.add( data );
                            LOGGER.debug( "Added {0} VDB to exported data service files", entry.getPath() );
                        } else {
                            result.setError( vdbResult.getErrorMessage(), vdbResult.getError() );
                            return;
                        }
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
                        final Result connectionResult = exporter.execute( connection, options );

                        if ( connectionResult.wasSuccessful() ) {
                            final String xml = ( String )connectionResult.getOutcome();
                            final byte[] data = xml.getBytes();
                            entryPaths.add( entry.getPath() );
                            contents.add( data );
                            LOGGER.debug( "Added {0} connection to exported data service files", entry.getPath() );
                        } else {
                            result.setError( connectionResult.getErrorMessage(), connectionResult.getError() );
                            return;
                        }
                    }
                } else {
                    LOGGER.debug( "No connections found to export" );
                }
            }

            { // export drivers, DDLs, UDFs, and resources
                final List< DataServiceEntry > entries = new ArrayList< DataServiceEntry >();
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
                                entryPaths.add( entry.getPath() );
                                contents.add( data );
                                LOGGER.debug( "Added {0} resource to exported data service files", entry.getPath() );
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

            final String[] pathsArray = entryPaths.toArray( new String[ entryPaths.size() ] );
            result.setData( RESULT_ENTRY_PATHS, pathsArray );

            final byte[][] contentsArray = contents.toArray( new byte[ contents.size() ][] );
            result.setOutcome( contentsArray, byte[][].class );
        } catch ( final Exception e ) {
            result.setError( TeiidI18n.errorExportingDataServiceFiles.text(), e );
        }
    }

    private void exportAsZip( final Node dataService,
                              final Options options,
                              final Map< DataServiceEntry, Node > entryNodeMap,
                              final ResultImpl result ) {
        ByteArrayOutputStream bos = null;

        try {
            bos = new ByteArrayOutputStream();
            final ZipOutputStream zipStream = new ZipOutputStream( bos );
            final NodeIterator itr = dataService.getNodes();

            if ( !itr.hasNext() ) {
                result.setError( TeiidI18n.missingDataServiceEntries.text( dataService.getPath() ), null );
                return;
            }

            // export the data service manifest
            exportManifest( dataService, options, entryNodeMap, result );

            if ( !result.wasSuccessful() ) {
                return;
            }

            final String manifestXml = ( String )result.getOutcome();
            final ZipEntry manZipEntry = new ZipEntry( MANIFEST_ZIP_PATH );
            zipStream.putNextEntry( manZipEntry );
            zipStream.write( manifestXml.getBytes() );
            zipStream.closeEntry();
            LOGGER.debug( "Added manifest zip entry {0}", manZipEntry.getName() );

            // export the data service entries
            final DataServiceManifest manifest = constructManifest( dataService, options, entryNodeMap );
            LOGGER.debug( "Manifest constructed" );

            { // export VDBs
                final List< VdbEntry > entries = new ArrayList< VdbEntry >();

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
                        final Result vdbResult = exporter.execute( vdb, options );

                        if ( vdbResult.wasSuccessful() ) {
                            final String xml = ( String )vdbResult.getOutcome();
                            final byte[] data = xml.getBytes();

                            final ZipEntry zipEntry = new ZipEntry( entry.getPath() );
                            zipStream.putNextEntry( zipEntry );
                            zipStream.write( data );
                            zipStream.flush();
                            zipStream.closeEntry();
                            LOGGER.debug( "Added VDB zip entry: {0}", entry.getPath() );
                        } else {
                            result.setError( vdbResult.getErrorMessage(), vdbResult.getError() );
                            return;
                        }
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
                        final Result connectionResult = exporter.execute( connection, options );

                        if ( connectionResult.wasSuccessful() ) {
                            final String xml = ( String )connectionResult.getOutcome();
                            final byte[] data = xml.getBytes();

                            final ZipEntry zipEntry = new ZipEntry( entry.getPath() );
                            zipStream.putNextEntry( zipEntry );
                            zipStream.write( data );
                            zipStream.flush();
                            zipStream.closeEntry();
                            LOGGER.debug( "Added connection zip entry: {0}", entry.getPath() );
                        } else {
                            result.setError( connectionResult.getErrorMessage(), connectionResult.getError() );
                            return;
                        }
                    }
                } else {
                    LOGGER.debug( "No connections found to export" );
                }
            }

            { // export drivers, DDLs, UDFs, and resources
                final List< DataServiceEntry > entries = new ArrayList< DataServiceEntry >();
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

            result.setOutcome( bos.toByteArray(), byte[].class );
        } catch ( final Exception e ) {
            if ( bos != null ) {
                try {
                    bos.close();
                } catch ( IOException error ) {
                    LOGGER.error( error, TeiidI18n.errorExportingDataServiceZip );
                }
            }

            result.setError( TeiidI18n.errorExportingDataServiceZip.text(), e );
        }
    }

    private void exportManifest( final Node dataService,
                                 final Options options,
                                 final Map< DataServiceEntry, Node > entryNodeMap,
                                 final ResultImpl result ) {
        try {
            final DataServiceManifest manifest = constructManifest( dataService, options, entryNodeMap );
            exportManifest( dataService, options, manifest, result );
        } catch ( final Exception e ) {
            result.setError( TeiidI18n.errorConstructingDataServiceManifest.text(), e );
        }
    }

    private void exportManifest( final Node dataService,
                                 final Options options,
                                 final DataServiceManifest manifest,
                                 final ResultImpl result ) {
        XMLStreamWriter xmlWriter = null;

        try {
            final StringWriter stringWriter = new StringWriter();
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

                final Date modifiedDate = manifest.getLastModified();
                final String formattedDate = getDateFormatter( options ).format( modifiedDate );
                xmlWriter.writeCharacters( formattedDate );
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

            final String pretty = ( isPrettyPrint( options ) ? prettyPrint( xml, options ) : xml );
            result.setOutcome( pretty, String.class );
        } catch ( final Exception e ) {
            result.setError( TeiidI18n.errorExportingDataServiceManifest.text(), e );
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

    private void exportServiceVdb( final Node dataService,
                                   final ResultImpl result ) {
        try {
            final String queryText = String.format( FIND_SERVICE_VDB_PATTERN, dataService.getPath() );
            final Session session = dataService.getSession();
            final QueryManager queryMgr = session.getWorkspace().getQueryManager();
            final Query query = queryMgr.createQuery( queryText, Query.JCR_SQL2 );
            final QueryResult queryResult = query.execute();
            final NodeIterator itr = queryResult.getNodes();

            // make sure there is a service VDB to export
            if ( itr.getSize() == 0 ) {
                result.setError( TeiidI18n.noServiceVdbToExport.text(), null );
            } else {
                final Node serviceVdbEntry = itr.nextNode(); // should only be one
                final Node vdb = findReference( serviceVdbEntry );
                final VdbExporter exporter = new VdbExporter();
                final Result vdbExportResult = exporter.execute( vdb, result.getOptions() );

                if ( vdbExportResult.wasSuccessful() ) {
                    result.setOutcome( vdbExportResult.getOutcome(), vdbExportResult.getType() );
                } else {
                    result.setError( vdbExportResult.getErrorMessage(), vdbExportResult.getError() );
                }
            }
        } catch ( final Exception e ) {
            result.setError( TeiidI18n.errorExportingDataServiceServiceVdb.text(), e );
        }
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

    private DateFormat getDateFormatter( final Options options ) {
        assert ( options != null );
        final Object temp = options.get( OptionName.DATE_FORMATTER, DataServiceManifest.DATE_FORMATTER );

        if ( !( temp instanceof DateFormat ) ) {
            return DataServiceManifest.DATE_FORMATTER;
        }

        return ( DateFormat )temp;
    }

    private ExportArtifact getExportArtifact( final Options options ) {
        assert ( options != null );
        final Object temp = options.get( OptionName.EXPORT_ARTIFACT, ExportArtifact.DEFAULT );

        if ( !( temp instanceof ExportArtifact ) ) {
            return ExportArtifact.DEFAULT;
        }

        return ( ExportArtifact )temp;
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
                path = ( entryFolder + ( entryFolder.endsWith( "/" ) ? node.getName() : ( '/' + node.getName() ) ) );
            }

            // ensure VDB and connection entry paths have the required suffix
            if ( ( ( entry instanceof VdbEntry ) || ( entry instanceof ServiceVdbEntry ) )
                    && ( !path.endsWith( DataServiceManifest.VDB_ENTRY_SUFFIX ) ) ) {
                if ( path.endsWith( ".xml" ) ) {
                    path = ( path.substring( 0, path.lastIndexOf( ".xml" ) ) + DataServiceManifest.VDB_ENTRY_SUFFIX );
                } else {
                    path = ( path + DataServiceManifest.VDB_ENTRY_SUFFIX );
                }
            } else if ( ( entry instanceof ConnectionEntry )
                    && ( !path.endsWith( DataServiceManifest.CONNECTION_ENTRY_SUFFIX ) ) ) {
                if ( path.endsWith( ".xml" ) ) {
                    path = ( path.substring( 0, path.lastIndexOf( ".xml" ) )
                            + DataServiceManifest.CONNECTION_ENTRY_SUFFIX );
                } else {
                    path = ( path + DataServiceManifest.CONNECTION_ENTRY_SUFFIX );
                }
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
         * Export data service as a collection of <code>byte</code> arrays of files.
         */
        DATA_SERVICE_AS_FILES,

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
         * Property whose value is the zip entry folder path for metadata files (*.ddl). Default value is <code>metadata/</code>.
         */
        public String METADATA_FOLDER = "export.metadata_folder";

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

}
