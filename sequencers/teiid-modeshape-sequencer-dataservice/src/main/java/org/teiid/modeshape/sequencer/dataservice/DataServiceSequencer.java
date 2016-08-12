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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import org.modeshape.common.annotation.ThreadSafe;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.StringUtil;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.vdb.VdbDynamicSequencer;
import org.teiid.modeshape.sequencer.vdb.lexicon.VdbLexicon;

/**
 * A sequencer of Teiid data service archives.
 */
@ThreadSafe
public class DataServiceSequencer extends Sequencer {

    /**
     * A system property for storing the absolute root path where {@link Connection connections} should be sequenced. If no value
     * is set, it defaults to the same parent path as the data service node.
     */
    public static final String CONNECTION_PATH_PROPERTY = "dv.connection.path";

    /**
     * A system property for storing the absolute root path where driver archives should be sequenced. If no value is set, it
     * defaults to the same parent path as the data service node.
     */
    public static final String DRIVER_PATH_PROPERTY = "dv.driver.path";

    private static final Logger LOGGER = Logger.getLogger( DataServiceSequencer.class );

    private static final String MANIFEST_FILE = "META-INF/dataservice.xml";

    /**
     * A system property for storing the absolute root path where metadata files, like DDL, should be sequenced. If no value is
     * set, it defaults to the same parent path as the data service node.
     */
    public static final String METADATA_PATH_PROPERTY = "dv.metadata.path";

    /**
     * A system property for storing the absolute root path where miscellaneous files should be sequenced. If no value is set, it
     * defaults to the same parent path as the data service node.
     */
    public static final String RESOURCE_PATH_PROPERTY = "dv.resource.path";

    /**
     * A system property for storing the absolute root path where UDF archives should be sequenced. If no value is set, it
     * defaults to the same parent path as the data service node.
     */
    public static final String UDF_PATH_PROPERTY = "dv.udf.path";

    /**
     * A system property for storing the absolute root path where VDBs should be sequenced. If no value is set, it defaults to the
     * same parent path as the data service node.
     */
    public static final String VDB_PATH_PROPERTY = "dv.vdb.path";

    private String connectionPath;

    private ConnectionSequencer datasourceSequencer; // constructed during initialize method

    private String driverPath;

    private String metadataPath;

    private String resourcePath;

    private String udfPath;

    private String vdbPath;

    private VdbDynamicSequencer vdbSequencer; // constructed during initialize method

    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#execute(javax.jcr.Property, javax.jcr.Node,
     *      org.modeshape.jcr.api.sequencer.Sequencer.Context)
     */
    @Override
    public boolean execute( final Property inputProperty,
                            final Node outputNode,
                            final Context context ) throws Exception {
        LOGGER.debug( "DataServiceSequencer.execute called:outputNode name='{0}', path='{1}'",
                      outputNode.getName(),
                      outputNode.getPath() );

        final Binary binaryValue = inputProperty.getBinary();
        DataServiceManifest manifest = null;
        Node serviceVdbEntryNode = null;

        try {
            // read manifest
            try (
            final ZipInputStream zis = new ZipInputStream( Objects.requireNonNull( binaryValue, "binaryValue" ).getStream() ) ) {
                ZipEntry entry = null;

                while ( ( entry = zis.getNextEntry() ) != null ) {
                    final String entryName = entry.getName();

                    if ( entry.isDirectory() ) {
                        LOGGER.debug( "ignoring directory '{0}'", entryName );
                        continue;
                    } else if ( entryName.endsWith( MANIFEST_FILE ) ) {
                        manifest = readManifest( binaryValue, zis, outputNode, context );
                        break;
                    } else {
                        LOGGER.debug( "skipping '{0}' and will sequence later", entryName );
                    }
                }
            }

            // make sure we have a manifest
            if ( manifest == null ) {
                throw new RuntimeException( TeiidI18n.missingDataServiceManifestFile.text( inputProperty.getPath() ) );
            }

            final ServiceVdbEntry serviceVdb = manifest.getServiceVdb();
            LOGGER.debug( ( serviceVdb == null ) ? "no service VDB found" : "found service VDB" );

            // sequence service VDB if necessary
            if ( serviceVdb != null ) {
                final String serviceVdbPath = serviceVdb.getPath();

                try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
                    ZipEntry entry = null;

                    while ( ( entry = zis.getNextEntry() ) != null ) {
                        final String entryName = entry.getName();

                        if ( entry.isDirectory() ) {
                            LOGGER.debug( "ignoring directory '{0}'", entryName );
                            continue;
                        } else if ( entryName.equals( serviceVdbPath ) ) {
                            serviceVdbEntryNode = sequenceServiceVdb( zis, serviceVdb, outputNode );
                            break;
                        } else if ( entryName.endsWith( MANIFEST_FILE ) ) {
                            LOGGER.debug( "already read the manifest" );
                            continue;
                        } else {
                            LOGGER.debug( "skipping '{0}' and will sequence later", entryName );
                        }
                    }
                }
            }

            // sequence everything else
            sequenceFiles( manifest, binaryValue, outputNode );
            sequenceConnections( manifest, binaryValue, outputNode, inputProperty, context );
            sequenceVdbs( manifest, binaryValue, outputNode, serviceVdbEntryNode, inputProperty, context );

            if ( LOGGER.isDebugEnabled() ) {
                final NodeIterator itr = outputNode.getNodes();

                while ( itr.hasNext() ) {
                    final Node kid = itr.nextNode();
                    final long numKids = kid.getNodes().getSize();
                    LOGGER.debug( kid.getPath() + " : type = " + kid.getPrimaryNodeType().getName() + ", num kids = "
                                  + kid.getNodes().getSize() );

                    if ( numKids > 0 ) {
                        final NodeIterator kidItr = kid.getNodes();

                        while ( kidItr.hasNext() ) {
                            final Node node = kidItr.nextNode();
                            LOGGER.debug( "    " + node.getPath() + " : type = " + node.getPrimaryNodeType().getName()
                                          + ", num kids = " + node.getNodes().getSize() );
                        }
                    }
                }
            }

            return true;
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.errorReadingDataserviceFile.text( inputProperty.getPath(), e.getMessage() ),
                                        e );
        }
    }

    private ConnectionEntry findConnectionEntry( final String path,
                                                 final DataServiceManifest manifest ) {
        for ( final ConnectionEntry dsEntry : manifest.getDataSources() ) {
            if ( dsEntry.getPath().equals( path ) ) {
                return dsEntry;
            }
        }

        return null;
    }

    private DataServiceEntry findDriverEntry( final String path,
                                              final DataServiceManifest manifest ) {
        for ( final DataServiceEntry entry : manifest.getDrivers() ) {
            if ( entry.getPath().equals( path ) ) {
                return entry;
            }
        }

        return null;
    }

    private Node findExistingNode( final Node parentNode,
                                   final DataServiceEntry entry,
                                   final String primaryNodeType ) throws Exception {
        final String path = ( parentNode.getPath() + '/' + entry.getEntryName() );

        // see if a node exists with that name
        if ( parentNode.getSession().nodeExists( path ) ) {
            final NodeIterator itr = parentNode.getNodes();

            // see if type matches
            while ( itr.hasNext() ) {
                final Node node = itr.nextNode();

                if ( node.getPrimaryNodeType().getName().equals( primaryNodeType ) ) {
                    LOGGER.debug( "found existing node at path {0} with type of {1}", node.getPath(), primaryNodeType );
                    return node;
                }
            }
        }

        LOGGER.debug( "No existing child node found at parent {0} with name of {1} and type of {2}",
                      parentNode.getPath(),
                      entry.getEntryName(),
                      primaryNodeType );
        return null;
    }

    private DataServiceEntry findMetadataEntry( final String path,
                                                final DataServiceManifest manifest ) {
        for ( final DataServiceEntry entry : manifest.getMetadata() ) {
            if ( entry.getPath().equals( path ) ) {
                return entry;
            }
        }

        return null;
    }

    private DataServiceEntry findResourceEntry( final String path,
                                                final DataServiceManifest manifest ) {
        for ( final DataServiceEntry entry : manifest.getResources() ) {
            if ( entry.getPath().equals( path ) ) {
                return entry;
            }
        }

        return null;
    }

    private DataServiceEntry findUdfEntry( final String path,
                                           final DataServiceManifest manifest ) {
        for ( final DataServiceEntry entry : manifest.getUdfs() ) {
            if ( entry.getPath().equals( path ) ) {
                return entry;
            }
        }

        return null;
    }

    private VdbEntry findVdbEntry( final String path,
                                   final DataServiceManifest manifest ) {
        for ( final VdbEntry entry : manifest.getVdbs() ) {
            if ( entry.getPath().equals( path ) ) {
                return entry;
            }
        }

        // see if a service VDB dependency
        if ( manifest.getServiceVdb() != null ) {
            for ( final VdbEntry entry : manifest.getServiceVdb().getVdbs() ) {
                if ( entry.getPath().equals( path ) ) {
                    return entry;
                }
            }
        }

        return null;
    }

    private Node getConnectionRoot( final Node dataServiceNode ) throws Exception {
        String path = ( ( this.connectionPath == null ) ? null : this.connectionPath );

        if ( StringUtil.isBlank( path ) ) {
            path = System.getProperty( CONNECTION_PATH_PROPERTY );
        }

        if ( !StringUtil.isBlank( path ) && dataServiceNode.getSession().nodeExists( path ) ) {
            return dataServiceNode.getSession().getNode( path );
        }

        return dataServiceNode.getParent();
    }

    private Node getDriverRoot( final Node dataServiceNode ) throws Exception {
        String path = ( ( this.driverPath == null ) ? null : this.driverPath );

        if ( StringUtil.isBlank( path ) ) {
            path = System.getProperty( DRIVER_PATH_PROPERTY );
        }

        if ( !StringUtil.isBlank( path ) && dataServiceNode.getSession().nodeExists( path ) ) {
            return dataServiceNode.getSession().getNode( path );
        }

        return dataServiceNode.getParent();
    }

    private Node getMetadataRoot( final Node dataServiceNode ) throws Exception {
        String path = ( ( this.metadataPath == null ) ? null : this.metadataPath );

        if ( StringUtil.isBlank( path ) ) {
            path = System.getProperty( METADATA_PATH_PROPERTY );
        }

        if ( !StringUtil.isBlank( path ) && dataServiceNode.getSession().nodeExists( path ) ) {
            return dataServiceNode.getSession().getNode( path );
        }

        return dataServiceNode.getParent();
    }

    private Node getResourceRoot( final Node dataServiceNode ) throws Exception {
        String path = ( ( this.resourcePath == null ) ? null : this.resourcePath );

        if ( StringUtil.isBlank( path ) ) {
            path = System.getProperty( RESOURCE_PATH_PROPERTY );
        }

        if ( !StringUtil.isBlank( path ) && dataServiceNode.getSession().nodeExists( path ) ) {
            return dataServiceNode.getSession().getNode( path );
        }

        return dataServiceNode.getParent();
    }

    private Node getUdfRoot( final Node dataServiceNode ) throws Exception {
        String path = ( ( this.udfPath == null ) ? null : this.udfPath );

        if ( StringUtil.isBlank( path ) ) {
            path = System.getProperty( UDF_PATH_PROPERTY );
        }

        if ( !StringUtil.isBlank( path ) && dataServiceNode.getSession().nodeExists( path ) ) {
            return dataServiceNode.getSession().getNode( path );
        }

        return dataServiceNode.getParent();
    }

    private Node getVdbRoot( final Node dataServiceNode ) throws Exception {
        String path = ( ( this.vdbPath == null ) ? null : this.vdbPath );

        if ( StringUtil.isBlank( path ) ) {
            path = System.getProperty( VDB_PATH_PROPERTY );
        }

        if ( !StringUtil.isBlank( path ) && dataServiceNode.getSession().nodeExists( path ) ) {
            return dataServiceNode.getSession().getNode( path );
        }

        return dataServiceNode.getParent();
    }

    /**
     * @throws IOException
     * @see org.modeshape.jcr.api.sequencer.Sequencer#initialize(javax.jcr.NamespaceRegistry,
     *      org.modeshape.jcr.api.nodetype.NodeTypeManager)
     */
    @Override
    public void initialize( final NamespaceRegistry registry,
                            final NodeTypeManager nodeTypeManager ) throws RepositoryException, IOException {
        LOGGER.debug( "enter initialize" );

        registerNodeTypes( "dv.cnd", nodeTypeManager, true );
        LOGGER.debug( "dv.cnd loaded" );

        this.vdbSequencer = new VdbDynamicSequencer();
        this.vdbSequencer.initialize( registry, nodeTypeManager );

        this.datasourceSequencer = new ConnectionSequencer();
        this.datasourceSequencer.initialize( registry, nodeTypeManager );

        LOGGER.debug( "exit initialize" );
    }

    private DataServiceManifest readManifest( final Binary binaryValue,
                                              final InputStream inputStream,
                                              final Node outputNode,
                                              final Context context ) throws Exception {
        LOGGER.debug( "before reading manifest xml" );

        final DataServiceManifest manifest = DataServiceManifest.read( inputStream );
        outputNode.setPrimaryType( DataVirtLexicon.DataService.NODE_TYPE );
        outputNode.setProperty( DataVirtLexicon.DataService.NAME, manifest.getName() );

        // description
        if ( !StringUtil.isBlank( manifest.getDescription() ) ) {
            outputNode.setProperty( DataVirtLexicon.DataService.DESCRIPTION, manifest.getDescription() );
        }

        // modified by
        if ( !StringUtil.isBlank( manifest.getModifiedBy() ) ) {
            outputNode.setProperty( DataVirtLexicon.DataService.MODIFIED_BY, manifest.getModifiedBy() );
        }

        // last modified date
        if ( manifest.getLastModified() != null ) {
            final LocalDateTime modifiedDate = manifest.getLastModified();
            final Calendar calendar = GregorianCalendar.from( modifiedDate.atZone( ZoneId.systemDefault() ) );
            outputNode.setProperty( DataVirtLexicon.DataService.LAST_MODIFIED, calendar );
        }

        // generic properties
        if ( !manifest.getProperties().isEmpty() ) {
            final Properties props = manifest.getProperties();

            for ( final String propName : props.stringPropertyNames() ) {
                outputNode.setProperty( propName, props.getProperty( propName ) );
            }
        }

        LOGGER.debug( "done reading manifest xml\n\n" );
        return manifest;
    }

    private void sequenceConnection( final InputStream stream,
                                     final ConnectionEntry dsEntry,
                                     final Node dataServiceNode ) throws Exception {
        final Node dsEntryNode = dataServiceNode.addNode( dsEntry.getEntryName(), DataVirtLexicon.ConnectionEntry.NODE_TYPE );
        dsEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.PATH, dsEntry.getPath() );
        dsEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.JDNI_NAME, dsEntry.getJndiName() );

        // sequence connection if necessary
        boolean shouldSequence = false;

        switch ( dsEntry.getPublishPolicy() ) {
            case ALWAYS:
                shouldSequence = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( getConnectionRoot( dataServiceNode ),
                                                     dsEntry,
                                                     DataVirtLexicon.Connection.NODE_TYPE );

                if ( match == null ) {
                    shouldSequence = true;
                } else {
                    // add reference to existing connection node from the connection entry node
                    final Value ref = dataServiceNode.getSession().getValueFactory().createValue( match );
                    dsEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.CONNECTION_REF, ref );
                }

                break;
            case NEVER:
                break;
            default:
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( dsEntry.getPublishPolicy(),
                                                                                   dsEntry.getPath() ) );
        }

        if ( shouldSequence ) {
            final Node parent = getConnectionRoot( dataServiceNode );
            final Node dsNode = parent.addNode( dsEntry.getEntryName(), DataVirtLexicon.Connection.NODE_TYPE );
            final boolean success = this.datasourceSequencer.sequenceConnection( stream, dsNode );

            if ( success ) {
                // reference sequenced node from the connection entry
                final Value ref = dataServiceNode.getSession().getValueFactory().createValue( dsNode );
                dsEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.CONNECTION_REF, ref );
            } else {
                dsNode.remove();
                dsEntryNode.remove();
                throw new Exception( TeiidI18n.dataSourceNotSequenced.text( dsEntry.getPath() ) );
            }
        }
    }

    private void sequenceConnections( final DataServiceManifest manifest,
                                      final Binary binaryValue,
                                      final Node dataServiceNode,
                                      final Property inputProperty,
                                      final Context context ) throws Exception {
        LOGGER.debug( "sequenceDataSources called: all connections sequenced at once" );
        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                final String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    continue;
                }

                if ( findConnectionEntry( entryName, manifest ) != null ) {
                    final ConnectionEntry dsEntry = findConnectionEntry( entryName, manifest );
                    sequenceConnection( zis, dsEntry, dataServiceNode );
                }
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.dataSourceSequencingError.text(), e );
        }
    }

    private void sequenceDriver( final ZipInputStream zis,
                                 final DataServiceEntry driverEntry,
                                 final Node dataServiceNode ) throws Exception {
        sequenceFile( zis,
                      driverEntry,
                      dataServiceNode,
                      getDriverRoot( dataServiceNode ),
                      DataVirtLexicon.DriverEntry.NODE_TYPE,
                      DataVirtLexicon.DriverFile.NODE_TYPE );
    }

    private void sequenceFile( final ZipInputStream zis,
                               final DataServiceEntry entry,
                               final Node dataServiceNode,
                               final Node resourceParentNode,
                               final String entryNodeType,
                               final String fileNodeType ) throws Exception {
        final Node entryNode = dataServiceNode.addNode( entry.getEntryName(), entryNodeType );
        entryNode.setProperty( DataVirtLexicon.DataServiceEntry.PATH, entry.getPath() );

        // save file if necessary
        boolean save = false;

        switch ( entry.getPublishPolicy() ) {
            case ALWAYS:
                save = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( resourceParentNode, entry, fileNodeType );

                if ( match == null ) {
                    save = true;
                } else {
                    // add reference to existing file node to the entry node
                    final Value ref = dataServiceNode.getSession().getValueFactory().createValue( match );
                    entryNode.setProperty( DataVirtLexicon.DataServiceEntry.SOURCE_RESOURCE, ref );
                }

                break;
            case NEVER:
                break;
            default:
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( entry.getPublishPolicy(), entry.getPath() ) );
        }

        if ( save ) {
            final Node fileNode = resourceParentNode.addNode( entry.getEntryName(), fileNodeType );

            // add reference to file node to its entry node
            final ValueFactory valueFactory = dataServiceNode.getSession().getValueFactory();
            final Value ref = valueFactory.createValue( fileNode );
            entryNode.setProperty( DataVirtLexicon.DataServiceEntry.SOURCE_RESOURCE, ref );

            // upload file
            final byte[] buf = new byte[ 1024 ];
            final File file = File.createTempFile( entry.getEntryName(), null );

            try ( final FileOutputStream fos = new FileOutputStream( file ) ) {
                int numRead = 0;

                while ( ( numRead = zis.read( buf ) ) > 0 ) {
                    fos.write( buf, 0, numRead );
                }
            }

            // set content and data properties
            final InputStream fileContent = new BufferedInputStream( new FileInputStream( file ) );
            final Binary binary = valueFactory.createBinary( fileContent );
            final Node contentNode = fileNode.addNode( JcrConstants.JCR_CONTENT, JcrConstants.NT_RESOURCE );
            contentNode.setProperty( JcrConstants.JCR_DATA, binary );

            // set last modified property
            final Calendar lastModified = Calendar.getInstance();
            lastModified.setTimeInMillis( file.lastModified() );
            contentNode.setProperty( "jcr:lastModified", lastModified );
        }
    }

    private void sequenceFiles( final DataServiceManifest manifest,
                                final Binary binaryValue,
                                final Node dataServiceNode ) throws Exception {
        LOGGER.debug( "sequenceFiles called: all files sequenced at once" );
        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                final String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    continue;
                }

                if ( findDriverEntry( entryName, manifest ) != null ) {
                    sequenceDriver( zis, findDriverEntry( entryName, manifest ), dataServiceNode );
                } else if ( findMetadataEntry( entryName, manifest ) != null ) {
                    sequenceMetadata( zis, findMetadataEntry( entryName, manifest ), dataServiceNode );
                } else if ( findResourceEntry( entryName, manifest ) != null ) {
                    sequenceResource( zis, findResourceEntry( entryName, manifest ), dataServiceNode );
                } else if ( findUdfEntry( entryName, manifest ) != null ) {
                    sequenceUdf( zis, findUdfEntry( entryName, manifest ), dataServiceNode );
                }
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.fileSequencingError.text( dataServiceNode.getPath() ), e );
        }
    }

    private void sequenceMetadata( final ZipInputStream zis,
                                   final DataServiceEntry metadataEntry,
                                   final Node dataServiceNode ) throws Exception {
        sequenceFile( zis,
                      metadataEntry,
                      dataServiceNode,
                      getMetadataRoot( dataServiceNode ),
                      DataVirtLexicon.MetadataEntry.DDL_FILE_NODE_TYPE,
                      DataVirtLexicon.MetadaFile.DDL_FILE_NODE_TYPE );
    }

    private void sequenceResource( final ZipInputStream zis,
                                   final DataServiceEntry resourceEntry,
                                   final Node dataServiceNode ) throws Exception {
        sequenceFile( zis,
                      resourceEntry,
                      dataServiceNode,
                      getResourceRoot( dataServiceNode ),
                      DataVirtLexicon.ResourceEntry.NODE_TYPE,
                      DataVirtLexicon.ResourceFile.NODE_TYPE );
    }

    private Node sequenceServiceVdb( final InputStream stream,
                                     final ServiceVdbEntry vdbEntry,
                                     final Node dataServiceNode ) throws Exception {
        LOGGER.debug( "sequenceServiceVdb" );
        final Node vdbEntryNode = dataServiceNode.addNode( vdbEntry.getEntryName(), DataVirtLexicon.ServiceVdbEntry.NODE_TYPE );
        vdbEntryNode.setProperty( DataVirtLexicon.ServiceVdbEntry.PATH, vdbEntry.getPath() );
        vdbEntryNode.setProperty( DataVirtLexicon.ServiceVdbEntry.VDB_NAME, vdbEntry.getVdbName() );
        vdbEntryNode.setProperty( DataVirtLexicon.ServiceVdbEntry.VDB_NAME, vdbEntry.getVdbName() );

        // sequence data source if necessary
        boolean shouldSequence = false;

        switch ( vdbEntry.getPublishPolicy() ) {
            case ALWAYS:
                shouldSequence = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( getVdbRoot( dataServiceNode ), vdbEntry, VdbLexicon.Vdb.VIRTUAL_DATABASE );

                if ( match == null ) {
                    shouldSequence = true;
                } else {
                    // add reference to existing node
                    final Value ref = dataServiceNode.getSession().getValueFactory().createValue( match );
                    vdbEntryNode.setProperty( DataVirtLexicon.DataServiceEntry.SOURCE_RESOURCE, ref );
                }

                break;
            case NEVER:
                break;
            default:
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( vdbEntry.getPublishPolicy(),
                                                                                   vdbEntry.getPath() ) );
        }

        if ( shouldSequence ) {
            final Node parent = getVdbRoot( dataServiceNode );
            final Node vdbNode = parent.addNode( vdbEntry.getEntryName(), VdbLexicon.Vdb.VIRTUAL_DATABASE );
            final boolean success = this.vdbSequencer.sequenceVdb( stream, vdbNode );

            if ( success ) {
                // reference sequenced node from the connection entry
                final Value ref = dataServiceNode.getSession().getValueFactory().createValue( vdbNode );
                vdbEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.CONNECTION_REF, ref );
            } else {
                vdbNode.remove();
                vdbEntryNode.remove();
                throw new Exception( TeiidI18n.dataSourceNotSequenced.text( vdbEntry.getPath() ) );
            }
        }

        return vdbEntryNode;
    }

    private void sequenceUdf( final ZipInputStream zis,
                              final DataServiceEntry udfEntry,
                              final Node dataServiceNode ) throws Exception {
        sequenceFile( zis,
                      udfEntry,
                      dataServiceNode,
                      getUdfRoot( dataServiceNode ),
                      DataVirtLexicon.ResourceEntry.UDF_FILE_NODE_TYPE,
                      DataVirtLexicon.ResourceFile.UDF_FILE_NODE_TYPE );
    }

    private void sequenceVdb( final InputStream stream,
                              final Node dataServiceNode,
                              final Node serviceVdbNode,
                              final VdbEntry vdbEntry,
                              final Property inputProperty,
                              final Context context ) throws Exception {
        final Node vdbEntryNode = ( ( vdbEntry.getContainer() instanceof DataServiceManifest ) ? dataServiceNode.addNode( vdbEntry.getEntryName(),
                                                                                                                          DataVirtLexicon.VdbEntry.NODE_TYPE )
                                                                                               : serviceVdbNode.addNode( vdbEntry.getEntryName(),
                                                                                                                         DataVirtLexicon.VdbEntry.NODE_TYPE ) );
        vdbEntryNode.setProperty( DataVirtLexicon.VdbEntry.PATH, vdbEntry.getPath() );
        vdbEntryNode.setProperty( DataVirtLexicon.VdbEntry.VDB_NAME, vdbEntry.getVdbName() );
        vdbEntryNode.setProperty( DataVirtLexicon.VdbEntry.VDB_VERSION, vdbEntry.getVdbVersion() );

        // sequence VDB if necessary
        boolean shouldSequence = false;

        switch ( vdbEntry.getPublishPolicy() ) {
            case ALWAYS:
                shouldSequence = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( getVdbRoot( dataServiceNode ), vdbEntry, VdbLexicon.Vdb.VIRTUAL_DATABASE );

                if ( match == null ) {
                    shouldSequence = true;
                } else {
                    // add reference to existing node
                    final Value ref = dataServiceNode.getSession().getValueFactory().createValue( match );
                    vdbEntryNode.setProperty( DataVirtLexicon.DataServiceEntry.SOURCE_RESOURCE, ref );
                }

                break;
            case NEVER:
                break;
            default:
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( vdbEntry.getPublishPolicy(),
                                                                                   vdbEntry.getPath() ) );
        }

        if ( shouldSequence ) {
            final Node parent = getVdbRoot( dataServiceNode );
            final Node vdbNode = parent.addNode( vdbEntry.getEntryName(), VdbLexicon.Vdb.VIRTUAL_DATABASE );
            final boolean success = this.vdbSequencer.sequenceVdb( stream, vdbNode );

            if ( success ) {
                // reference sequenced node from the VDB entry
                final Value ref = dataServiceNode.getSession().getValueFactory().createValue( vdbNode );
                vdbEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.CONNECTION_REF, ref );
            } else {
                vdbNode.remove();
                vdbEntryNode.remove();
                throw new Exception( TeiidI18n.dataSourceNotSequenced.text( vdbEntry.getPath() ) );
            }
        }
    }

    private void sequenceVdbs( final DataServiceManifest manifest,
                               final Binary binaryValue,
                               final Node dataServiceNode,
                               final Node serviceVdbEntryNode,
                               final Property inputProperty,
                               final Context context ) throws Exception {
        LOGGER.debug( "sequenceVdbs called: all VDBs sequenced at once" );
        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                final String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    continue;
                }

                if ( findVdbEntry( entryName, manifest ) != null ) {
                    final VdbEntry vdbEntry = findVdbEntry( entryName, manifest );
                    sequenceVdb( zis, dataServiceNode, serviceVdbEntryNode, vdbEntry, inputProperty, context );
                }
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.vdbSequencingError.text( dataServiceNode.getPath() ), e );
        }
    }

    /**
     * @param connectionPath the absolute path of the root node where connection files are sequenced (can be <code>null</code> or
     *        empty if {@link #CONNECTION_PATH_PROPERTY} or the default path should be used)
     */
    public void setConnectionPath( final String connectionPath ) {
        this.connectionPath = ( StringUtil.isBlank( connectionPath ) ? null : connectionPath );
    }

    /**
     * @param driverPath the absolute path of the root node where driver files are sequenced (can be <code>null</code> or empty if
     *        {@link #DRIVER_PATH_PROPERTY} or the default path should be used)
     */
    public void setDriverPath( final String driverPath ) {
        this.driverPath = ( StringUtil.isBlank( driverPath ) ? null : driverPath );
    }

    /**
     * @param metadataPath the absolute path of the root node where metadata files are sequenced (can be <code>null</code> or
     *        empty if {@link #METADATA_PATH_PROPERTY} or the default path should be used)
     */
    public void setMetadataPath( final String metadataPath ) {
        this.metadataPath = ( StringUtil.isBlank( metadataPath ) ? null : metadataPath );
    }

    /**
     * @param resourcePath the absolute path of the root node where miscellaneous files are sequenced (can be <code>null</code> or
     *        empty if {@link #RESOURCE_PATH_PROPERTY} or the default path should be used)
     */
    public void setResourcePath( final String resourcePath ) {
        this.resourcePath = ( StringUtil.isBlank( resourcePath ) ? null : resourcePath );
    }

    /**
     * @param udfPath the absolute path of the root node where UDF files are sequenced (can be <code>null</code> or empty if
     *        {@link #UDF_PATH_PROPERTY} or the default path should be used)
     */
    public void setUdfPath( final String udfPath ) {
        this.udfPath = ( StringUtil.isBlank( udfPath ) ? null : udfPath );
    }

    /**
     * @param vdbPath the absolute path of the root node where VDB files are sequenced (can be <code>null</code> or empty if
     *        {@link #VDB_PATH_PROPERTY} or the default path should be used)
     */
    public void setVdbPath( final String vdbPath ) {
        this.vdbPath = ( StringUtil.isBlank( vdbPath ) ? null : vdbPath );
    }

}
