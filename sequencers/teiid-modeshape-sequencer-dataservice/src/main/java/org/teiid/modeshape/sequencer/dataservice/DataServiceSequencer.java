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
 * A sequencer of Teiid Dataservice files.
 */
@ThreadSafe
public class DataServiceSequencer extends Sequencer {

    private static final Logger LOGGER = Logger.getLogger( DataServiceSequencer.class );
    private static final String MANIFEST_FILE = "META-INF/dataservice.xml";

    private DataSourceSequencer datasourceSequencer; // constructed during initialize method
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
            sequenceDataSources( manifest, binaryValue, outputNode, inputProperty, context );
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

    private DataSourceEntry findDataSourceEntry( final String path,
                                                 final DataServiceManifest manifest ) {
        for ( final DataSourceEntry dsEntry : manifest.getDataSources() ) {
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

    private Node findExistingNode( final Node dataServiceNode,
                                   final DataServiceEntry entry,
                                   final String primaryNodeType ) throws Exception {
        final Node parent = dataServiceNode.getParent();
        final String path = ( parent.getPath() + '/' + entry.getEntryName() );

        // see if a node exists with that name
        if ( parent.getSession().nodeExists( path ) ) {
            final NodeIterator itr = parent.getNodes();

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
                      parent.getPath(),
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

        this.datasourceSequencer = new DataSourceSequencer();
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
        outputNode.setProperty( DataVirtLexicon.DataServiceArchive.NAME, manifest.getName() );

        // description
        if ( !StringUtil.isBlank( manifest.getDescription() ) ) {
            outputNode.setProperty( DataVirtLexicon.DataServiceArchive.DESCRIPTION, manifest.getDescription() );
        }

        // modified by
        if ( !StringUtil.isBlank( manifest.getModifiedBy() ) ) {
            outputNode.setProperty( DataVirtLexicon.DataServiceArchive.MODIFIED_BY, manifest.getModifiedBy() );
        }

        // last modified date
        if ( manifest.getLastModified() != null ) {
            final LocalDateTime modifiedDate = manifest.getLastModified();
            final Calendar calendar = GregorianCalendar.from( modifiedDate.atZone( ZoneId.systemDefault() ) );
            outputNode.setProperty( DataVirtLexicon.DataServiceArchive.LAST_MODIFIED, calendar );
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

    private void sequenceDataSource( final InputStream stream,
                                     final DataSourceEntry dsEntry,
                                     final Node dataServiceNode ) throws Exception {
        final Node dsEntryNode = dataServiceNode.addNode( dsEntry.getEntryName(), DataVirtLexicon.DataSourceEntry.NODE_TYPE );
        dsEntryNode.setProperty( DataVirtLexicon.DataSourceEntry.PATH, dsEntry.getPath() );
        dsEntryNode.setProperty( DataVirtLexicon.DataSourceEntry.JDNI_NAME, dsEntry.getJndiName() );

        // sequence data source if necessary
        boolean shouldSequence = false;

        switch ( dsEntry.getDeployPolicy() ) {
            case ALWAYS:
                shouldSequence = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( dataServiceNode, dsEntry, DataVirtLexicon.DataSource.NODE_TYPE );

                if ( match == null ) {
                    shouldSequence = true;
                } else {
                    // add reference to existing data source node from the data source entry node
                    final Value ref = dataServiceNode.getSession().getValueFactory().createValue( match );
                    dsEntryNode.setProperty( DataVirtLexicon.DataSourceEntry.DATA_SOURCE_REF, ref );
                }

                break;
            case NEVER:
                break;
            default:
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( dsEntry.getDeployPolicy(),
                                                                                   dsEntry.getPath() ) );
        }

        if ( shouldSequence ) {
            // put data source node as sibling of data service node
            final Node parent = dataServiceNode.getParent();
            final Node dsNode = parent.addNode( dsEntry.getEntryName(), DataVirtLexicon.DataSource.NODE_TYPE );
            final boolean success = this.datasourceSequencer.sequenceDatasource( stream, dsNode );

            if ( success ) {
                // reference sequenced node from the data source entry
                final Value ref = dataServiceNode.getSession().getValueFactory().createValue( dsNode );
                dsEntryNode.setProperty( DataVirtLexicon.DataSourceEntry.DATA_SOURCE_REF, ref );
            } else {
                dsNode.remove();
                dsEntryNode.remove();
                throw new Exception( TeiidI18n.dataSourceNotSequenced.text( dsEntry.getPath() ) );
            }
        }
    }

    private void sequenceDataSources( final DataServiceManifest manifest,
                                      final Binary binaryValue,
                                      final Node dataServiceNode,
                                      final Property inputProperty,
                                      final Context context ) throws Exception {
        LOGGER.debug( "sequenceDataSources called: all data sources sequenced at once" );
        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                final String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    continue;
                }

                if ( findDataSourceEntry( entryName, manifest ) != null ) {
                    final DataSourceEntry dsEntry = findDataSourceEntry( entryName, manifest );
                    sequenceDataSource( zis, dsEntry, dataServiceNode );
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
                      DataVirtLexicon.DriverEntry.NODE_TYPE,
                      DataVirtLexicon.DriverFile.NODE_TYPE );
    }

    private void sequenceFile( final ZipInputStream zis,
                               final DataServiceEntry entry,
                               final Node dataServiceNode,
                               final String entryNodeType,
                               final String fileNodeType ) throws Exception {
        final Node entryNode = dataServiceNode.addNode( entry.getEntryName(), entryNodeType );
        entryNode.setProperty( DataVirtLexicon.DataServiceEntry.PATH, entry.getPath() );

        // TODO only do this if we need to upload
        // extract file
        final byte[] buf = new byte[ 1024 ];
        final File file = File.createTempFile( entry.getEntryName(), null );

        try ( final FileOutputStream fos = new FileOutputStream( file ) ) {
            int numRead = 0;

            while ( ( numRead = zis.read( buf ) ) > 0 ) {
                fos.write( buf, 0, numRead );
            }
        }

        // save file if necessary
        boolean save = false;

        switch ( entry.getDeployPolicy() ) {
            case ALWAYS:
                save = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( dataServiceNode, entry, fileNodeType );

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
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( entry.getDeployPolicy(), entry.getPath() ) );
        }

        if ( save ) {
            // add file node as sibling of data service node
            final Node parent = dataServiceNode.getParent();
            final Node fileNode = parent.addNode( entry.getEntryName(), fileNodeType );

            // add reference to file node to its entry node
            final ValueFactory valueFactory = dataServiceNode.getSession().getValueFactory();
            final Value ref = valueFactory.createValue( fileNode );
            entryNode.setProperty( DataVirtLexicon.DataServiceEntry.SOURCE_RESOURCE, ref );

            // upload file
            final Node contentNode = fileNode.addNode( JcrConstants.JCR_CONTENT, JcrConstants.NT_RESOURCE );

            // set data property
            final InputStream fileContent = new BufferedInputStream( new FileInputStream( file ) );
            final Binary binary = valueFactory.createBinary( fileContent );
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
                      DataVirtLexicon.MetadataEntry.DDL_FILE_NODE_TYPE,
                      DataVirtLexicon.MetadaFile.DDL_FILE_NODE_TYPE );
    }

    private void sequenceResource( final ZipInputStream zis,
                                   final DataServiceEntry resourceEntry,
                                   final Node dataServiceNode ) throws Exception {
        sequenceFile( zis,
                      resourceEntry,
                      dataServiceNode,
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

        switch ( vdbEntry.getDeployPolicy() ) {
            case ALWAYS:
                shouldSequence = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( dataServiceNode, vdbEntry, VdbLexicon.Vdb.VIRTUAL_DATABASE );

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
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( vdbEntry.getDeployPolicy(),
                                                                                   vdbEntry.getPath() ) );
        }

        if ( shouldSequence ) {
            // put vdb node as sibling of data service node
            final Node parent = dataServiceNode.getParent();
            final Node vdbNode = parent.addNode( vdbEntry.getEntryName(), VdbLexicon.Vdb.VIRTUAL_DATABASE );
            final boolean success = this.vdbSequencer.sequenceVdb( stream, vdbNode );

            if ( success ) {
                // reference sequenced node from the data source entry
                final Value ref = dataServiceNode.getSession().getValueFactory().createValue( vdbNode );
                vdbEntryNode.setProperty( DataVirtLexicon.DataSourceEntry.DATA_SOURCE_REF, ref );
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

        switch ( vdbEntry.getDeployPolicy() ) {
            case ALWAYS:
                shouldSequence = true;
                break;
            case IF_MISSING:
                final Node match = findExistingNode( dataServiceNode, vdbEntry, VdbLexicon.Vdb.VIRTUAL_DATABASE );

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
                throw new RuntimeException( TeiidI18n.unexpectedDeployPolicy.text( vdbEntry.getDeployPolicy(),
                                                                                   vdbEntry.getPath() ) );
        }

        if ( shouldSequence ) {
            // put vdb node as sibling of data service node
            final Node parent = dataServiceNode.getParent();
            final Node vdbNode = parent.addNode( vdbEntry.getEntryName(), VdbLexicon.Vdb.VIRTUAL_DATABASE );
            final boolean success = this.vdbSequencer.sequenceVdb( stream, vdbNode );

            if ( success ) {
                // reference sequenced node from the data source entry
                final Value ref = dataServiceNode.getSession().getValueFactory().createValue( vdbNode );
                vdbEntryNode.setProperty( DataVirtLexicon.DataSourceEntry.DATA_SOURCE_REF, ref );
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

}
