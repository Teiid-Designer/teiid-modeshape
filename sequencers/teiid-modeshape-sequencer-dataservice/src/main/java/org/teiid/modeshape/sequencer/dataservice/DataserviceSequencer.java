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
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import org.modeshape.common.annotation.ThreadSafe;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;
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
public class DataserviceSequencer extends Sequencer {

    private static final Logger LOGGER = Logger.getLogger( DataserviceSequencer.class );
    private static final String MANIFEST_FILE = "META-INF/dataservice.xml";

    private static final String DATA_SOURCES_PATH = "datasources"; // node name of where data sources are sequenced
    private static final String DRIVERS_PATH = "drivers"; // node name of where data source drivers are sequenced
    private static final String VDBS_PATH = "vdbs"; // node name of container where VDBs are sequenced

    static Node getDataSourcesNode( final Node dataserviceOutputNode ) throws Exception {
        final Node parent = dataserviceOutputNode.getParent().getParent();

        if ( parent.hasNode( DATA_SOURCES_PATH ) ) {
            return parent.getNode( DATA_SOURCES_PATH );
        }

        return parent.addNode( DATA_SOURCES_PATH );
    }

    static Node getDriversNode( final Node dataserviceOutputNode ) throws Exception {
        final Node parent = dataserviceOutputNode.getParent().getParent();

        if ( parent.hasNode( DRIVERS_PATH ) ) {
            return parent.getNode( DRIVERS_PATH );
        }

        return parent.addNode( DRIVERS_PATH );
    }

    static Node getVdbsNode( final Node dataserviceOutputNode ) throws Exception {
        final Node parent = dataserviceOutputNode.getParent().getParent();

        if ( parent.hasNode( VDBS_PATH ) ) {
            return parent.getNode( VDBS_PATH );
        }

        return parent.addNode( VDBS_PATH );
    }

    private VdbDynamicSequencer vdbSequencer; // constructed during initialize method
    private DatasourceSequencer datasourceSequencer; // constructed during initialize method

    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#execute(javax.jcr.Property, javax.jcr.Node,
     *      org.modeshape.jcr.api.sequencer.Sequencer.Context)
     */
    @SuppressWarnings( "null" )
    @Override
    public boolean execute( final Property inputProperty,
                            final Node outputNode,
                            final Context context ) throws Exception {
        LOGGER.debug( "DataserviceSequencer.execute called:outputNode name='{0}', path='{1}'",
                      outputNode.getName(),
                      outputNode.getPath() );

        final Binary binaryValue = inputProperty.getBinary();
        CheckArg.isNotNull( binaryValue, "binary" );

        DataserviceManifest manifest = null;
        final Map< Object, Node > nodes = new HashMap<>();

        boolean firstFile = true;
        boolean firstFileWasManifest = false;
        boolean foundManifest = false;

        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( ( entry = zis.getNextEntry() ) != null ) && ( firstFile || !foundManifest ) ) {
                String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    LOGGER.debug( "----ignoring directory '{0}'", entryName );
                    continue;
                } else if ( entryName.endsWith( MANIFEST_FILE ) ) {
                    foundManifest = true;
                    manifest = readManifest( binaryValue, zis, outputNode, context );
                } else if ( firstFileWasManifest ) {
                    if ( findDataSource( entryName, manifest ) != null ) {
                        sequenceDataSource( zis, findDataSource( entryName, manifest ), outputNode, nodes );
                    } else if ( isDriverPath( entryName, manifest ) ) {
                        sequenceDriver( zis, entryName, outputNode, nodes );
                    } else if ( findImportVdb( entryName, manifest ) != null ) {
                        sequenceImportVdb( zis, findImportVdb( entryName, manifest ), outputNode, nodes );
                    } else if ( isServiceVdbPath( entryName, manifest ) ) {
                        sequenceServiceVdb( zis, manifest.getServiceVdb(), outputNode, nodes );
                    }
                } else {
                    LOGGER.debug( "----skipping '{0}'. will sequence later", entryName );
                }

                if ( firstFile ) {
                    firstFile = false;

                    if ( foundManifest ) {
                        firstFileWasManifest = true;
                    }
                }
            }

            // if we didn't find manifest first we need to sequence other files now
            LOGGER.debug( "----firstFileWasManifest = " + firstFileWasManifest );
            if ( !firstFileWasManifest ) {
                sequenceVdbs( manifest, binaryValue, outputNode, nodes, inputProperty, context );
                sequenceDataSources( manifest, binaryValue, outputNode, nodes, inputProperty, context );
                sequenceDrivers( manifest, binaryValue, outputNode, nodes );
            }

            if ( LOGGER.isDebugEnabled() ) {
                { // dataservices
                    final NodeIterator itr = outputNode.getNodes();

                    while ( itr.hasNext() ) {
                        final Node node = itr.nextNode();
                        LOGGER.debug( node.getPath() + " : type = " + node.getPrimaryNodeType().getName() + ", num kids = "
                                      + node.getNodes().getSize() );
                    }
                }

                { // VDBs
                    final NodeIterator itr = getVdbsNode( outputNode ).getNodes();

                    while ( itr.hasNext() ) {
                        final Node node = itr.nextNode();
                        LOGGER.debug( node.getPath() + " : type = " + node.getPrimaryNodeType().getName() + ", num kids = "
                                      + node.getNodes().getSize() );
                    }
                }

                { // data sources
                    final NodeIterator itr = getDataSourcesNode( outputNode ).getNodes();

                    while ( itr.hasNext() ) {
                        final Node node = itr.nextNode();
                        LOGGER.debug( node.getPath() + " : type = " + node.getPrimaryNodeType().getName() + ", num kids = "
                                      + node.getNodes().getSize() );
                    }
                }

                { // drivers
                    final NodeIterator itr = getDriversNode( outputNode ).getNodes();

                    while ( itr.hasNext() ) {
                        final Node node = itr.nextNode();
                        LOGGER.debug( node.getPath() + " : type = " + node.getPrimaryNodeType().getName() + ", num kids = "
                                      + node.getNodes().getSize() );
                    }
                }
            }

            return true;
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.errorReadingDataserviceFile.text( inputProperty.getPath(), e.getMessage() ),
                                        e );
        }
    }

    private DataSourceDescriptor findDataSource( final String path,
                                                 final DataserviceManifest manifest ) {
        for ( final DataserviceImportVdb vdb : manifest.getImportVdbs() ) {
            if ( vdb.getDatasource().getPath().equals( path ) ) {
                return vdb.getDatasource();
            }
        }

        return null;
    }

    private DataserviceImportVdb findImportVdb( final String path,
                                                final DataserviceManifest manifest ) {
        for ( final DataserviceImportVdb vdb : manifest.getImportVdbs() ) {
            if ( vdb.getPath().equals( path ) ) {
                return vdb;
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

        this.vdbSequencer = new VdbDynamicSequencer();
        this.vdbSequencer.initialize( registry, nodeTypeManager );

        registerNodeTypes( "dv.cnd", nodeTypeManager, true );
        LOGGER.debug( "dv.cnd loaded" );

        this.datasourceSequencer = new DatasourceSequencer();
        this.datasourceSequencer.initialize( registry, nodeTypeManager );

        LOGGER.debug( "exit initialize" );
    }

    private boolean isDriverPath( final String path,
                                  final DataserviceManifest manifest ) {
        for ( final DataserviceImportVdb vdb : manifest.getImportVdbs() ) {
            final DataSourceDescriptor dsd = vdb.getDatasource();

            for ( final String driverPath : dsd.getDriverPaths() ) {
                if ( driverPath.equals( path ) ) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean isServiceVdbPath( final String path,
                                      final DataserviceManifest manifest ) {
        return manifest.getServiceVdb().getPath().equals( path );
    }

    private DataserviceManifest readManifest( final Binary binaryValue,
                                              final InputStream inputStream,
                                              final Node outputNode,
                                              final Context context ) throws Exception {
        LOGGER.debug( "----before reading manifest xml" );

        final DataserviceManifest manifest = DataserviceManifest.read( inputStream );
        outputNode.setPrimaryType( DataVirtLexicon.Dataservice.NODE_TYPE );

        LOGGER.debug( ">>>>done reading manifest xml\n\n" );
        return manifest;
    }

    private void sequenceDataSource( final InputStream stream,
                                     final DataSourceDescriptor dsd,
                                     final Node outputNode,
                                     final Map< Object, Node > nodes ) throws Exception {
        // add data sources in the data sources area
        final Node node = getDataSourcesNode( outputNode ).addNode( dsd.getPath(), DataVirtLexicon.Datasource.NODE_TYPE );
        nodes.put( dsd, node );

        final boolean sequenced = this.datasourceSequencer.sequenceDatasource( stream, node );

        if ( sequenced ) {
            final Value ref = outputNode.getSession().getValueFactory().createValue( node );
            final DataserviceImportVdb vdb = dsd.getParent();
            final Node vdbNode = nodes.get( vdb );
            vdbNode.setProperty( DataVirtLexicon.DataserviceVdb.DATA_SOURCE, ref );
        } else {
            node.remove();
            throw new Exception( TeiidI18n.dataSourceNotSequenced.text( dsd.getPath() ) );
        }
    }

    private void sequenceDataSources( final DataserviceManifest manifest,
                                      final Binary binaryValue,
                                      final Node outputNode,
                                      final Map< Object, Node > nodes,
                                      final Property inputProperty,
                                      final Context context ) throws Exception {
        LOGGER.debug( "----sequenceDataSources called: all data sources sequenced at once" );
        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    continue;
                }

                if ( findDataSource( entryName, manifest ) != null ) {
                    final DataSourceDescriptor dsd = findDataSource( entryName, manifest );
                    sequenceDataSource( zis, dsd, outputNode, nodes );
                }
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.dataSourceSequencingError.text(), e );
        }
    }

    private void sequenceDriver( final ZipInputStream zis,
                                 final String driverPath,
                                 final Node outputNode,
                                 final Map< Object, Node > nodes ) throws Exception {
        // extract driver file
        final byte[] buf = new byte[ 1024 ];
        final File file = File.createTempFile( driverPath, null );

        try ( final FileOutputStream fos = new FileOutputStream( file ) ) {
            int numRead = 0;

            while ( ( numRead = zis.read( buf ) ) > 0 ) {
                fos.write( buf, 0, numRead );
            }
        }

        // add under the drivers node
        final Node driverNode = getDriversNode( outputNode ).addNode( driverPath, DataVirtLexicon.DatasourceDriver.NODE_TYPE );
        final Node contentNode = driverNode.addNode( JcrConstants.JCR_CONTENT, JcrConstants.NT_RESOURCE );

        // set data property
        final InputStream is = new BufferedInputStream( new FileInputStream( file ) );
        final Binary binary = outputNode.getSession().getValueFactory().createBinary( is );
        contentNode.setProperty( JcrConstants.JCR_DATA, binary );

        // set last modified property
        final Calendar lastModified = Calendar.getInstance();
        lastModified.setTimeInMillis( file.lastModified() );
        contentNode.setProperty( "jcr:lastModified", lastModified );

        // add reference to driver node to its data source
        final Value ref = outputNode.getSession().getValueFactory().createValue( driverNode );
        boolean foundDs = false;

        // find data source
        for ( final Object obj : nodes.keySet() ) {
            if ( obj instanceof DataSourceDescriptor ) {
                final Collection< String > driverPaths = ( ( DataSourceDescriptor )obj ).getDriverPaths();

                if ( driverPaths.contains( driverPath ) ) {
                    foundDs = true;
                    final Node dsNode = nodes.get( obj );

                    if ( dsNode.hasProperty( DataVirtLexicon.Datasource.DRIVERS ) ) {
                        final Value[] currValue = dsNode.getProperty( DataVirtLexicon.Datasource.DRIVERS ).getValues();
                        final Value[] newValue = new Value[ currValue.length + 1 ];
                        System.arraycopy( currValue, 0, newValue, 0, currValue.length );
                        newValue[ currValue.length ] = ref;
                        dsNode.setProperty( DataVirtLexicon.Datasource.DRIVERS, newValue );
                    } else {
                        dsNode.setProperty( DataVirtLexicon.Datasource.DRIVERS, new Value[] { ref } );
                    }

                    break;
                }
            }
        }

        if ( !foundDs ) {
            throw new RuntimeException( TeiidI18n.driverDataSourceNotFound.text( driverPath ) );
        }
    }

    private void sequenceDrivers( final DataserviceManifest manifest,
                                  final Binary binaryValue,
                                  final Node outputNode,
                                  final Map< Object, Node > nodes ) {
        LOGGER.debug( "----sequenceDrivers called: all drivers sequenced at once" );
        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    continue;
                }

                if ( isDriverPath( entryName, manifest ) ) {
                    sequenceDriver( zis, entryName, outputNode, nodes );
                }
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.dataSourceDriverSequencingError.text(), e );
        }
    }

    private void sequenceImportVdb( final InputStream stream,
                                    final DataserviceImportVdb vdb,
                                    final Node outputNode,
                                    final Map< Object, Node > nodes ) throws Exception {
        final Node node = outputNode.addNode( vdb.getPath(), DataVirtLexicon.DataserviceVdb.NODE_TYPE );
        nodes.put( vdb, node );

        // add VDB to the VDBs area
        final Node vdbNode = getVdbsNode( outputNode ).addNode( vdb.getPath(), VdbLexicon.Vdb.VIRTUAL_DATABASE );
        final boolean sequenced = this.vdbSequencer.sequenceVdb( stream, vdbNode );

        if ( sequenced ) {
            // rename node to VDB name
            final String vdbName = vdbNode.getProperty( VdbLexicon.Vdb.NAME ).getString();
            outputNode.getSession().move( node.getPath(), ( outputNode.getPath() + '/' + vdbName ) );

            final Value ref = outputNode.getSession().getValueFactory().createValue( vdbNode );
            node.setProperty( DataVirtLexicon.DataserviceVdb.VDB, ref );

            // set parent property
            if ( outputNode.hasProperty( DataVirtLexicon.Dataservice.VDBS ) ) {
                final Value[] currValue = outputNode.getProperty( DataVirtLexicon.Dataservice.VDBS ).getValues();
                final Value[] newValue = new Value[ currValue.length + 1 ];
                System.arraycopy( currValue, 0, newValue, 0, currValue.length );
                newValue[ currValue.length ] = ref;
                outputNode.setProperty( DataVirtLexicon.Dataservice.VDBS, newValue );
            } else {
                outputNode.setProperty( DataVirtLexicon.Dataservice.VDBS, new Value[] { ref } );
            }
        } else {
            node.remove();
            vdbNode.remove();
            throw new Exception( TeiidI18n.importVdbNotSequenced.text( vdb.getPath() ) );
        }
    }

    private void sequenceServiceVdb( final InputStream stream,
                                     final DataserviceServiceVdb vdb,
                                     final Node outputNode,
                                     final Map< Object, Node > nodes ) throws Exception {
        // add VDB to the VDBs area
        final Node vdbNode = getVdbsNode( outputNode ).addNode( vdb.getPath(), VdbLexicon.Vdb.VIRTUAL_DATABASE );
        nodes.put( vdb, vdbNode );

        final boolean sequenced = this.vdbSequencer.sequenceVdb( stream, vdbNode );

        if ( sequenced ) {
            // rename node to VDB name
            final String vdbName = vdbNode.getProperty( VdbLexicon.Vdb.NAME ).getString();
            outputNode.getSession().move( vdbNode.getPath(), ( outputNode.getPath() + '/' + vdbName ) );

            final Value ref = outputNode.getSession().getValueFactory().createValue( vdbNode );
            outputNode.setProperty( DataVirtLexicon.Dataservice.SERVICE_VDB, ref );
        } else {
            vdbNode.remove();
            throw new Exception( TeiidI18n.serviceVdbNotSequenced.text( vdb.getPath() ) );
        }
    }

    private void sequenceVdbs( final DataserviceManifest manifest,
                               final Binary binaryValue,
                               final Node outputNode,
                               final Map< Object, Node > nodes,
                               final Property inputProperty,
                               final Context context ) throws Exception {
        LOGGER.debug( "----sequenceVdbs called: all VDBs sequenced at once" );
        final String serviceVdbEntryPath = manifest.getServiceVdb().getPath();

        try ( final ZipInputStream zis = new ZipInputStream( binaryValue.getStream() ) ) {
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                String entryName = entry.getName();

                if ( entry.isDirectory() ) {
                    continue;
                }

                if ( entryName.equals( serviceVdbEntryPath ) ) {
                    sequenceServiceVdb( zis, manifest.getServiceVdb(), outputNode, nodes );
                } else if ( findImportVdb( entryName, manifest ) != null ) {
                    sequenceImportVdb( zis, findImportVdb( entryName, manifest ), outputNode, nodes );
                }
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.dataserviceVdbSequencingError.text(), e );
        }
    }

}
