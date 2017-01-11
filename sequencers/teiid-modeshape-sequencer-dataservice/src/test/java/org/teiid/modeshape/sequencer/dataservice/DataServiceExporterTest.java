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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Value;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.teiid.modeshape.sequencer.AbstractSequencerTest;
import org.teiid.modeshape.sequencer.Options;
import org.teiid.modeshape.sequencer.Result;
import org.teiid.modeshape.sequencer.dataservice.DataServiceExporter.ExportArtifact;
import org.teiid.modeshape.sequencer.dataservice.DataServiceExporter.OptionName;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.vdb.VdbManifest;
import org.teiid.modeshape.sequencer.vdb.VdbModel;

public final class DataServiceExporterTest extends AbstractSequencerTest {

	@Rule
    public TemporaryFolder dir = new TemporaryFolder();

    private VdbModel assertModel( final List< VdbModel > models,
                                  final String modelName ) {
        for ( final VdbModel model : models ) {
            if ( modelName.equals( model.getName() ) ) {
                return model;
            }
        }

        fail( "Model "
              + modelName
              + " not found" );
        return null;
    }

    @Override
    protected InputStream getRepositoryConfigStream() {
        return resourceStream( "config/repo-config.json" );
    }

    @Test
    public void shouldCorrectVdbAndConnectionEntryNames() throws Exception {
        final Node dsNode = this.rootNode.addNode( "MyDataService", DataVirtLexicon.DataService.NODE_TYPE );
        final String serviceVdbEntryName = "MyServiceVdb";
        final String connectionEntryPath = "ResourceConnection";

        { // add service VDB entry
          // import and sequence VDB
            createNodeWithContentFromFile( "patients-vdb.xml", "vdbs/patients-vdb.xml" );
            final Node serviceVdbNode = getOutputNode( this.rootNode, "vdbs/patients-vdb.xml" );
            final String refId = serviceVdbNode.getIdentifier();

            // don't set path and the entry name will be used for the path
            final Node entryNode = dsNode.addNode( serviceVdbEntryName, DataVirtLexicon.ServiceVdbEntry.NODE_TYPE );
            entryNode.setProperty( DataVirtLexicon.ServiceVdbEntry.VDB_REF, refId );
            entryNode.setProperty( DataVirtLexicon.ServiceVdbEntry.VDB_NAME, "patients" );
            entryNode.setProperty( DataVirtLexicon.ServiceVdbEntry.VDB_VERSION, "1" );
        }

        { // add connection entry
          // import and sequence connection
            createNodeWithContentFromFile( "resource-connection.xml", "connections/resource-connection.xml" );
            final Node connectionNode = getOutputNode( this.rootNode, "connections/resourceConnection" );
            final String refId = connectionNode.getIdentifier();

            final Node connectionEntryNode = dsNode.addNode( "ResourceConnectionEntry",
                    DataVirtLexicon.ConnectionEntry.NODE_TYPE );
            // set path but it doesn't have the right suffix
            connectionEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.PATH, connectionEntryPath );
            connectionEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.CONNECTION_REF, refId );
            connectionEntryNode.setProperty( DataVirtLexicon.ConnectionEntry.JDNI_NAME, "java:/jndiName" );
        }

        { // add VDB entry
          // import and sequence VDB
            createNodeWithContentFromFile( "product-view-vdb.xml", "vdbs/product-view-vdb.xml" );
            final Node vdbNode = getOutputNode( this.rootNode, "vdbs/product-view-vdb.xml" );
            final String refId = vdbNode.getIdentifier();

            final Node entryNode = dsNode.addNode( "ProductViewVdbEntry", DataVirtLexicon.VdbEntry.NODE_TYPE );
            // set path to right extension but not the right complete suffix
            entryNode.setProperty( DataVirtLexicon.VdbEntry.PATH, "product-view.xml" );
            entryNode.setProperty( DataVirtLexicon.VdbEntry.VDB_REF, refId );
            entryNode.setProperty( DataVirtLexicon.VdbEntry.VDB_NAME, "DynamicProducts" );
            entryNode.setProperty( DataVirtLexicon.VdbEntry.VDB_VERSION, "2" );
        }

        // export
        final DataServiceExporter exporter = new DataServiceExporter();
        final Options options = new Options();
        options.set( OptionName.EXPORT_ARTIFACT, ExportArtifact.MANIFEST_AS_XML );
        final Result result = exporter.execute( dsNode, options );
        assertThat( result.wasSuccessful(), is( true ) );

        // read back in as a manifest
        final String xml = ( String ) result.getOutcome();
        final DataServiceManifestReader reader = new DataServiceManifestReader();
        final DataServiceManifest manifest = reader.read( new ByteArrayInputStream( xml.getBytes() ) );
        assertThat( manifest, is( notNullValue() ) );

        // test entry paths
        assertThat( manifest.getServiceVdb(), is( notNullValue() ) );
        assertThat( manifest.getServiceVdb().getEntryName(),
                is( serviceVdbEntryName + DataServiceManifest.VDB_ENTRY_SUFFIX ) );
        assertThat( manifest.getConnections().length, is( 1 ) );
        assertThat( manifest.getConnections()[ 0 ].getEntryName(),
                is( connectionEntryPath + DataServiceManifest.CONNECTION_ENTRY_SUFFIX ) );
        assertThat( manifest.getVdbs().length, is( 1 ) );
        assertThat( manifest.getVdbs()[ 0 ].getEntryName(), is( "product-view-vdb.xml" ) );
    }

    @Test
    public void shouldExportDataServiceAsFiles() throws Exception {
        createNodeWithContentFromFile( "MyDataService.zip", "dataservice/sample-ds.zip" );
        final Node dataServiceNode = getOutputNode( this.rootNode, "dataservices/MyDataService.zip", 120 );
        assertThat( dataServiceNode, is( notNullValue() ) );

        final DataServiceExporter exporter = new DataServiceExporter();
        final Options options = new Options();
        options.set( OptionName.EXPORT_ARTIFACT, ExportArtifact.DATA_SERVICE_AS_FILES );
        final Result result = exporter.execute( dataServiceNode, options );
        assertThat( result, is( notNullValue() ) );
        assertThat( result.getError(), is( nullValue() ) );
        assertThat( result.getErrorMessage(), is( nullValue() ) );
        assertThat( result.getOutcome(), is( instanceOf( result.getType() ) ) );

        final List< String > paths = new ArrayList< String >( Arrays.asList( new String[] { 
                "connections/books-connection.xml",
                "connections/portfolio-connection.xml",
                "drivers/books-driver-1.jar",
                "drivers/books-driver-2.jar",
                "drivers/portfolio-driver.jar",
                "META-INF/dataservice.xml",
                "metadata/firstDdl.ddl",
                "metadata/secondDdl.ddl",
                "resources/firstResource.xml",
                "resources/secondResource.xml",
                "udfs/secondUdf.jar",
                "vdbs/books-vdb.xml",
                "vdbs/Portfolio-vdb.xml",
                "vdbs/twitter-vdb.xml",
                "product-view-vdb.xml" } ) );

        final byte[][] exportContents = ( byte[][] )result.getOutcome();
        final String[] entryPaths = ( String[] )result.getData( DataServiceExporter.RESULT_ENTRY_PATHS );
        final int numFiles = paths.size();
        assertThat( exportContents.length, is( numFiles ) );
        assertThat( entryPaths.length, is( numFiles ) );

        // make sure files can be created
        for ( int i = 0; i < numFiles; ++i ) {
            final byte[] content = exportContents[ i ];
            assertThat( content, is( notNullValue() ) );
            assertThat( content.length, is( not( 0 ) ) );

            final String[] segments = entryPaths[ i ].split( "/" );

            for ( int j = 0; j < ( segments.length - 1 ); ++j ) {
                if ( this.dir.getRoot().listFiles( new Filter( segments[ j ] ) ).length == 0 ) {
                    final File folder = this.dir.newFolder( segments[ j ] );
                    folder.mkdir();
                }
            }
            
            final File file = new File( this.dir.getRoot().getAbsolutePath(), entryPaths[ i ] );
            
            // write out contents
            final OutputStream os = new FileOutputStream( file );
            os.write(content);
            os.close();
        }

        // make sure all files were created
        for ( final String fileName : this.dir.getRoot().list() ) {
            final File file = new File( this.dir.getRoot().getAbsolutePath(), fileName );

            if ( file.isDirectory() ) {
                for ( final String name : file.list() ) {
                    removePath( paths, ( fileName + '/' + name ) );
                }
            } else {
               paths.remove( fileName );
            }
        }

        assertThat( paths.toString(), paths.isEmpty(), is( true ) );
    }

    private void removePath( final List< String > paths,
                             final String fileName ) {
        paths.remove( fileName );
    }

    class Filter implements FilenameFilter {
        final String segment;

        Filter( final String segment ) {
            this.segment = segment;
        }

    @Override
    	public boolean accept( final File dir, final String name ) {
        if ( !name.equals( segment ) ) {
            return false;
        }

        return new File( dir.getAbsolutePath(), name ).exists();
    	}
};

    @Test
    public void shouldExportDataServiceAsZip() throws Exception {
        createNodeWithContentFromFile( "MyDataService.zip", "dataservice/sample-ds.zip" );
        final Node dataServiceNode = getOutputNode( this.rootNode, "dataservices/MyDataService.zip" );
        assertThat( dataServiceNode, is( notNullValue() ) );

        final DataServiceExporter exporter = new DataServiceExporter();
        final Result result = exporter.execute( dataServiceNode, null );
        assertThat( result, is( notNullValue() ) );
        assertThat( result.getError(), is( nullValue() ) );
        assertThat( result.getErrorMessage(), is( nullValue() ) );
        assertThat( result.getOutcome(), is( instanceOf( result.getType() ) ) );

        final byte[] zipBytes = ( byte[] )result.getOutcome();
        final List< String > paths = new ArrayList< String >( Arrays.asList( new String[] { "connections/books-connection.xml",
                                                                                    "connections/portfolio-connection.xml",
                                                                                    "drivers/books-driver-1.jar",
                                                                                    "drivers/books-driver-2.jar",
                                                                                    "drivers/portfolio-driver.jar",
                                                                                    "META-INF/dataservice.xml",
                                                                                    "metadata/firstDdl.ddl",
                                                                                    "metadata/secondDdl.ddl",
                                                                                    "resources/firstResource.xml",
                                                                                    "resources/secondResource.xml",
                                                                                    "udfs/secondUdf.jar",
                                                                                    "vdbs/books-vdb.xml",
                                                                                    "vdbs/Portfolio-vdb.xml",
                                                                                    "vdbs/twitter-vdb.xml",
                                                                                    "product-view-vdb.xml" } ) );

        ZipInputStream zis = null;

        try {
            zis = new ZipInputStream( new ByteArrayInputStream( zipBytes ) );
            ZipEntry entry = null;

            while ( ( entry = zis.getNextEntry() ) != null ) {
                assertThat( "Entry "
                            + entry.getName()
                            + " not found to remove",
                            paths.remove( entry.getName() ),
                            is( true ) );
            }

            assertThat( "Paths not found: "
                        + paths.toString(),
                        paths.isEmpty(),
                        is( true ) );
        } finally {
            if ( zis != null ) {
                zis.close();
            }
        }
    }

    @Test
    public void shouldExportDataServiceInSpecifiedFolders() throws Exception {
        createNodeWithContentFromFile( "MyDataService.zip", "dataservice/sample-ds.zip" );
        final Node dataServiceNode = getOutputNode( this.rootNode, "dataservices/MyDataService.zip" );
        assertThat( dataServiceNode, is( notNullValue() ) );

        { // need to unset the entry path property since it was set during sequencing
            final NodeIterator itr = dataServiceNode.getNodes();

            while (itr.hasNext()) {
                final Node entry = itr.nextNode();
                entry.setProperty( DataVirtLexicon.DataServiceEntry.PATH, ( Value ) null );

                // clear dependency VDB paths
                if ( DataVirtLexicon.ServiceVdbEntry.NODE_TYPE.equals( entry.getPrimaryNodeType().getName() ) ) {
                    final NodeIterator depItr = entry.getNodes();

                    while (depItr.hasNext()) {
                        final Node dependency = depItr.nextNode();
                        dependency.setProperty( DataVirtLexicon.DataServiceEntry.PATH, ( Value ) null );
                    }
                }
            }
        }

        final String connectionsFolder = "TEST-connections/";
        final String driversFolder = "TEST-drivers/";
        final String metadataFolder = "TEST-metadata/";
        final String resourcesFolder = "TEST-resources/";
        final String udfsFolder = "TEST-udfs/";
        final String vdbsFolder = "TEST-vdbs/";

        final Options options = new Options();
        options.set( OptionName.EXPORT_ARTIFACT, ExportArtifact.MANIFEST_AS_XML );
        options.set( OptionName.CONNECTIONS_FOLDER, connectionsFolder );
        options.set( OptionName.DRIVERS_EXPORT_FOLDER, driversFolder );
        options.set( OptionName.METADATA_FOLDER, metadataFolder );
        options.set( OptionName.RESOURCES_FOLDER, resourcesFolder );
        options.set( OptionName.UDFS_FOLDER, udfsFolder );
        options.set( OptionName.VDBS_FOLDER, vdbsFolder );

        final DataServiceExporter exporter = new DataServiceExporter();
        final Result result = exporter.execute( dataServiceNode, options );
        assertThat( result, is( notNullValue() ) );
        assertThat( result.getError(), is( nullValue() ) );
        assertThat( result.getErrorMessage(), is( nullValue() ) );
        assertThat( result.getOutcome(), is( instanceOf( result.getType() ) ) );

        final String xml = ( String ) result.getOutcome();
        final DataServiceManifestReader reader = new DataServiceManifestReader();
        final DataServiceManifest manifest = reader.read( new ByteArrayInputStream( xml.getBytes() ) );
        assertThat( manifest, is( notNullValue() ) );

        { // service VDB
            final ServiceVdbEntry serviceVdbEntry = manifest.getServiceVdb();
            // service VDBs are not put inside the VDBs folder
            assertThat( serviceVdbEntry.getPath(), is( "product-view-vdb.xml" ) );
        }

        { // connections
            final String[] paths = manifest.getConnectionPaths();
            assertThat( Arrays.asList( paths ), hasItems( connectionsFolder + "books-connection.xml",
                    connectionsFolder + "portfolio-connection.xml" ) );
        }

        { // drivers
            final String[] paths = manifest.getDriverPaths();
            assertThat( Arrays.asList( paths ), hasItems( driversFolder + "books-driver-1.jar",
                    driversFolder + "books-driver-2.jar", driversFolder + "portfolio-driver.jar" ) );
        }

        { // metadata
            final String[] paths = manifest.getMetadataPaths();
            assertThat( Arrays.asList( paths ),
                    hasItems( metadataFolder + "firstDdl.ddl", metadataFolder + "secondDdl.ddl" ) );
        }

        { // resources
            final String[] paths = manifest.getResourcePaths();
            assertThat( Arrays.asList( paths ),
                    hasItems( resourcesFolder + "firstResource.xml", resourcesFolder + "secondResource.xml" ) );
        }

        { // UDFs
            final String[] paths = manifest.getUdfPaths();
            assertThat( Arrays.asList( paths ), hasItem( udfsFolder + "secondUdf.jar" ) );
        }

        { // VDBs
            final String[] paths = manifest.getVdbPaths();
            assertThat( Arrays.asList( paths ),
                    hasItems( vdbsFolder + "books-vdb.xml", vdbsFolder + "Portfolio-vdb.xml" ) );

            final String[] dependencyPaths = manifest.getServiceVdb().getVdbPaths();
            assertThat( Arrays.asList( dependencyPaths ), hasItem( vdbsFolder + "twitter-vdb.xml" ) );
        }
    }

    @Test
    public void shouldExportDataServiceManifestAsXml() throws Exception {
        createNodeWithContentFromFile( "MyDataService.zip", "dataservice/sample-ds.zip" );
        final Node dataServiceNode = getOutputNode( this.rootNode, "dataservices/MyDataService.zip" );
        assertThat( dataServiceNode, is( notNullValue() ) );

        final DataServiceExporter exporter = new DataServiceExporter();
        final Options options = new Options();
        options.set( OptionName.EXPORT_ARTIFACT, ExportArtifact.MANIFEST_AS_XML );
        final Result result = exporter.execute( dataServiceNode, options );
        assertThat( result, is( notNullValue() ) );
        assertThat( result.getError(), is( nullValue() ) );
        assertThat( result.getErrorMessage(), is( nullValue() ) );
        assertThat( result.wasSuccessful(), is( true ) );
        assertThat( result, is( notNullValue() ) );
        assertThat( result.getOutcome(), is( instanceOf( result.getType() ) ) );

        // round trip
        final String xml = ( String )result.getOutcome();
        final DataServiceManifestReader reader = new DataServiceManifestReader();
        final DataServiceManifest manifest = reader.read( new ByteArrayInputStream( xml.getBytes() ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "MyDataService" ) );
        assertThat( manifest.getDescription(), is( "This is my data service description" ) );
        assertThat( manifest.getConnections().length, is( 2 ) );
        assertThat( manifest.getDrivers().length, is( 3 ) );
        assertThat( manifest.getMetadata().length, is( 2 ) );
        assertThat( manifest.getResources().length, is( 2 ) );
        assertThat( manifest.getUdfs().length, is( 1 ) ); // one UDF is never published
        assertThat( manifest.getVdbs().length, is( 2 ) );
        assertThat( manifest.getServiceVdb(), is( notNullValue() ) );
        assertThat( manifest.getServiceVdb().getEntryName(), is( "product-view-vdb.xml" ) );
        assertThat( manifest.getServiceVdb().getPath(), is( "product-view-vdb.xml" ) );
        assertThat( manifest.getServiceVdb().getVdbName(), is( "ServiceVdb" ) );
        assertThat( manifest.getServiceVdb().getVdbVersion(), is( "1" ) );
        assertThat( manifest.getServiceVdb().getVdbs().length, is( 1 ) );

        final VdbEntry dependency = manifest.getServiceVdb().getVdbs()[ 0 ];
        assertThat( dependency.getPath(), is( "vdbs/twitter-vdb.xml" ) );
        assertThat( dependency.getVdbName(), is( "twitter" ) );
        assertThat( dependency.getVdbVersion(), is( "1" ) );
    }

    @Test
    public void shouldExportDataServiceVdbAsXml() throws Exception {
        createNodeWithContentFromFile( "serviceVdbOnly.zip", "dataservice/serviceVdbOnly-ds.zip" );
        final Node dataServiceNode = getOutputNode( this.rootNode, "dataservices/serviceVdbOnly.zip" );
        assertThat( dataServiceNode, is( notNullValue() ) );

        final DataServiceExporter exporter = new DataServiceExporter();
        final Options options = new Options();
        options.set( OptionName.EXPORT_ARTIFACT, ExportArtifact.SERVICE_VDB_AS_XML );

        final Result result = exporter.execute( dataServiceNode, options );
        assertThat( result, is( notNullValue() ) );
        assertThat( result.getError(), is( nullValue() ) );
        assertThat( result.getErrorMessage(), is( nullValue() ) );
        assertThat( result.wasSuccessful(), is( true ) );
        assertThat( result.getOutcome(), is( instanceOf( result.getType() ) ) );

        // round trip
        final String xml = ( String )result.getOutcome();
        final VdbManifest manifest = VdbManifest.read( new ByteArrayInputStream( xml.getBytes() ), null );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "DynamicProducts" ) );
        assertThat( manifest.getVersion(), is( 1 ) );
        assertThat( manifest.getDescription(), is( "Product Dynamic VDB" ) );
        assertThat( manifest.getProperties().size(), is( 1 ) );
        assertThat( manifest.getProperties().get( "UseConnectorMetadata" ), is( "true" ) );
        assertThat( manifest.getModels().size(), is( 3 ) );
        assertModel( manifest.getModels(), "ProductsMySQL_Dynamic" );
        assertModel( manifest.getModels(), "ProductViews" );
        assertModel( manifest.getModels(), "ProductSummary" );
    }

}
