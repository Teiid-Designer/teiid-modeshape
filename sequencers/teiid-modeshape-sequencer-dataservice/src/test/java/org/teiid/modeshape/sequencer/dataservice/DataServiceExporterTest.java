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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.jcr.Node;
import org.junit.Test;
import org.teiid.modeshape.sequencer.AbstractSequencerTest;
import org.teiid.modeshape.sequencer.Options;
import org.teiid.modeshape.sequencer.Result;
import org.teiid.modeshape.sequencer.dataservice.DataServiceExporter.ExportArtifact;
import org.teiid.modeshape.sequencer.dataservice.DataServiceExporter.OptionName;
import org.teiid.modeshape.sequencer.vdb.VdbManifest;
import org.teiid.modeshape.sequencer.vdb.VdbModel;

public final class DataServiceExporterTest extends AbstractSequencerTest {

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

        final byte[][] exportContents = ( byte[][] )result.getOutcome();
        final String[] entryPaths = ( String[] )result.getData( DataServiceExporter.RESULT_ENTRY_PATHS );
        final int numFiles = 15;
        assertThat( exportContents.length, is( numFiles ) );
        assertThat( entryPaths.length, is( numFiles ) );

        final Path dir = Files.createTempDirectory( "ds-" );
        final List< String > paths = new ArrayList<>( Arrays.asList( new String[] { "connections/books-connection.xml",
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
                                                                                    ( dir.getFileName().toString()
                                                                                      + '/'
                                                                                      + "product-view-vdb.xml" ) } ) );

        // make sure files can be created
        for ( int i = 0; i < numFiles; ++i ) {
            final byte[] content = exportContents[ i ];
            assertThat( content, is( notNullValue() ) );
            assertThat( content.length, is( not( 0 ) ) );

            final Path file = dir.resolve( entryPaths[ i ] );
            Files.createDirectories( file.getParent() );

            final Path path = Files.createFile( file );
            Files.write( path, content );
        }

        // make sure all files were created
        Files.walk( dir ).filter( Files::isRegularFile ).forEach( path -> paths.remove( path.getParent().getFileName().toString()
                                                                                        + '/'
                                                                                        + path.getFileName().toString() ) );
        assertThat( paths.toString(), paths.isEmpty(), is( true ) );
    }

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
        final List< String > paths = new ArrayList<>( Arrays.asList( new String[] { "connections/books-connection.xml",
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

        try ( final ZipInputStream zis = new ZipInputStream( new ByteArrayInputStream( zipBytes ) ) ) {
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
