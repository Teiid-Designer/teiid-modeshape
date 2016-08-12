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
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import java.io.InputStream;
import java.time.LocalDateTime;
import org.junit.Test;
import org.xml.sax.SAXParseException;

public final class DataServiceManifestTest {

    @Test( expected = SAXParseException.class )
    public void shouldFailReadingManifestWithDuplicateConnectionJndiNames() throws Exception {
        DataServiceManifest.read( streamFor( "/dataservice/duplicateJndiNamesManifest.xml" ) );
    }

    @Test( expected = SAXParseException.class )
    public void shouldFailReadingManifestWithDuplicateEntryPaths() throws Exception {
        DataServiceManifest.read( streamFor( "/dataservice/duplicatePathsManifest.xml" ) );
    }

    @Test( expected = SAXParseException.class )
    public void shouldFailReadingManifestWithDuplicateVdbs() throws Exception {
        DataServiceManifest.read( streamFor( "/dataservice/duplicateVdbsManifest.xml" ) );
    }

    @Test
    public void shouldReadConnectionsOnlyManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/dataSourcesOnlyManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "DataSourceEntriesOnly" ) );
        assertThat( manifest.getDescription(), is( nullValue() ) );
        assertThat( manifest.getLastModified(), is( nullValue() ) );
        assertThat( manifest.getModifiedBy(), is( nullValue() ) );

        assertThat( manifest.getProperties().size(), is( 0 ) );

        assertThat( manifest.getDataSources().length, is( 1 ) );
        assertThat( manifest.getDataSourcePaths().length, is( 1 ) );
        assertThat( manifest.getDrivers().length, is( 0 ) );
        assertThat( manifest.getDriverPaths().length, is( 0 ) );
        assertThat( manifest.getMetadata().length, is( 0 ) );
        assertThat( manifest.getMetadataPaths().length, is( 0 ) );
        assertThat( manifest.getResources().length, is( 0 ) );
        assertThat( manifest.getResourcePaths().length, is( 0 ) );
        assertThat( manifest.getUdfs().length, is( 0 ) );
        assertThat( manifest.getUdfPaths().length, is( 0 ) );
        assertThat( manifest.getVdbs().length, is( 0 ) );
        assertThat( manifest.getVdbPaths().length, is( 0 ) );

        final ConnectionEntry ds = manifest.getDataSources()[ 0 ];
        assertThat( ds.getPath(), is( "products-connection.xml" ) );
        assertThat( manifest.getDataSourcePaths()[ 0 ], is( ds.getPath() ) );
        assertThat( ds.getPublishPolicy(), is( DataServiceEntry.PublishPolicy.IF_MISSING ) );
        assertThat( ds.getJndiName(), is( "productsConnection" ) );
    }

    @Test
    public void shouldReadDataServiceManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/dataserviceManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "MyDataService" ) );
        assertThat( manifest.getDescription(), is( "This is my data service description" ) );
        assertThat( manifest.getLastModified(), is( LocalDateTime.of( 2002, 5, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( "elvis" ) );

        assertThat( manifest.getProperties().size(), is( 1 ) );
        assertThat( manifest.getPropertyValue( "propA" ), is( "Value A" ) );

        assertThat( manifest.getDataSources().length, is( 2 ) );
        assertThat( manifest.getDataSourcePaths().length, is( 2 ) );
        assertThat( manifest.getDrivers().length, is( 2 ) );
        assertThat( manifest.getDriverPaths().length, is( 2 ) );
        assertThat( manifest.getMetadata().length, is( 2 ) );
        assertThat( manifest.getMetadataPaths().length, is( 2 ) );
        assertThat( manifest.getResources().length, is( 2 ) );
        assertThat( manifest.getResourcePaths().length, is( 2 ) );
        assertThat( manifest.getUdfs().length, is( 2 ) );
        assertThat( manifest.getUdfPaths().length, is( 2 ) );
        assertThat( manifest.getVdbs().length, is( 2 ) );
        assertThat( manifest.getVdbPaths().length, is( 2 ) );
    }

    @Test
    public void shouldReadDriversOnlyManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/driversOnlyManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "DriverEntriesOnly" ) );
        assertThat( manifest.getDescription(), is( "This is my driver entries only description" ) );
        assertThat( manifest.getLastModified(), is( LocalDateTime.of( 2002, 5, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( nullValue() ) );

        assertThat( manifest.getProperties().size(), is( 1 ) );
        assertThat( manifest.getPropertyValue( "foo" ), is( "bar" ) );

        assertThat( manifest.getDataSources().length, is( 0 ) );
        assertThat( manifest.getDataSourcePaths().length, is( 0 ) );
        assertThat( manifest.getDrivers().length, is( 1 ) );
        assertThat( manifest.getDriverPaths().length, is( 1 ) );
        assertThat( manifest.getMetadata().length, is( 0 ) );
        assertThat( manifest.getMetadataPaths().length, is( 0 ) );
        assertThat( manifest.getResources().length, is( 0 ) );
        assertThat( manifest.getResourcePaths().length, is( 0 ) );
        assertThat( manifest.getUdfs().length, is( 0 ) );
        assertThat( manifest.getUdfPaths().length, is( 0 ) );
        assertThat( manifest.getVdbs().length, is( 0 ) );
        assertThat( manifest.getVdbPaths().length, is( 0 ) );
    }

    @Test
    public void shouldReadMetadataOnlyManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/metadataOnlyManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "MetadataEntriesOnly" ) );
        assertThat( manifest.getDescription(), is( "This is my metadata entries only description" ) );
        assertThat( manifest.getLastModified(), is( nullValue() ) );
        assertThat( manifest.getModifiedBy(), is( nullValue() ) );

        assertThat( manifest.getProperties().size(), is( 0 ) );

        assertThat( manifest.getDataSources().length, is( 0 ) );
        assertThat( manifest.getDataSourcePaths().length, is( 0 ) );
        assertThat( manifest.getDrivers().length, is( 0 ) );
        assertThat( manifest.getDriverPaths().length, is( 0 ) );
        assertThat( manifest.getMetadata().length, is( 1 ) );
        assertThat( manifest.getMetadataPaths().length, is( 1 ) );
        assertThat( manifest.getResources().length, is( 0 ) );
        assertThat( manifest.getResourcePaths().length, is( 0 ) );
        assertThat( manifest.getUdfs().length, is( 0 ) );
        assertThat( manifest.getUdfPaths().length, is( 0 ) );
        assertThat( manifest.getVdbs().length, is( 0 ) );
        assertThat( manifest.getVdbPaths().length, is( 0 ) );
    }

    @Test
    public void shouldReadResourcesOnlyManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/resourcesOnlyManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "ResourceEntriesOnly" ) );
        assertThat( manifest.getDescription(), is( "This is my resources entries only description" ) );
        assertThat( manifest.getLastModified(), is( LocalDateTime.of( 2002, 5, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( "elvis" ) );

        assertThat( manifest.getProperties().size(), is( 0 ) );

        assertThat( manifest.getDataSources().length, is( 0 ) );
        assertThat( manifest.getDataSourcePaths().length, is( 0 ) );
        assertThat( manifest.getDrivers().length, is( 0 ) );
        assertThat( manifest.getDriverPaths().length, is( 0 ) );
        assertThat( manifest.getMetadata().length, is( 0 ) );
        assertThat( manifest.getMetadataPaths().length, is( 0 ) );
        assertThat( manifest.getResources().length, is( 1 ) );
        assertThat( manifest.getResourcePaths().length, is( 1 ) );
        assertThat( manifest.getUdfs().length, is( 0 ) );
        assertThat( manifest.getUdfPaths().length, is( 0 ) );
        assertThat( manifest.getVdbs().length, is( 0 ) );
        assertThat( manifest.getVdbPaths().length, is( 0 ) );
    }

    @Test
    public void shouldReadUdfsOnlyManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/udfsOnlyManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "UdfEntriesOnly" ) );
        assertThat( manifest.getDescription(), is( nullValue() ) );
        assertThat( manifest.getLastModified(), is( LocalDateTime.of( 2002, 5, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( nullValue() ) );

        assertThat( manifest.getProperties().size(), is( 3 ) );
        assertThat( manifest.getPropertyValue( "propA" ), is( "Value A" ) );
        assertThat( manifest.getPropertyValue( "propB" ), is( "Value B" ) );
        assertThat( manifest.getPropertyValue( "propC" ), is( "Value C" ) );

        assertThat( manifest.getDataSources().length, is( 0 ) );
        assertThat( manifest.getDataSourcePaths().length, is( 0 ) );
        assertThat( manifest.getDrivers().length, is( 0 ) );
        assertThat( manifest.getDriverPaths().length, is( 0 ) );
        assertThat( manifest.getMetadata().length, is( 0 ) );
        assertThat( manifest.getMetadataPaths().length, is( 0 ) );
        assertThat( manifest.getResources().length, is( 0 ) );
        assertThat( manifest.getResourcePaths().length, is( 0 ) );
        assertThat( manifest.getUdfs().length, is( 1 ) );
        assertThat( manifest.getUdfPaths().length, is( 1 ) );
        assertThat( manifest.getVdbs().length, is( 0 ) );
        assertThat( manifest.getVdbPaths().length, is( 0 ) );
    }

    @Test
    public void shouldReadVdbsOnlyManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/vdbsOnlyManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "VdbEntriesOnly" ) );
        assertThat( manifest.getDescription(), is( "This is my VDB entries only description" ) );
        assertThat( manifest.getLastModified(), is( LocalDateTime.of( 2016, 10, 30, 10, 5, 33 ) ) );
        assertThat( manifest.getModifiedBy(), is( "sledge" ) );

        assertThat( manifest.getProperties().size(), is( 0 ) );

        assertThat( manifest.getDataSources().length, is( 0 ) );
        assertThat( manifest.getDataSourcePaths().length, is( 0 ) );
        assertThat( manifest.getDrivers().length, is( 0 ) );
        assertThat( manifest.getDriverPaths().length, is( 0 ) );
        assertThat( manifest.getMetadata().length, is( 0 ) );
        assertThat( manifest.getMetadataPaths().length, is( 0 ) );
        assertThat( manifest.getResources().length, is( 0 ) );
        assertThat( manifest.getResourcePaths().length, is( 0 ) );
        assertThat( manifest.getUdfs().length, is( 0 ) );
        assertThat( manifest.getUdfPaths().length, is( 0 ) );
        assertThat( manifest.getVdbs().length, is( 3 ) );
        assertThat( manifest.getVdbPaths().length, is( 3 ) );
    }

    private InputStream streamFor( final String resourcePath ) throws Exception {
        final InputStream istream = getClass().getResourceAsStream( resourcePath );
        assertThat( istream, is( notNullValue() ) );
        return istream;
    }
    //
    // @Test( expected = Exception.class )
    // public void shouldNotAllowSettingServiceVdbMoreThanOnce() throws Exception {
    // final DataServiceManifest manifest = new DataServiceManifest();
    // manifest.setServiceVdb( new DataserviceServiceVdb( manifest ) );
    // manifest.setServiceVdb( new DataserviceServiceVdb( manifest ) );
    // }
    //
    // @Test
    // public void shouldAddImportVdb() throws Exception {
    // final DataServiceManifest manifest = new DataServiceManifest();
    // manifest.addImportVdb( new DataserviceImportVdb( manifest ) );
    // assertThat( manifest.getImportVdbs().size(), is( 1 ) );
    // }
    //
    // @Test
    // public void shouldHaveExpectedInitialState() throws Exception {
    // final DataServiceManifest manifest = new DataServiceManifest();
    // assertThat( manifest.getServiceVdb(), is( nullValue() ) );
    // assertThat( manifest.getImportVdbs().isEmpty(), is( true ) );
    // }

}
