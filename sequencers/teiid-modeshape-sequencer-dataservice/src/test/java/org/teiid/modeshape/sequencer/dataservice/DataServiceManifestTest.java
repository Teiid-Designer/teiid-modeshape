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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;
import org.teiid.modeshape.sequencer.dataservice.DataServiceEntry.PublishPolicy;
import org.xml.sax.SAXParseException;

public final class DataServiceManifestTest {

    private Date createDate( final int year,
    		                     final int month,
    		                     final int day,
    		                     final int hour,
    		                     final int minute,
    		                     final int second ) {
        final Calendar calendar = Calendar.getInstance();
        calendar.set( Calendar.YEAR, year );
        calendar.set( Calendar.MONTH, month );
        calendar.set( Calendar.DAY_OF_MONTH, day );
        calendar.set( Calendar.HOUR_OF_DAY, hour );
        calendar.set( Calendar.MINUTE, minute );
        calendar.set( Calendar.SECOND, second );
        calendar.set( Calendar.MILLISECOND, 0 );
        
        return calendar.getTime();
    }

    private DataServiceEntry findEntry( final DataServiceEntry[] entries,
                                        final String path ) {
        for ( final DataServiceEntry entry : entries ) {
            if ( entry.getPath().equals( path ) ) {
                return entry;
            }
        }

        fail( "Entry with path " + path + " could not be found" );
        return null;
    }

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

        assertThat( manifest.getConnections().length, is( 1 ) );
        assertThat( manifest.getConnectionPaths().length, is( 1 ) );
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

        final ConnectionEntry ds = manifest.getConnections()[ 0 ];
        assertThat( ds.getPath(), is( "products-connection.xml" ) );
        assertThat( manifest.getConnectionPaths()[ 0 ], is( ds.getPath() ) );
        assertThat( ds.getPublishPolicy(), is( DataServiceEntry.PublishPolicy.IF_MISSING ) );
        assertThat( ds.getJndiName(), is( "productsConnection" ) );
    }

    @Test
    public void shouldReadDataServiceManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/dataserviceManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "MyDataService" ) );
        assertThat( manifest.getDescription(), is( "This is my data service description" ) );
        assertThat( manifest.getLastModified(), is( createDate( 2002, 4, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( "elvis" ) );

        assertThat( manifest.getProperties().size(), is( 1 ) );
        assertThat( manifest.getPropertyValue( "propA" ), is( "Value A" ) );

        { // connections
            assertThat( manifest.getConnections().length, is( 2 ) );
            assertThat( manifest.getConnectionPaths().length, is( 2 ) );
            assertThat( Arrays.asList( manifest.getConnectionPaths() ),
                        hasItems( "first-connection.xml", "second-connection.xml" ) );

            final ConnectionEntry first = ( ConnectionEntry )findEntry( manifest.getConnections(), "first-connection.xml" );
            assertThat( first.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );
            assertThat( first.getJndiName(), is( "firstConnection" ) );

            final ConnectionEntry second = ( ConnectionEntry )findEntry( manifest.getConnections(), "second-connection.xml" );
            assertThat( second.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );
            assertThat( second.getJndiName(), is( "secondConnection" ) );
        }

        { // drivers
            assertThat( manifest.getDrivers().length, is( 2 ) );
            assertThat( manifest.getDriverPaths().length, is( 2 ) );
            assertThat( Arrays.asList( manifest.getDriverPaths() ), hasItems( "firstDriver.jar", "secondDriver.jar" ) );

            final DataServiceEntry first = findEntry( manifest.getDrivers(), "firstDriver.jar" );
            assertThat( first.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );

            final DataServiceEntry second = findEntry( manifest.getDrivers(), "secondDriver.jar" );
            assertThat( second.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );
        }

        { // metadata
            assertThat( manifest.getMetadata().length, is( 2 ) );
            assertThat( manifest.getMetadataPaths().length, is( 2 ) );
            assertThat( Arrays.asList( manifest.getMetadataPaths() ), hasItems( "firstDdl.ddl", "secondDdl.ddl" ) );

            final DataServiceEntry first = findEntry( manifest.getMetadata(), "firstDdl.ddl" );
            assertThat( first.getPublishPolicy(), is( PublishPolicy.ALWAYS ) );

            final DataServiceEntry second = findEntry( manifest.getMetadata(), "secondDdl.ddl" );
            assertThat( second.getPublishPolicy(), is( PublishPolicy.ALWAYS ) );
        }

        { // resources
            assertThat( manifest.getResources().length, is( 2 ) );
            assertThat( manifest.getResourcePaths().length, is( 2 ) );
            assertThat( Arrays.asList( manifest.getResourcePaths() ), hasItems( "firstResource.xml", "secondResource.xml" ) );

            final DataServiceEntry first = findEntry( manifest.getResources(), "firstResource.xml" );
            assertThat( first.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );

            final DataServiceEntry second = findEntry( manifest.getResources(), "secondResource.xml" );
            assertThat( second.getPublishPolicy(), is( PublishPolicy.ALWAYS ) );
        }

        { // udfs
            assertThat( manifest.getUdfs().length, is( 2 ) );
            assertThat( manifest.getUdfPaths().length, is( 2 ) );
            assertThat( Arrays.asList( manifest.getUdfPaths() ), hasItems( "firstUdf.jar", "secondUdf.jar" ) );

            final DataServiceEntry first = findEntry( manifest.getUdfs(), "firstUdf.jar" );
            assertThat( first.getPublishPolicy(), is( PublishPolicy.NEVER ) );

            final DataServiceEntry second = findEntry( manifest.getUdfs(), "secondUdf.jar" );
            assertThat( second.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );
        }

        { // vdbs
            assertThat( manifest.getVdbs().length, is( 2 ) );
            assertThat( manifest.getVdbPaths().length, is( 2 ) );
            assertThat( Arrays.asList( manifest.getVdbPaths() ), hasItems( "first-vdb.xml", "second-vdb.xml" ) );

            final VdbEntry first = ( VdbEntry )findEntry( manifest.getVdbs(), "first-vdb.xml" );
            assertThat( first.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );
            assertThat( first.getVdbName(), is( "FirstVdb" ) );
            assertThat( first.getVdbVersion(), is( "1" ) );

            final VdbEntry second = ( VdbEntry )findEntry( manifest.getVdbs(), "second-vdb.xml" );
            assertThat( second.getPublishPolicy(), is( PublishPolicy.IF_MISSING ) );
            assertThat( second.getVdbName(), is( "SecondVdb" ) );
            assertThat( second.getVdbVersion(), is( "1" ) );
        }
    }

    @Test
    public void shouldReadDriversOnlyManifest() throws Exception {
        final DataServiceManifest manifest = DataServiceManifest.read( streamFor( "/dataservice/driversOnlyManifest.xml" ) );
        assertThat( manifest, is( notNullValue() ) );
        assertThat( manifest.getName(), is( "DriverEntriesOnly" ) );
        assertThat( manifest.getDescription(), is( "This is my driver entries only description" ) );
        assertThat( manifest.getLastModified(), is( createDate( 2002, 4, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( nullValue() ) );

        assertThat( manifest.getProperties().size(), is( 1 ) );
        assertThat( manifest.getPropertyValue( "foo" ), is( "bar" ) );

        assertThat( manifest.getConnections().length, is( 0 ) );
        assertThat( manifest.getConnectionPaths().length, is( 0 ) );
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

        assertThat( manifest.getConnections().length, is( 0 ) );
        assertThat( manifest.getConnectionPaths().length, is( 0 ) );
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
        assertThat( manifest.getLastModified(), is( createDate( 2002, 4, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( "elvis" ) );

        assertThat( manifest.getProperties().size(), is( 0 ) );

        assertThat( manifest.getConnections().length, is( 0 ) );
        assertThat( manifest.getConnectionPaths().length, is( 0 ) );
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
        assertThat( manifest.getLastModified(), is( createDate( 2002, 4, 30, 9, 30, 10 ) ) );
        assertThat( manifest.getModifiedBy(), is( nullValue() ) );

        assertThat( manifest.getProperties().size(), is( 3 ) );
        assertThat( manifest.getPropertyValue( "propA" ), is( "Value A" ) );
        assertThat( manifest.getPropertyValue( "propB" ), is( "Value B" ) );
        assertThat( manifest.getPropertyValue( "propC" ), is( "Value C" ) );

        assertThat( manifest.getConnections().length, is( 0 ) );
        assertThat( manifest.getConnectionPaths().length, is( 0 ) );
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
        
        assertThat( manifest.getLastModified(), is( createDate( 2016, 9, 30, 10, 5, 33 ) ) );
        assertThat( manifest.getModifiedBy(), is( "sledge" ) );

        assertThat( manifest.getProperties().size(), is( 0 ) );

        assertThat( manifest.getConnections().length, is( 0 ) );
        assertThat( manifest.getConnectionPaths().length, is( 0 ) );
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
        final URL resourceUrl = getClass().getResource( resourcePath );
        final InputStream istream = resourceUrl.openStream();
        assertThat( istream, is( notNullValue() ) );
        return istream;
    }

    @Test
    public void shouldHaveExpectedInitialState() throws Exception {
        final DataServiceManifest manifest = new DataServiceManifest();
        assertThat( manifest.getDescription(), is( nullValue() ) );
        assertThat( manifest.getLastModified(), is( nullValue() ) );
        assertThat( manifest.getModifiedBy(), is( nullValue() ) );
        assertThat( manifest.getServiceVdb(), is( nullValue() ) );
        assertThat( manifest.getServiceVdbPath(), is( nullValue() ) );
        assertThat( manifest.getProperties().size(), is( 0 ) );
        assertThat( manifest.getConnectionPaths().length, is( 0 ) );
        assertThat( manifest.getConnections().length, is( 0 ) );
        assertThat( manifest.getDriverPaths().length, is( 0 ) );
        assertThat( manifest.getDrivers().length, is( 0 ) );
        assertThat( manifest.getMetadataPaths().length, is( 0 ) );
        assertThat( manifest.getMetadata().length, is( 0 ) );
        assertThat( manifest.getResourcePaths().length, is( 0 ) );
        assertThat( manifest.getResources().length, is( 0 ) );
        assertThat( manifest.getUdfPaths().length, is( 0 ) );
        assertThat( manifest.getUdfs().length, is( 0 ) );
        assertThat( manifest.getVdbPaths().length, is( 0 ) );
        assertThat( manifest.getVdbs().length, is( 0 ) );
    }

}
