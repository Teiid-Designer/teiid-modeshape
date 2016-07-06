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
import java.io.InputStream;
import org.junit.Test;

public final class DataserviceManifestTest {
    
    @Test( expected = Exception.class )
    public void shouldFailIfImportVdbMissingDataSource() throws Exception {
        DataserviceManifest.read( streamFor( "/dataservice/manifestMissingDataSource.xml" ) );
    }
    
    @Test( expected = Exception.class )
    public void shouldFailIfImportVdbMissingDriver() throws Exception {
        DataserviceManifest.read( streamFor( "/dataservice/manifestMissingDriver.xml" ) );
    }
    
    @Test( expected = Exception.class )
    public void shouldFailIfMissingServiceVdb() throws Exception {
        DataserviceManifest.read( streamFor( "/dataservice/manifestMissingServiceVdb.xml" ) );
    }

    @Test
    public void shouldReadDataserviceManifest() throws Exception {
        final int importVdbSize = 2;
        final DataserviceManifest manifest = DataserviceManifest.read( streamFor( "/dataservice/dataserviceManifest.xml" ) );
        assertThat( manifest.getServiceVdb(), is( notNullValue() ) );
        assertThat( manifest.getServiceVdb().getPath(), is( "myService-vdb.xml" ) );
        assertThat( manifest.getImportVdbs().size(), is( importVdbSize ) );

        final DataserviceImportVdb[] vdbs = manifest.getImportVdbs().toArray( new DataserviceImportVdb[ importVdbSize ] );

        // portfolio
        final DataserviceImportVdb portfolioVdb = vdbs[ 0 ].getPath().equals( "Portfolio-vdb.xml" ) ? vdbs[ 0 ] : vdbs[ 1 ];

        assertThat( portfolioVdb.getDatasource(), is( notNullValue() ) );
        assertThat( portfolioVdb.getDatasource().getPath(), is( "datasource.xml" ) );
        assertThat( portfolioVdb.getDatasource().getDriverPaths().size(), is( 1 ) );
        assertThat( portfolioVdb.getDatasource().getDriverPaths().iterator().next(),
                    is( "mysql-connector-java-5.1.39-bin.jar" ) );

        // books
        final DataserviceImportVdb booksVdb = vdbs[ 1 ].getPath().equals( "books-vdb.xml" ) ? vdbs[ 1 ] : vdbs[ 0 ];

        assertThat( booksVdb.getDatasource(), is( notNullValue() ) );
        assertThat( booksVdb.getDatasource().getPath(), is( "books-ds.xml" ) );
        assertThat( booksVdb.getDatasource().getDriverPaths().size(), is( 2 ) );
        assertThat( booksVdb.getDatasource().getDriverPaths(), hasItems( "mydb-driver-1.jar", "mydb-driver-2.jar" ) );
    }

    private InputStream streamFor( String resourcePath ) throws Exception {
        InputStream istream = getClass().getResourceAsStream( resourcePath );
        assertThat( istream, is( notNullValue() ) );
        return istream;
    }

    @Test( expected = Exception.class )
    public void shouldNotAllowSettingServiceVdbMoreThanOnce() throws Exception {
        final DataserviceManifest manifest = new DataserviceManifest();
        manifest.setServiceVdb( new DataserviceServiceVdb( manifest ) );
        manifest.setServiceVdb( new DataserviceServiceVdb( manifest ) );
    }

    @Test
    public void shouldAddImportVdb() throws Exception {
        final DataserviceManifest manifest = new DataserviceManifest();
        manifest.addImportVdb( new DataserviceImportVdb( manifest ) );
        assertThat( manifest.getImportVdbs().size(), is( 1 ) );
    }

    @Test
    public void shouldHaveExpectedInitialState() throws Exception {
        final DataserviceManifest manifest = new DataserviceManifest();
        assertThat( manifest.getServiceVdb(), is( nullValue() ) );
        assertThat( manifest.getImportVdbs().isEmpty(), is( true ) );
    }

}
