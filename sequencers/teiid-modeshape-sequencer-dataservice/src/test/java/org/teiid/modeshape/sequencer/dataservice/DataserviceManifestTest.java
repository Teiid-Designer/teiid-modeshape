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
import static org.junit.Assert.assertThat;

import java.io.InputStream;

import org.junit.Test;

/**
 * 
 */
public class DataserviceManifestTest {

    @Test
    public void shouldReadDataserviceManifest() throws Exception {
        DataserviceManifest manifest = DataserviceManifest.read(streamFor("/dataservice/dataserviceManifest.xml"), null/*, context*/);
        assertThat(manifest.getServiceVdbName(), is("myService-vdb.xml"));
        assertThat(manifest.getVdbNames(), is("Portfolio-vdb.xml, twitter-vdb.xml"));
        assertThat(manifest.getDatasourceNames(), is("datasource.xml"));
        assertThat(manifest.getDriverNames(), is("mysql-connector-java-5.1.39-bin.jar"));
    }

    private InputStream streamFor( String resourcePath ) throws Exception {
        InputStream istream = getClass().getResourceAsStream(resourcePath);
        assertThat(istream, is(notNullValue()));
        return istream;
    }
}
