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
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import javax.jcr.Node;
import org.junit.Test;
import org.teiid.modeshape.sequencer.AbstractSequencerTest;

public final class ConnectionExporterTest extends AbstractSequencerTest {

    @Override
    protected InputStream getRepositoryConfigStream() {
        return resourceStream( "config/repo-config.json" );
    }

    @Test
    public void shouldExportJdbcConnection() throws Exception {
        createNodeWithContentFromFile( "jdbc-connection.xml", "connections/jdbc-connection.xml" );
        final Node connectionNode = getOutputNode( this.rootNode, "connections/jdbcConnection" );
        assertThat( connectionNode, is( notNullValue() ) );

        final ConnectionExporter exporter = new ConnectionExporter();
        final Object temp = exporter.execute( connectionNode, null );
        assertThat( temp, is( notNullValue() ) );
        assertThat( temp, is( instanceOf( String.class ) ) );

        // round trip
        final String xml = ( String )temp;
        final ConnectionReader reader = new ConnectionReader();
        final Connection connection = reader.read( new ByteArrayInputStream( xml.getBytes() ) );
        assertThat( connection, is( notNullValue() ) );
        assertThat( connection.getName(), is( "jdbcConnection" ) );
        assertThat( connection.getJndiName(), is( "java:/jdbcSource" ) );
        assertThat( connection.getDriverName(), is( "dsDriver" ) );
        assertThat( connection.getClassName(), is( nullValue() ) );
        assertThat( connection.getProperties().size(), is( 4 ) );
        assertThat( connection.getPropertyValue( "prop1" ), is( "one" ) );
        assertThat( connection.getPropertyValue( "prop2" ), is( "two" ) );
        assertThat( connection.getPropertyValue( "prop3" ), is( "three" ) );
        assertThat( connection.getPropertyValue( "prop4" ), is( "four" ) );
    }

    @Test
    public void shouldExportResourceConnection() throws Exception {
        createNodeWithContentFromFile( "resource-connection.xml", "connections/resource-connection.xml" );
        final Node connectionNode = getOutputNode( this.rootNode, "connections/resourceConnection" );
        assertThat( connectionNode, is( notNullValue() ) );

        final ConnectionExporter exporter = new ConnectionExporter();
        final Object temp = exporter.execute( connectionNode, null );
        assertThat( temp, is( notNullValue() ) );
        assertThat( temp, is( instanceOf( String.class ) ) );

        // round trip
        final String xml = ( String )temp;
        final ConnectionReader reader = new ConnectionReader();
        final Connection connection = reader.read( new ByteArrayInputStream( xml.getBytes() ) );
        assertThat( connection, is( notNullValue() ) );
        assertThat( connection.getName(), is( "resourceConnection" ) );
        assertThat( connection.getJndiName(), is( "java:/jndiName" ) );
        assertThat( connection.getDriverName(), is( "dsDriver" ) );
        assertThat( connection.getClassName(), is( "dsClassname" ) );
        assertThat( connection.getProperties().size(), is( 2 ) );
        assertThat( connection.getPropertyValue( "prop1" ), is( "prop1Value" ) );
        assertThat( connection.getPropertyValue( "prop2" ), is( "prop2Value" ) );
    }

}
