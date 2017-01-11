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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import java.io.InputStream;
import javax.jcr.Node;
import org.junit.Test;
import org.teiid.modeshape.sequencer.AbstractSequencerTest;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;

public final class ConnectionSequencerTest extends AbstractSequencerTest {

    @Override
    protected InputStream getRepositoryConfigStream() {
        return resourceStream( "config/repo-config.json" );
    }

    @Test
    public void shouldHaveValidCnds() throws Exception {
        registerNodeTypes( "org/teiid/modeshape/sequencer/dataservice/dv.cnd" );
    }

    @Test
    public void shouldSequenceJdbcConnection() throws Exception {
        createNodeWithContentFromFile( "jdbc-connection.xml", "connections/jdbc-connection.xml" );
        final Node outputNode = getOutputNode( this.rootNode, "connections/jdbcConnection" );
        assertNotNull( outputNode );
        assertThat( outputNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.Connection.NODE_TYPE ) );

        // check properties
        assertThat( outputNode.getProperty( DataVirtLexicon.Connection.TYPE ).getString(), is( "JDBC" ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.Connection.DRIVER_NAME ).getString(), is( "dsDriver" ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.Connection.JNDI_NAME ).getString(), is( "java:/jdbcSource" ) );
        assertThat( outputNode.getProperty( "prop1" ).getString(), is( "one" ) );
        assertThat( outputNode.getProperty( "prop2" ).getString(), is( "two" ) );
        assertThat( outputNode.getProperty( "prop3" ).getString(), is( "three" ) );
        assertThat( outputNode.getProperty( "prop4" ).getString(), is( "four" ) );
    }

    @Test
    public void shouldSequenceResourceConnection() throws Exception {
        createNodeWithContentFromFile( "resource-connection.xml", "connections/resource-connection.xml" );
        final Node outputNode = getOutputNode( this.rootNode, "connections/resourceConnection" );
        assertNotNull( outputNode );
        assertThat( outputNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.Connection.NODE_TYPE ) );

        // check properties
        assertThat( outputNode.getProperty( DataVirtLexicon.Connection.TYPE ).getString(), is( "RESOURCE" ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.Connection.CLASS_NAME ).getString(), is( "dsClassname" ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.Connection.DRIVER_NAME ).getString(), is( "dsDriver" ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.Connection.JNDI_NAME ).getString(), is( "java:/jndiName" ) );
        assertThat( outputNode.getProperty( "prop2" ).getString(), is( "prop2Value" ) );
        assertThat( outputNode.getProperty( "prop1" ).getString(), is( "prop1Value" ) );
    }

}
