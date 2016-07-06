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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Session;
import javax.jcr.Value;
import org.junit.Test;
import org.teiid.modeshape.sequencer.AbstractSequencerTest;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.vdb.lexicon.VdbLexicon;

public final class DataserviceSequencerTest extends AbstractSequencerTest {

    private void assertDataSource( final Node parent,
                                   final String dataSourceNodeName,
                                   final String type,
                                   final String jndiName,
                                   final String className,
                                   final String[] driverNames,
                                   final Map< String, String > props ) throws Exception {
        assertThat( DataserviceSequencer.getDataSourcesNode( parent ).hasNode( dataSourceNodeName ), is( true ) );
        final Node dsNode = DataserviceSequencer.getDataSourcesNode( parent ).getNode( dataSourceNodeName );

        assertThat( dsNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.Datasource.NODE_TYPE ) );
        assertThat( dsNode.getProperty( DataVirtLexicon.Datasource.TYPE ).getString(), is( type ) );
        assertThat( dsNode.getProperty( DataVirtLexicon.Datasource.JNDI_NAME ).getString(), is( jndiName ) );
        assertThat( dsNode.getProperty( DataVirtLexicon.Datasource.CLASS_NAME ).getString(), is( className ) );

        // drivers
        final Collection< String > names = Arrays.asList( driverNames );
        final Node[] driverRefs = getReferencedNodes( dsNode, DataVirtLexicon.Datasource.DRIVERS );
        assertThat( driverRefs.length, is( driverNames.length ) );

        for ( final Node ref : driverRefs ) {
            assertThat( names.contains( ref.getName() ), is( true ) );
        }

        // properties
        if ( ( props != null ) && !props.isEmpty() ) {
            for ( final Map.Entry< String, String > entry : props.entrySet() ) {
                assertThat( dsNode.hasProperty( entry.getKey() ), is( true ) );
                assertThat( dsNode.getProperty( entry.getKey() ).getString(), is( entry.getValue() ) );
            }
        }
    }

    private void assertDriver( final Node parent,
                               final String driverNodeName ) throws Exception {
        assertThat( DataserviceSequencer.getDriversNode( parent ).hasNode( driverNodeName ), is( true ) );
        final Node driverNode = DataserviceSequencer.getDriversNode( parent ).getNode( driverNodeName );

        assertThat( driverNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.DatasourceDriver.NODE_TYPE ) );
    }

    private void assertImportVdb( final Node parent,
                                  final String dvVdbNodeName,
                                  final String vdbNodeName,
                                  final String dataSourceNodeName ) throws Exception {
        assertThat( parent.hasNode( dvVdbNodeName ), is( true ) );

        final Node vdbNode = parent.getNode( dvVdbNodeName );
        assertThat( vdbNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.DataserviceVdb.NODE_TYPE ) );
        assertThat( vdbNode.hasProperty( DataVirtLexicon.DataserviceVdb.VDB ), is( true ) );
        assertThat( vdbNode.hasProperty( DataVirtLexicon.DataserviceVdb.DATA_SOURCE ), is( true ) );

        { // VDB
            final Node vdbRef = getReferencedNode( vdbNode, DataVirtLexicon.DataserviceVdb.VDB );
            assertThat( vdbRef, is( notNullValue() ) );
            assertThat( vdbRef.getPrimaryNodeType().getName(), is( VdbLexicon.Vdb.VIRTUAL_DATABASE ) );
            assertThat( vdbRef.getName(), is( vdbNodeName ) );
            assertThat( DataserviceSequencer.getVdbsNode( parent ).hasNode( vdbNodeName ), is( true ) );
        }

        { // Data Source
            final Node dsRef = getReferencedNode( vdbNode, DataVirtLexicon.DataserviceVdb.DATA_SOURCE );
            assertThat( dsRef, is( notNullValue() ) );
            assertThat( dsRef.getPrimaryNodeType().getName(), is( DataVirtLexicon.Datasource.NODE_TYPE ) );
            assertThat( dsRef.getName(), is( dataSourceNodeName ) );
        }
    }

    private Node getReferencedNode( final Node node,
                                    final String nameOfPropertyWithRefValue ) throws Exception {
        final Session session = node.getSession();
        final Property prop = node.getProperty( nameOfPropertyWithRefValue );
        return session.getNodeByIdentifier( prop.getValue().getString() );
    }

    private Node[] getReferencedNodes( final Node node,
                                       final String nameOfPropertyWithRefValue ) throws Exception {
        final Session session = node.getSession();
        final Property prop = node.getProperty( nameOfPropertyWithRefValue );
        final Value[] values = prop.getValues();
        final Node[] refNodes = new Node[ values.length ];
        int i = 0;

        for ( final Value value : values ) {
            refNodes[ i++ ] = session.getNodeByIdentifier( value.getString() );
        }

        return refNodes;
    }

    @Override
    protected InputStream getRepositoryConfigStream() {
        return resourceStream( "config/repo-config.json" );
    }

    @Test
    public void shouldHaveValidCnds() throws Exception {
        registerNodeTypes( "org/teiid/modeshape/sequencer/dataservice/dv.cnd" );
    }

    @SuppressWarnings( "serial" )
    @Test
    public void shouldSequenceDataservice() throws Exception {
        createNodeWithContentFromFile( "MyDataservice.zip", "dataservice/sample-ds.zip" );
        final Node outputNode = getOutputNode( this.rootNode, "dataservices/MyDataservice.zip", 300 );
        assertNotNull( outputNode );
        assertThat( outputNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.Dataservice.NODE_TYPE ) );

        // properties
        assertThat( outputNode.hasProperty( DataVirtLexicon.Dataservice.SERVICE_VDB ), is( true ) );
        assertThat( outputNode.hasProperty( DataVirtLexicon.Dataservice.VDBS ), is( true ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.Dataservice.VDBS ).getValues().length, is( 2 ) );

        // output nodes
        // /dataservices/MyDataservice.zip/BooksExample : type = dv:dataServiceVdb, num kids = 0
        // /dataservices/MyDataservice.zip/Portfolio : type = dv:dataServiceVdb, num kids = 0
        // /dataservices/MyDataservice.zip/DynamicProducts : type = vdb:virtualDatabase, num kids = 3
        // /vdbs/books-vdb.xml : type = vdb:virtualDatabase, num kids = 4
        // /vdbs/Portfolio-vdb.xml : type = vdb:virtualDatabase, num kids = 5
        // /datasources/booksDatasource.tds : type = dv:dataSource, num kids = 0
        // /datasources/portfolioDatasource.tds : type = dv:dataSource, num kids = 0
        // /drivers/books-driver-1.jar : type = dv:driver, num kids = 1
        // /drivers/books-driver-2.jar : type = dv:driver, num kids = 1
        // /drivers/portfolio-driver.jar : type = dv:driver, num kids = 1
        assertThat( outputNode.getNodes().getSize(), is( 3L ) );
        assertThat( DataserviceSequencer.getVdbsNode( outputNode ).getNodes().getSize(), is( 2L ) );
        assertThat( DataserviceSequencer.getDataSourcesNode( outputNode ).getNodes().getSize(), is( 2L ) );
        assertThat( DataserviceSequencer.getDriversNode( outputNode ).getNodes().getSize(), is( 3L ) );

        { // service VDB
            assertThat( outputNode.hasNode( "DynamicProducts" ), is( true ) );
            final Node serviceVdb = outputNode.getNode( "DynamicProducts" );

            assertThat( serviceVdb.getPrimaryNodeType().getName(), is( VdbLexicon.Vdb.VIRTUAL_DATABASE ) );
            assertThat( outputNode.hasProperty( DataVirtLexicon.Dataservice.SERVICE_VDB ), is( true ) );

            final Node ref = outputNode.getSession().getNodeByIdentifier( outputNode.getProperty( DataVirtLexicon.Dataservice.SERVICE_VDB ).getValue().getString() );
            assertThat( serviceVdb.getPath(), is( ref.getPath() ) );
        }

        // import VDBs
        assertImportVdb( outputNode, "Portfolio", "Portfolio-vdb.xml", "portfolioDatasource.tds" );
        assertImportVdb( outputNode, "BooksExample", "books-vdb.xml", "booksDatasource.tds" );

        // data sources
        assertDataSource( outputNode,
                          "portfolioDatasource.tds",
                          "RESOURCE",
                          "java:/jndiName",
                          "portfolioDriverClassname",
                          new String[] { "portfolio-driver.jar" },
                          new HashMap< String, String >() {
                              {
                                  put( "prop1", "prop1Value" );
                                  put( "prop2", "prop2Value" );
                              }
                          } );

        assertDataSource( outputNode,
                          "booksDatasource.tds",
                          "JDBC",
                          "java:/jdbcSource",
                          "booksDriverClassname",
                          new String[] { "books-driver-1.jar", "books-driver-2.jar" },
                          new HashMap< String, String >() {
                              {
                                  put( "prop1", "one" );
                                  put( "prop2", "two" );
                                  put( "prop3", "three" );
                                  put( "prop4", "four" );
                              }
                          } );

        // drivers
        assertDriver( outputNode, "portfolio-driver.jar" );
        assertDriver( outputNode, "books-driver-1.jar" );
        assertDriver( outputNode, "books-driver-2.jar" );
    }

}
