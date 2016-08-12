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
import java.util.Calendar;
import java.util.GregorianCalendar;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.Session;
import org.junit.Test;
import org.modeshape.jcr.api.JcrConstants;
import org.teiid.modeshape.sequencer.AbstractSequencerTest;
import org.teiid.modeshape.sequencer.dataservice.DataServiceEntry.PublishPolicy;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.vdb.lexicon.VdbLexicon;

public final class DataServiceSequencerTest extends AbstractSequencerTest {

    private void assertConnection( final Node dataServiceNode,
                                   final String dsEntryName,
                                   final DataServiceEntry.PublishPolicy deployPolicy ) throws Exception {
        assertThat( dataServiceNode.hasNode( dsEntryName ), is( true ) );

        final Node dsEntryNode = dataServiceNode.getNode( dsEntryName );
        assertThat( dsEntryNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.ConnectionEntry.NODE_TYPE ) );
        assertThat( dsEntryNode.hasProperty( DataVirtLexicon.ConnectionEntry.PATH ), is( true ) );
        assertThat( dsEntryNode.getProperty( DataVirtLexicon.ConnectionEntry.PATH ).getString(),
                    is( "connections/" + dsEntryName ) );

        // check reference
        assertReferencedResource( dsEntryNode,
                                  DataVirtLexicon.ConnectionEntry.CONNECTION_REF,
                                  DataVirtLexicon.Connection.NODE_TYPE,
                                  deployPolicy,
                                  false );
    }

    private void assertDdl( final Node dataServiceNode,
                            final String ddlEntryName,
                            final DataServiceEntry.PublishPolicy deployPolicy ) throws Exception {
        assertThat( dataServiceNode.hasNode( ddlEntryName ), is( true ) );

        final Node ddlEntryNode = dataServiceNode.getNode( ddlEntryName );
        assertThat( ddlEntryNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.MetadataEntry.DDL_FILE_NODE_TYPE ) );
        assertThat( ddlEntryNode.hasProperty( DataVirtLexicon.MetadataEntry.PATH ), is( true ) );
        assertThat( ddlEntryNode.getProperty( DataVirtLexicon.MetadataEntry.PATH ).getString(),
                    is( "metadata/" + ddlEntryName ) );

        // check reference
        assertReferencedResource( ddlEntryNode,
                                  DataVirtLexicon.MetadataEntry.METADATA_REF,
                                  DataVirtLexicon.MetadaFile.DDL_FILE_NODE_TYPE,
                                  deployPolicy,
                                  true );
    }

    private void assertDriver( final Node dataServiceNode,
                               final String driverEntryName,
                               final DataServiceEntry.PublishPolicy deployPolicy ) throws Exception {
        assertThat( dataServiceNode.hasNode( driverEntryName ), is( true ) );

        final Node driverEntryNode = dataServiceNode.getNode( driverEntryName );
        assertThat( driverEntryNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.DriverEntry.NODE_TYPE ) );
        assertThat( driverEntryNode.hasProperty( DataVirtLexicon.DriverEntry.PATH ), is( true ) );
        assertThat( driverEntryNode.getProperty( DataVirtLexicon.DriverEntry.PATH ).getString(),
                    is( "drivers/" + driverEntryName ) );

        // check reference
        assertReferencedResource( driverEntryNode,
                                  DataVirtLexicon.DriverEntry.DRIVER_REF,
                                  DataVirtLexicon.DriverFile.NODE_TYPE,
                                  deployPolicy,
                                  true );
    }

    private void assertReferencedResource( final Node node,
                                           final String nameOfPropertyWithRefValue,
                                           final String typeOfResource,
                                           final DataServiceEntry.PublishPolicy deployPolicy,
                                           final boolean checkForFileContent ) throws Exception {
        // make sure property exists
        boolean refExists = ( ( deployPolicy == PublishPolicy.ALWAYS ) || ( deployPolicy == PublishPolicy.IF_MISSING ) );
        assertThat( node.hasProperty( nameOfPropertyWithRefValue ), is( refExists ) );

        if ( refExists ) {
            // find referenced node
            final Session session = node.getSession();
            final Property prop = node.getProperty( nameOfPropertyWithRefValue );
            final Node ref = session.getNodeByIdentifier( prop.getValue().getString() );
            assertThat( ref.getPrimaryNodeType().getName(), is( typeOfResource ) );

            if ( checkForFileContent ) {
                assertThat( ref.hasNode( JcrConstants.JCR_CONTENT ), is( true ) );
                assertThat( ref.getNode( JcrConstants.JCR_CONTENT ).hasProperty( JcrConstants.JCR_DATA ), is( true ) );
            }
        }
    }

    private void assertResource( final Node dataServiceNode,
                                 final String resourceEntryName,
                                 final DataServiceEntry.PublishPolicy deployPolicy ) throws Exception {
        assertThat( dataServiceNode.hasNode( resourceEntryName ), is( true ) );

        final Node resourceEntryNode = dataServiceNode.getNode( resourceEntryName );
        assertThat( resourceEntryNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.ResourceEntry.NODE_TYPE ) );
        assertThat( resourceEntryNode.hasProperty( DataVirtLexicon.ResourceEntry.PATH ), is( true ) );
        assertThat( resourceEntryNode.getProperty( DataVirtLexicon.ResourceEntry.PATH ).getString(),
                    is( "resources/" + resourceEntryName ) );

        // check reference
        assertReferencedResource( resourceEntryNode,
                                  DataVirtLexicon.ResourceEntry.RESOURCE_REF,
                                  DataVirtLexicon.ResourceFile.NODE_TYPE,
                                  deployPolicy,
                                  true );
    }

    private void assertUdf( final Node dataServiceNode,
                            final String udfEntryName,
                            final DataServiceEntry.PublishPolicy deployPolicy ) throws Exception {
        assertThat( dataServiceNode.hasNode( udfEntryName ), is( true ) );

        final Node udfEntryNode = dataServiceNode.getNode( udfEntryName );
        assertThat( udfEntryNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.ResourceEntry.UDF_FILE_NODE_TYPE ) );
        assertThat( udfEntryNode.hasProperty( DataVirtLexicon.ResourceEntry.PATH ), is( true ) );
        assertThat( udfEntryNode.getProperty( DataVirtLexicon.ResourceEntry.PATH ).getString(), is( "udfs/" + udfEntryName ) );

        // check reference
        assertReferencedResource( udfEntryNode,
                                  DataVirtLexicon.ResourceEntry.RESOURCE_REF,
                                  DataVirtLexicon.ResourceFile.UDF_FILE_NODE_TYPE,
                                  deployPolicy,
                                  true );
    }

    private void assertVdb( final Node dataServiceNode,
                            final String vdbEntryName,
                            final DataServiceEntry.PublishPolicy deployPolicy ) throws Exception {
        assertThat( dataServiceNode.hasNode( vdbEntryName ), is( true ) );

        final Node vdbEntryNode = dataServiceNode.getNode( vdbEntryName );
        assertThat( vdbEntryNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.VdbEntry.NODE_TYPE ) );
        assertThat( vdbEntryNode.hasProperty( DataVirtLexicon.VdbEntry.PATH ), is( true ) );
        assertThat( vdbEntryNode.getProperty( DataVirtLexicon.VdbEntry.PATH ).getString(), is( "vdbs/" + vdbEntryName ) );

        // check reference
        assertReferencedResource( vdbEntryNode,
                                  DataVirtLexicon.VdbEntry.VDB_REF,
                                  VdbLexicon.Vdb.VIRTUAL_DATABASE,
                                  deployPolicy,
                                  false );
    }

    @Override
    protected InputStream getRepositoryConfigStream() {
        return resourceStream( "config/repo-config.json" );
    }

    @Test
    public void shouldHaveValidCnds() throws Exception {
        registerNodeTypes( "org/teiid/modeshape/sequencer/dataservice/dv.cnd" );
    }

    @Test
    public void shouldSequenceDataService() throws Exception {
        createNodeWithContentFromFile( "MyDataService.zip", "dataservice/sample-ds.zip" );
        final Node outputNode = getOutputNode( this.rootNode, "dataservices/MyDataService.zip" );
        assertNotNull( outputNode );
        assertThat( outputNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.DataService.NODE_TYPE ) );

        // properties
        assertThat( outputNode.hasProperty( DataVirtLexicon.DataService.NAME ), is( true ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.DataService.NAME ).getString(), is( "MyDataService" ) );
        assertThat( outputNode.hasProperty( DataVirtLexicon.DataService.DESCRIPTION ), is( true ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.DataService.DESCRIPTION ).getString(),
                    is( "This is my data service description" ) );
        assertThat( outputNode.hasProperty( DataVirtLexicon.DataService.MODIFIED_BY ), is( true ) );
        assertThat( outputNode.getProperty( DataVirtLexicon.DataService.MODIFIED_BY ).getString(), is( "elvis" ) );
        assertThat( outputNode.hasProperty( "propA" ), is( true ) );
        assertThat( outputNode.getProperty( "propA" ).getString(), is( "Value A" ) );
        assertThat( outputNode.hasProperty( DataVirtLexicon.DataService.LAST_MODIFIED ), is( true ) );

        final Calendar modifiedDate = new GregorianCalendar( 2002, 4, 30, 9, 30, 10 );
        assertThat( outputNode.getProperty( DataVirtLexicon.DataService.LAST_MODIFIED ).getDate(), is( modifiedDate ) );

        // check child entries
        /*
         /dataservices/MyDataService.zip/product-view-vdb.xml : type = dv:serviceVdbEntry
             /dataservices/MyDataService.zip/product-view-vdb.xml/twitter-vdb.xml : type = dv:vdbEntry
         /dataservices/MyDataService.zip/books-driver-1.jar : type = dv:driverEntry
         /dataservices/MyDataService.zip/books-driver-2.jar : type = dv:driverEntry
         /dataservices/MyDataService.zip/portfolio-driver.jar : type = dv:driverEntry
         /dataservices/MyDataService.zip/firstDdl.ddl : type = dv:ddlEntry
         /dataservices/MyDataService.zip/secondDdl.ddl : type = dv:ddlEntry
         /dataservices/MyDataService.zip/firstResource.xml : type = dv:resourceEntry
         /dataservices/MyDataService.zip/secondResource.xml : type = dv:resourceEntry
         /dataservices/MyDataService.zip/firstUdf.jar : type = dv:udfEntry
         /dataservices/MyDataService.zip/secondUdf.jar : type = dv:udfEntry
         /dataservices/MyDataService.zip/books-connection.xml : type = dv:dataSourceEntry
         /dataservices/MyDataService.zip/portfoliobooks-connection.xml : type = dv:dataSourceEntry
         /dataservices/MyDataService.zip/books-vdb.xml : type = dv:vdbEntry
         /dataservices/MyDataService.zip/Portfolio-vdb.xml : type = dv:vdbEntry
        */

        final NodeIterator itr = outputNode.getNodes();
        assertThat( itr.getSize(), is( 14L ) );

        { // service VDB
            final String serviceVdbPath = "product-view-vdb.xml";
            assertThat( outputNode.hasNode( serviceVdbPath ), is( true ) );

            final Node serviceVdbEntryNode = outputNode.getNode( serviceVdbPath );
            assertThat( serviceVdbEntryNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.ServiceVdbEntry.NODE_TYPE ) );
            assertThat( serviceVdbEntryNode.hasProperty( DataVirtLexicon.DriverEntry.PATH ), is( true ) );
            assertThat( serviceVdbEntryNode.getProperty( DataVirtLexicon.DriverEntry.PATH ).getString(), is( serviceVdbPath ) );

            // check reference
            assertReferencedResource( serviceVdbEntryNode,
                                      DataVirtLexicon.ServiceVdbEntry.VDB_REF,
                                      VdbLexicon.Vdb.VIRTUAL_DATABASE,
                                      PublishPolicy.IF_MISSING,
                                      false );
            final Node vdbNode = outputNode.getParent().getNode( "product-view-vdb.xml" );
            assertThat( vdbNode.getProperty( VdbLexicon.Vdb.NAME ).getString(), is( "DynamicProducts" ) );

            // dependencies
            assertThat( serviceVdbEntryNode.getNodes().getSize(), is( 1L ) );
            final Node dependencyNode = serviceVdbEntryNode.getNodes().nextNode();
            assertThat( dependencyNode.getName(), is( "twitter-vdb.xml" ) );
            assertReferencedResource( dependencyNode,
                                      DataVirtLexicon.VdbEntry.VDB_REF,
                                      VdbLexicon.Vdb.VIRTUAL_DATABASE,
                                      PublishPolicy.IF_MISSING,
                                      false );
            final Node importVdbNode = outputNode.getParent().getNode( "twitter-vdb.xml" );
            assertThat( importVdbNode.getProperty( VdbLexicon.Vdb.NAME ).getString(), is( "twitter" ) );
        }

        // drivers
        assertDriver( outputNode, "books-driver-1.jar", PublishPolicy.DEFAULT );
        assertDriver( outputNode, "books-driver-2.jar", PublishPolicy.DEFAULT );
        assertDriver( outputNode, "portfolio-driver.jar", PublishPolicy.DEFAULT );

        // metadata
        assertDdl( outputNode, "firstDdl.ddl", PublishPolicy.ALWAYS );
        assertDdl( outputNode, "secondDdl.ddl", PublishPolicy.ALWAYS );

        // resources
        assertResource( outputNode, "firstResource.xml", PublishPolicy.DEFAULT );
        assertResource( outputNode, "secondResource.xml", PublishPolicy.ALWAYS );

        // udfs
        assertUdf( outputNode, "firstUdf.jar", PublishPolicy.NEVER );
        assertUdf( outputNode, "secondUdf.jar", PublishPolicy.DEFAULT );

        { // books connection
            assertConnection( outputNode, "books-connection.xml", PublishPolicy.IF_MISSING );
            final Node dsNode = outputNode.getParent().getNode( "booksConnection" );
            assertThat( dsNode.getProperty( DataVirtLexicon.Connection.TYPE ).getString(), is( Connection.Type.JDBC.name() ) );
            assertThat( dsNode.getProperty( DataVirtLexicon.Connection.JNDI_NAME ).getString(), is( "java:/jdbcSource" ) );
            assertThat( dsNode.getProperty( DataVirtLexicon.Connection.DRIVER_NAME ).getString(), is( "books-driver-1.jar" ) );
            assertThat( dsNode.hasProperty( DataVirtLexicon.Connection.CLASS_NAME ), is( false ) );
            assertThat( dsNode.getProperty( "prop1" ).getString(), is( "one" ) );
            assertThat( dsNode.getProperty( "prop2" ).getString(), is( "two" ) );
            assertThat( dsNode.getProperty( "prop3" ).getString(), is( "three" ) );
            assertThat( dsNode.getProperty( "prop4" ).getString(), is( "four" ) );
        }

        { // portfolio connection
            assertConnection( outputNode, "portfolio-connection.xml", PublishPolicy.DEFAULT );
            final Node dsNode = outputNode.getParent().getNode( "portfolioConnection" );
            assertThat( dsNode.getProperty( DataVirtLexicon.Connection.TYPE ).getString(),
                        is( Connection.Type.RESOURCE.name() ) );
            assertThat( dsNode.getProperty( DataVirtLexicon.Connection.JNDI_NAME ).getString(), is( "java:/jndiName" ) );
            assertThat( dsNode.getProperty( DataVirtLexicon.Connection.DRIVER_NAME ).getString(), is( "portfolio-driver.jar" ) );
            assertThat( dsNode.getProperty( DataVirtLexicon.Connection.CLASS_NAME ).getString(),
                        is( "portfolioDriverClassname" ) );
            assertThat( dsNode.getProperty( "prop1" ).getString(), is( "prop1Value" ) );
            assertThat( dsNode.getProperty( "prop2" ).getString(), is( "prop2Value" ) );
        }

        // VDBs
        assertVdb( outputNode, "books-vdb.xml", PublishPolicy.IF_MISSING );
        assertVdb( outputNode, "Portfolio-vdb.xml", PublishPolicy.DEFAULT );
    }

    @Test
    public void shouldSequenceDataServiceAtCustomPaths() throws Exception {
        final Node connectionsNode = this.rootNode.addNode( "connections" );
        System.setProperty( DataServiceSequencer.CONNECTION_PATH_PROPERTY, connectionsNode.getPath() );

        final Node driversNode = this.rootNode.addNode( "drivers" );
        System.setProperty( DataServiceSequencer.DRIVER_PATH_PROPERTY, driversNode.getPath() );

        final Node metadataNode = this.rootNode.addNode( "metadata" );
        System.setProperty( DataServiceSequencer.METADATA_PATH_PROPERTY, metadataNode.getPath() );

        final Node resourcesNode = this.rootNode.addNode( "resources" );
        System.setProperty( DataServiceSequencer.RESOURCE_PATH_PROPERTY, resourcesNode.getPath() );

        final Node udfsNode = this.rootNode.addNode( "udfs" );
        System.setProperty( DataServiceSequencer.UDF_PATH_PROPERTY, udfsNode.getPath() );

        final Node vdbsNode = this.rootNode.addNode( "vdbs" );
        System.setProperty( DataServiceSequencer.VDB_PATH_PROPERTY, vdbsNode.getPath() );

        createNodeWithContentFromFile( "MyDataService.zip", "dataservice/sample-ds.zip" );
        final Node outputNode = getOutputNode( this.rootNode, "dataservices/MyDataService.zip" );
        assertNotNull( outputNode );
        assertThat( outputNode.getPrimaryNodeType().getName(), is( DataVirtLexicon.DataService.NODE_TYPE ) );

        assertThat( vdbsNode.hasNode( "product-view-vdb.xml" ), is( true ) );
        assertThat( vdbsNode.hasNode( "twitter-vdb.xml" ), is( true ) );
        assertThat( vdbsNode.hasNode( "books-vdb.xml" ), is( true ) );
        assertThat( vdbsNode.hasNode( "Portfolio-vdb.xml" ), is( true ) );
        assertThat( driversNode.hasNode( "books-driver-1.jar" ), is( true ) );
        assertThat( driversNode.hasNode( "books-driver-2.jar" ), is( true ) );
        assertThat( driversNode.hasNode( "portfolio-driver.jar" ), is( true ) );
        assertThat( metadataNode.hasNode( "firstDdl.ddl" ), is( true ) );
        assertThat( metadataNode.hasNode( "secondDdl.ddl" ), is( true ) );
        assertThat( resourcesNode.hasNode( "firstResource.xml" ), is( true ) );
        assertThat( resourcesNode.hasNode( "secondResource.xml" ), is( true ) );
        assertThat( udfsNode.hasNode( "firstUdf.jar" ), is( false ) ); // never publish
        assertThat( udfsNode.hasNode( "secondUdf.jar" ), is( true ) );
        assertThat( connectionsNode.hasNode( "booksConnection" ), is( true ) );
        assertThat( connectionsNode.hasNode( "portfolioConnection" ), is( true ) );
    }

}
