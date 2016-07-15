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

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import org.modeshape.common.annotation.ThreadSafe;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;

/**
 * A sequencer of DV Datasource files.
 */
@ThreadSafe
public class DatasourceSequencer extends Sequencer {

    private static final String[] DATASOURCE_FILE_EXTENSIONS = { ".tds" };
    private static final Logger LOGGER = Logger.getLogger( DatasourceSequencer.class );

    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#execute(javax.jcr.Property, javax.jcr.Node,
     *      org.modeshape.jcr.api.sequencer.Sequencer.Context)
     */
    @Override
    public boolean execute( final Property inputProperty,
                            final Node outputNode,
                            final Context context ) throws Exception {
        final Binary binaryValue = inputProperty.getBinary();
        CheckArg.isNotNull( binaryValue, "binary" );

        try ( InputStream datasourceStream = binaryValue.getStream() ) {
            DataserviceDatasource[] datasources = readDatasource( datasourceStream, outputNode, context );
            if ( datasources == null ) {
                throw new Exception( "DatasourceSequencer.execute failed. The file cannot be read." );
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.errorReadingDatasourceFile.text( inputProperty.getPath(), e.getMessage() ), e );
        }
        return true;
    }

    /**
     * @param resourceName the name of the resource being checked (cannot be <code>null</code>)
     * @return <code>true</code> if the resource has a datasource file extension
     */
    public boolean hasDatasourceFileExtension( final String resourceName ) {
        for ( final String extension : DATASOURCE_FILE_EXTENSIONS ) {
            if ( resourceName.endsWith( extension ) ) {
                return true;
            }
        }

        // not a datasource file
        return false;
    }

    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#initialize(javax.jcr.NamespaceRegistry,
     *      org.modeshape.jcr.api.nodetype.NodeTypeManager)
     */
    @Override
    public void initialize( final NamespaceRegistry registry,
                            final NodeTypeManager nodeTypeManager ) throws RepositoryException, IOException {
        LOGGER.debug( "enter initialize" );

        registerNodeTypes( "dv.cnd", nodeTypeManager, true );
        LOGGER.debug( "dv.cnd loaded" );

        LOGGER.debug( "exit initialize" );
    }

    protected DataserviceDatasource[] readDatasource( final InputStream inputStream,
                                                      final Node outputNode,
                                                      final Context context ) throws Exception {
        LOGGER.debug( "----before reading datasource" );

        final DataSourceReader reader = new DataSourceReader();
        final DataserviceDatasource[] datasources = reader.read( inputStream );
        assert ( datasources != null ) : "datasources is null";
        
        for ( final DataserviceDatasource ds : datasources ) {
            
        }

        // Create the output node for each data source
        outputNode.getSession().move( outputNode.getPath(), ( outputNode.getParent().getPath() + '/' + datasource.getName() ) );
        outputNode.setPrimaryType( DataVirtLexicon.Datasource.NODE_TYPE );
        outputNode.addMixin( JcrConstants.MIX_REFERENCEABLE );

        outputNode.setProperty( DataVirtLexicon.Datasource.TYPE, datasource.getType().name() );

        Enumeration< ? > e = datasource.getProperties().propertyNames();

        while ( e.hasMoreElements() ) {
            String propKey = ( String )e.nextElement();
            // JNDI Name property
            if ( DataVirtLexicon.DatasourceXml.JNDI_NAME_PROP.equals( propKey ) ) {
                outputNode.setProperty( DataVirtLexicon.Datasource.JNDI_NAME,
                                        datasource.getPropertyValue( DataVirtLexicon.DatasourceXml.JNDI_NAME_PROP ) );
                // Driver Name property
            } else if ( DataVirtLexicon.DatasourceXml.DRIVER_NAME_PROP.equals( propKey ) ) {
                outputNode.setProperty( DataVirtLexicon.Datasource.DRIVER_NAME,
                                        datasource.getPropertyValue( DataVirtLexicon.DatasourceXml.DRIVER_NAME_PROP ) );
                // Classname property
            } else if ( DataVirtLexicon.DatasourceXml.CLASSNAME_PROP.equals( propKey ) ) {
                outputNode.setProperty( DataVirtLexicon.Datasource.CLASS_NAME,
                                        datasource.getPropertyValue( DataVirtLexicon.DatasourceXml.CLASSNAME_PROP ) );
                // arbitrary source property
            } else {
                outputNode.setProperty( propKey, datasource.getPropertyValue( propKey ) );
            }
        }

        LOGGER.debug( ">>>>done reading datasource xml\n\n" );
        return datasource;
    }

    public boolean sequenceDatasource( final InputStream datasourceStream,
                                       final Node datasourceOutputNode ) throws Exception {
        final DataserviceDatasource ds = readDatasource( datasourceStream, datasourceOutputNode, null );

        if ( ds == null ) {
            throw new RuntimeException( TeiidI18n.errorReadingDatasourceFile.text( datasourceOutputNode.getPath() ) );
        }

        return true;
    }

}
