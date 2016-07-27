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
import java.util.Properties;
import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import org.modeshape.common.annotation.ThreadSafe;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;
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

        try ( final InputStream datasourceStream = binaryValue.getStream() ) {
            final DataserviceDatasource datasources = readDatasource( datasourceStream, outputNode, context );
            
            if ( datasources == null ) {
                throw new Exception( "DatasourceSequencer.execute failed. The file cannot be read." ); // TODO i18n
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

    protected DataserviceDatasource readDatasource( final InputStream inputStream,
                                                      final Node outputNode,
                                                    final Context context ) throws Exception {
        LOGGER.debug( "----before reading datasource" );

        final DataSourceReader reader = new DataSourceReader();
        final DataserviceDatasource datasource = reader.read( inputStream );
        assert ( datasource != null ) : "datasources is null";

        // Create the output node for each data source
        outputNode.getSession().move( outputNode.getPath(), ( outputNode.getParent().getPath() + '/' + datasource.getName() ) );
        outputNode.setPrimaryType( DataVirtLexicon.Datasource.NODE_TYPE );
        outputNode.setProperty( DataVirtLexicon.Datasource.TYPE, datasource.getType().name() );

        // JNDI name
        if ( datasource.getJndiName() != null ) {
            outputNode.setProperty( DataVirtLexicon.Datasource.JNDI_NAME, datasource.getJndiName() );
        }

        // driver name
        if ( datasource.getDriverName() != null ) {
            outputNode.setProperty( DataVirtLexicon.Datasource.DRIVER_NAME, datasource.getDriverName() );
        }

        // class name
        if ( datasource.getClassName() != null ) {
            outputNode.setProperty( DataVirtLexicon.Datasource.CLASS_NAME, datasource.getClassName() );
        }

        // customer properties
        final Properties props = datasource.getProperties();

        if ( !props.isEmpty() ) {
            for ( final String prop : props.stringPropertyNames() ) {
                outputNode.setProperty( prop, props.getProperty( prop ) );
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
