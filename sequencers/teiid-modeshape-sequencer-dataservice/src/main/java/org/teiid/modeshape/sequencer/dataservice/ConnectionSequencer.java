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
import java.util.Objects;
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
 * A sequencer of DV connection files.
 */
@ThreadSafe
public class ConnectionSequencer extends Sequencer {

    private static final Logger LOGGER = Logger.getLogger( ConnectionSequencer.class );

    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#execute(javax.jcr.Property, javax.jcr.Node,
     *      org.modeshape.jcr.api.sequencer.Sequencer.Context)
     */
    @Override
    public boolean execute( final Property inputProperty,
                            final Node outputNode,
                            final Context context ) throws Exception {
        LOGGER.debug( "Connection sequencer execute called with output node of " + outputNode.getPath() );
        final Binary binaryValue = inputProperty.getBinary();
        CheckArg.isNotNull( binaryValue, "binary" );

        try ( final InputStream connectionStream = binaryValue.getStream() ) {
            final Connection connection = readConnection( connectionStream, outputNode, context );

            if ( connection == null ) {
                throw new Exception( TeiidI18n.noDatasourceFound.text( inputProperty.getPath() ) );
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.errorReadingDatasourceFile.text( inputProperty.getPath(), e.getMessage() ), e );
        }

        return true;
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

    private Connection readConnection( final InputStream inputStream,
                                       final Node outputNode,
                                       final Context context ) throws Exception {
        assert ( inputStream != null );
        assert ( outputNode != null );
        LOGGER.debug( "----before reading connection" );

        final ConnectionReader reader = new ConnectionReader();
        final Connection connection = reader.read( inputStream );

        // Create the output node for each connection
        outputNode.getSession().move( outputNode.getPath(), ( outputNode.getParent().getPath() + '/' + connection.getName() ) );
        outputNode.setPrimaryType( DataVirtLexicon.Connection.NODE_TYPE );
        outputNode.setProperty( DataVirtLexicon.Connection.TYPE, connection.getType().name() );

        // JNDI name
        if ( connection.getJndiName() != null ) {
            outputNode.setProperty( DataVirtLexicon.Connection.JNDI_NAME, connection.getJndiName() );
        }

        // driver name
        if ( connection.getDriverName() != null ) {
            outputNode.setProperty( DataVirtLexicon.Connection.DRIVER_NAME, connection.getDriverName() );
        }

        // class name
        if ( connection.getClassName() != null ) {
            outputNode.setProperty( DataVirtLexicon.Connection.CLASS_NAME, connection.getClassName() );
        }

        // custom properties
        final Properties props = connection.getProperties();

        if ( !props.isEmpty() ) {
            for ( final String prop : props.stringPropertyNames() ) {
                outputNode.setProperty( prop, props.getProperty( prop ) );
            }
        }

        LOGGER.debug( ">>>>done reading connection xml\n\n" );
        return connection;
    }

    /**
     * @param connectionStream the stream being processed (cannot be <code>null</code>)
     * @param connectionOutputNode the repository output node (cannot be <code>null</code>)
     * @return <code>true</code> if the connection was sequenced successfully
     * @throws Exception
     */
    public boolean sequenceConnection( final InputStream connectionStream,
                                       final Node connectionOutputNode ) throws Exception {
        final Connection ds = readConnection( Objects.requireNonNull( connectionStream, "connectionStream" ),
                                              Objects.requireNonNull( connectionOutputNode, "connectionOutputNode" ),
                                              null );

        if ( ds == null ) {
            throw new RuntimeException( TeiidI18n.errorReadingDatasourceFile.text( connectionOutputNode.getPath() ) );
        }

        return true;
    }

}
