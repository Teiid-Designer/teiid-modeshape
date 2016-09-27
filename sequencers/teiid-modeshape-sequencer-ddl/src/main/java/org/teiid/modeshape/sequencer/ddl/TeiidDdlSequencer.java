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
package org.teiid.modeshape.sequencer.ddl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.teiid.modeshape.sequencer.ddl.node.AstNode;

/**
 * A {@link Sequencer sequencer} for Teiid DDL.
 */
public class TeiidDdlSequencer extends DdlSequencer {

    private static final String[] GRAMMARS = new String[] { TeiidDdlParser.ID };

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.ddl.DdlSequencer#createParsers(java.util.List)
     */
    @Override
    protected DdlParsers createParsers( final List< DdlParser > parsers ) {
        return super.createParsers( getParserList() ); // make sure Teiid parser is always used
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.ddl.DdlSequencer#getGrammars()
     */
    @Override
    public String[] getGrammars() {
        return GRAMMARS;
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.ddl.DdlSequencer#getParserList()
     */
    @Override
    protected List< DdlParser > getParserList() {
        return Collections.singletonList( ( DdlParser )new TeiidDdlParser() );
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.ddl.DdlSequencer#initialize(javax.jcr.NamespaceRegistry,
     *      org.modeshape.jcr.api.nodetype.NodeTypeManager)
     */
    @Override
    public void initialize( final NamespaceRegistry registry,
                            final NodeTypeManager nodeTypeManager ) throws RepositoryException, IOException {
        final URL standardDdlCndUrl = getClass().getResource( "/org/teiid/modeshape/sequencer/ddl/StandardDdl.cnd" );
        registerNodeTypes( standardDdlCndUrl.openStream(), nodeTypeManager, true );

        final URL teiidDdlCndUrl = getClass().getResource( "/org/teiid/modeshape/sequencer/ddl/TeiidDdl.cnd" );
        registerNodeTypes( teiidDdlCndUrl.openStream(), nodeTypeManager, true );
    }
    
    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.ddl.DdlSequencer#preProcess(org.teiid.modeshape.sequencer.ddl.node.AstNode,
     *      javax.jcr.Node)
     */
    @Override
    protected void preProcess( final AstNode astNode,
                               final Node parentNode ) throws RepositoryException {
        // register any namespaces found in the DDL
        if ( astNode.hasMixin( TeiidDdlLexicon.OptionNamespace.STATEMENT ) ) {
            final NamespaceRegistry registry = parentNode.getSession().getWorkspace().getNamespaceRegistry();
            final String uri = astNode.getProperty( TeiidDdlLexicon.OptionNamespace.URI ).toString();
            registry.registerNamespace( astNode.getName(), uri );
        }
    }

    /**
     * @param ddl the Teiid DDL being processed (cannot be <code>null</code>)
     * @param outputNode the repository output node (cannot be <code>null</code>)
     * @throws Exception if an error occurs
     */
    public void sequenceDdl( final String ddl,
                             final Node outputNode ) throws Exception {
        final ByteArrayInputStream ddlStream = new ByteArrayInputStream( ddl.getBytes() );
        final Binary binary = outputNode.getSession().getValueFactory().createBinary( ddlStream );
        final Property temp = outputNode.setProperty( "ddlStream", binary );

        try {
            if ( !execute( temp, outputNode, null ) ) {
                throw new Exception( "Error sequencing DDL: " + ddl ); // TODO i18n this
            }
        } finally {
            temp.remove();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.ddl.DdlSequencer#setGrammars(java.lang.String[])
     */
    @Override
    public void setGrammars( final String[] grammarNamesOrClasses ) {
        // nothing to do
    }

}
