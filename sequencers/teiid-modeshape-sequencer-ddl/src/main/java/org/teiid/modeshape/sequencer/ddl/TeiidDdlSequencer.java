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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import org.modeshape.common.logging.Logger;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.modeshape.sequencer.ddl.DdlParser;
import org.modeshape.sequencer.ddl.DdlParsers;
import org.modeshape.sequencer.ddl.DdlSequencer;

/**
 * A {@link Sequencer sequencer} for Teiid DDL.
 */
public final class TeiidDdlSequencer extends DdlSequencer {

	private static final String[] GRAMMARS = new String[] { TeiidDdlParser.ID };
    private static final Logger LOGGER = Logger.getLogger(TeiidDdlSequencer.class);

	/**
	 * {@inheritDoc}
	 *
	 * @see org.modeshape.sequencer.ddl.DdlSequencer#createParsers(java.util.List)
	 */
	@Override
	protected DdlParsers createParsers( final List< DdlParser > parsers ) {
		return super.createParsers( getParserList() ); // make sure Teiid parser is always used
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.modeshape.sequencer.ddl.DdlSequencer#execute(javax.jcr.Property,
	 *      javax.jcr.Node, org.modeshape.jcr.api.sequencer.Sequencer.Context)
	 */
	@Override
	public boolean execute( final Property inputProperty, 
			                final Node outputNode, 
			                final Context context ) throws Exception {
		LOGGER.debug( "TeiidDdlSequencer.execute called:outputNode name='{0}', path='{1}'", 
				      outputNode.getName(),
				      outputNode.getPath() );
		return super.execute( inputProperty, outputNode, context );
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.modeshape.sequencer.ddl.DdlSequencer#getGrammars()
	 */
	@Override
	public String[] getGrammars() {
		return GRAMMARS;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.modeshape.sequencer.ddl.DdlSequencer#getParserList()
	 */
	@Override
	protected List<DdlParser> getParserList() {
		return Collections.singletonList( ( DdlParser )new TeiidDdlParser() );
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.modeshape.sequencer.ddl.DdlSequencer#initialize(javax.jcr.NamespaceRegistry,
	 *      org.modeshape.jcr.api.nodetype.NodeTypeManager)
	 */
	@Override
	public void initialize( final NamespaceRegistry registry, 
			                final NodeTypeManager nodeTypeManager ) throws RepositoryException, IOException {
        registerNodeTypes( "/org/modeshape/sequencer/ddl/StandardDdl.cnd", nodeTypeManager, true );
		registerNodeTypes( "TeiidDdl.cnd", nodeTypeManager, true );
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.modeshape.sequencer.ddl.DdlSequencer#setGrammars(java.lang.String[])
	 */
	@Override
	public void setGrammars( final String[] grammarNamesOrClasses ) {
		// nothing to do
	}

}
