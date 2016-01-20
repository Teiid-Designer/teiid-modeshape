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

import org.modeshape.common.text.ParsingException;
import org.modeshape.sequencer.ddl.DdlTokenStream;
import org.modeshape.sequencer.ddl.node.AstNode;
import org.teiid.modeshape.sequencer.ddl.TeiidDdlConstants.DdlStatement;
import org.teiid.modeshape.sequencer.ddl.TeiidDdlConstants.TeiidReservedWord;

/**
 * A parser for the Teiid <option namespace> DDL statement
 * <p>
 * <code>
 * SET NAMESPACE <string> AS <identifier>
 * </code>
 */
final class OptionNamespaceParser extends StatementParser {

    OptionNamespaceParser( final TeiidDdlParser teiidDdlParser ) {
        super(teiidDdlParser);
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.StatementParser#matches(org.modeshape.sequencer.ddl.DdlTokenStream)
     */
    @Override
    boolean matches( final DdlTokenStream tokens ) {
        return tokens.matches(DdlStatement.OPTION_NAMESPACE.tokens());
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.StatementParser#parse(org.modeshape.sequencer.ddl.DdlTokenStream,
     *      org.modeshape.sequencer.ddl.node.AstNode)
     */
    @Override
    AstNode parse( final DdlTokenStream tokens,
                   final AstNode parentNode ) throws ParsingException {
        if (tokens.canConsume(DdlStatement.OPTION_NAMESPACE.tokens())) {
            final String uri = parseValue(tokens);

            if (tokens.canConsume(TeiidReservedWord.AS.toDdl())) {
                final String alias = parseIdentifier(tokens);
                addNamespaceAlias(alias, uri);
                final AstNode optionNamespaceNode = getNodeFactory().node(alias,
                                                                          parentNode,
                                                                          TeiidDdlLexicon.OptionNamespace.STATEMENT);
                optionNamespaceNode.setProperty(TeiidDdlLexicon.OptionNamespace.URI, uri);
                return optionNamespaceNode;
            }
        }

        throw new TeiidDdlParsingException(tokens, "Unparsable option namespace statement");
    }

    @Override
    protected void postProcess( AstNode rootNode ) {

    }
}
