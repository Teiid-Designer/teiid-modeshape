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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.teiid.modeshape.sequencer.ddl.DdlTokenStream.DdlTokenizer;
import org.teiid.modeshape.sequencer.ddl.node.AstNode;

/**
 * A DDL parser for the Teiid dialect.
 */
public final class TeiidDdlParser extends StandardDdlParser implements TeiidDdlConstants {

    /**
     * The Teiid parser identifier.
     */
    public static final String ID = "TEIID";

    private final Map<String, String> namespaceAliases;
    private final Collection<StatementParser> parsers;

    /**
     * Constructs a Teiid DDL parser.
     */
    public TeiidDdlParser() {
        setDatatypeParser(new TeiidDataTypeParser());
        this.namespaceAliases = new HashMap<String, String>();

        // setup statement parsers
        final List<StatementParser> temp = new ArrayList<StatementParser>(5);
        temp.add(new CreateTableParser(this));
        temp.add(new CreateProcedureParser(this));
        temp.add(new CreateTriggerParser(this));
        temp.add(new AlterOptionsParser(this));
        temp.add(new OptionNamespaceParser(this));
        this.parsers = Collections.unmodifiableCollection(temp);
    }

    boolean accessParseDefaultClause( final DdlTokenStream tokens,
                                      final AstNode columnNode ) {
        return super.parseDefaultClause(tokens, columnNode);
    }

    String accessParseUntilTerminator( final DdlTokenStream tokens ) {
        return super.parseUntilTerminator(tokens);
    }

    String accessParseUntilTerminatorIgnoreEmbeddedStatements( final DdlTokenStream tokens ) {
        return super.parseUntilTerminatorIgnoreEmbeddedStatements(tokens);
    }

    void addNamespaceAlias( final String alias,
                            final String identifier ) {
        this.namespaceAliases.put(alias, identifier);
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.StandardDdlParser#getCustomDataTypeStartWords()
     */
    @Override
    protected List<String> getCustomDataTypeStartWords() {
        return TeiidDataType.getStartWords();
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.StandardDdlParser#getId()
     */
    @Override
    public String getId() {
        return ID;
    }

    /**
     * @param alias the alias whose namespace URI is being requested (cannot be <code>null</code> or empty)
     * @return the URI or <code>null</code> if not found
     */
    String getNamespaceUri( final String alias ) {
        return this.namespaceAliases.get(alias);
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.StandardDdlParser#getValidSchemaChildTypes()
     */
    @Override
    protected String[] getValidSchemaChildTypes() {
        return TeiidDdlLexicon.getValidSchemaChildTypes();
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.StandardDdlParser#initializeTokenStream(org.teiid.modeshape.sequencer.ddl.DdlTokenStream)
     */
    @Override
    protected void initializeTokenStream( final DdlTokenStream tokens ) {
        super.initializeTokenStream(tokens);
        tokens.registerKeyWords(TeiidReservedWord.asList());
        tokens.registerKeyWords(TeiidNonReservedWord.asList());

        for (final DdlStatement stmt : DdlStatement.values()) {
            tokens.registerStatementStartPhrase(stmt.tokens());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.StandardDdlParser#parseNextStatement(org.teiid.modeshape.sequencer.ddl.DdlTokenStream,
     *      org.teiid.modeshape.sequencer.ddl.node.AstNode)
     */
    @Override
    protected AstNode parseNextStatement( final DdlTokenStream tokens,
                                          final AstNode parentNode ) {
        for (final StatementParser parser : this.parsers) {
            if (parser.matches(tokens)) {
                markStartOfStatement(tokens);
                final AstNode statementNode = parser.parse(tokens, parentNode);
                markEndOfStatement(tokens, statementNode);
                return statementNode;
            }
        }
        
        // don't consume comment as super will do that for ignorable statements
        if (tokens.matches(DdlTokenizer.COMMENT)) return null; // TODO these comments are being thrown out

        // Unparsable DDL statement
        throw new TeiidDdlParsingException(tokens, "Unparsable DDL statement");
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.teiid.modeshape.sequencer.ddl.DdlParser#postProcess(org.teiid.modeshape.sequencer.ddl.node.AstNode)
     */
    @Override
    public void postProcess( AstNode rootNode ) {
        super.postProcess(rootNode);

        for (final StatementParser parser : this.parsers) {
            parser.postProcess(rootNode);
        }
    }

}
