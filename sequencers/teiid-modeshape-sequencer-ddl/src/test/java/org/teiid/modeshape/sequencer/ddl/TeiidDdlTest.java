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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.teiid.modeshape.sequencer.ddl.node.AstNode;

public abstract class TeiidDdlTest {

    protected void assertMixinType( final AstNode node,
                                    final String expectedNodeType ) {
        assertThat(hasMixin(node, expectedNodeType), is(true));
    }

    protected void assertProperty( final AstNode node,
                                   final String name,
                                   final Object expectedValue ) {
        Object actualValue = node.getProperty(name);
        assertThat(actualValue, is(expectedValue));
    }

    protected DdlTokenStream getTokens( final String content ) {
        final DdlTokenStream tokens = new DdlTokenStream(content, DdlTokenStream.ddlTokenizer(false), false);
        tokens.start();
        return tokens;
    }

    protected boolean hasMixin( final AstNode node,
                                final String mixin ) {
        return node.getMixins().contains(mixin);
    }

}
