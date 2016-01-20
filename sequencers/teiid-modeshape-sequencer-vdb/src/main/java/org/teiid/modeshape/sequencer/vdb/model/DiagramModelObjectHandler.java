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
package org.teiid.modeshape.sequencer.vdb.model;

import javax.jcr.Node;

import org.teiid.modeshape.sequencer.vdb.xmi.XmiElement;

/**
 * The model object handler for the {@link org.teiid.modeshape.sequencer.vdb.lexicon.DiagramLexicon.Namespace#URI diagram} namespace.
 * Currently diagram objects are not sequenced.
 */
public final class DiagramModelObjectHandler extends ModelObjectHandler {

    /**
     * @see org.teiid.modeshape.sequencer.vdb.model.ModelObjectHandler#process(org.teiid.modeshape.sequencer.vdb.xmi.XmiElement, javax.jcr.Node)
     */
    @Override
    protected void process( final XmiElement element,
                            final Node node ) throws Exception {
        // diagram objects are not being sequenced
    }
}
