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
package org.teiid.modeshape.sequencer.dataservice.lexicon;

import static org.teiid.modeshape.sequencer.dataservice.lexicon.XmiLexicon.Namespace.PREFIX;

/**
 * Constants associated with the XMI namespace used in reading XMI models and writing JCR nodes.
 */
public interface XmiLexicon {

    /**
     * The URI and prefix constants of the XMI namespace.
     */
    public interface Namespace {
        String PREFIX = "xmi";
        String URI = "http://www.omg.org/XMI";
    }

    /**
     * Constants associated with the XMI namespace that identify XMI model identifiers.
     */
    public interface ModelId {
        String UUID = "uuid";
        String XMI_TAG = "XMI";
    }

    /**
     * JCR identifiers relating to the XMI namespace.
     */
    public interface JcrId {
        String MODEL = PREFIX + ":model";
        String REFERENCEABLE = PREFIX + ":referenceable";
        String UUID = PREFIX + ':' + ModelId.UUID;
        String VERSION = PREFIX + ":version";
        String XMI = PREFIX + ":xmi";
    }
}
