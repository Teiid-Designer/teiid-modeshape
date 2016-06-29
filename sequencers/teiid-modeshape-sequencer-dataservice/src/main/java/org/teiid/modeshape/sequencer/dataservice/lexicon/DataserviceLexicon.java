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

import static org.teiid.modeshape.sequencer.dataservice.lexicon.DataserviceLexicon.Namespace.PREFIX;

/**
 * Constants associated with the Dataservice namespace used in reading Dataservice manifests and writing JCR nodes.
 */
public interface DataserviceLexicon {

    /**
     * The URI and prefix constants of the Dataservice namespace.
     */
    public interface Namespace {
        String PREFIX = "tko";
        String URI = "http://www.teiid.org/komodo/1.0";
    }

    /**
     * Constants associated with the Dataservice namespace that identify Dataservice manifest identifiers.
     */
    public interface ManifestIds {
        String DATASERVICE = "dataservice";
        String SERVICE_VDB = "service-vdb";
        String VDBS = "vdbs";
        String DATASOURCES = "datasources";
        String DRIVERS = "drivers";
    }

    /**
     * JCR identifiers relating to the Dataservice manifest.
     */
    public interface Dataservice {
        String DATA_SERVICE = PREFIX + ":dataService";
    }
}
