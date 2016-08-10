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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * Represents a Service VDB in a Data Service archive.
 */
public class ServiceVdbEntry extends VdbEntry implements VdbEntryContainer {

    private final Collection< VdbEntry > dependencies = new ArrayList<>();

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.dataservice.VdbEntryContainer#addVdb(org.teiid.modeshape.sequencer.dataservice.VdbEntry)
     */
    @Override
    public void addVdb( final VdbEntry dependency ) {
        this.dependencies.add( Objects.requireNonNull( dependency, "dependency" ) );
        dependency.setContainer( this );
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.dataservice.VdbEntryContainer#getVdbPaths()
     */
    @Override
    public String[] getVdbPaths() {
        final DataServiceEntry[] entries = getVdbs();

        if ( entries.length == 0 ) {
            return DataServiceEntry.NO_PATHS;
        }

        return DataServiceEntry.getPaths( entries );
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.dataservice.VdbEntryContainer#getVdbs()
     */
    @Override
    public VdbEntry[] getVdbs() {
        if ( this.dependencies.isEmpty() ) {
            return VdbEntry.NO_VDBS;
        }

        return this.dependencies.toArray( new VdbEntry[ this.dependencies.size() ] );
    }

}
