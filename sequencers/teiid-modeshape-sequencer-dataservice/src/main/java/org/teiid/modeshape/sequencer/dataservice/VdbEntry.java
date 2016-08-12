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

import java.util.Objects;

/**
 * Represents a connection entry in a Data Service archive.
 */
public class VdbEntry extends DataServiceEntry {

    static final VdbEntry[] NO_VDBS = new VdbEntry[ 0 ];

    private VdbEntryContainer container;
    private String vdbName;
    private String vdbVersion;

    /**
     * @return this entry's container (can be <code>null</code> if not yet set)
     */
    public VdbEntryContainer getContainer() {
        return this.container;
    }

    /**
     * @return the VDB name (can be <code>null</code> or empty)
     */
    public String getVdbName() {
        return this.vdbName;
    }

    /**
     * @return the VDB version (can be <code>null</code> or empty)
     */
    public String getVdbVersion() {
        return this.vdbVersion;
    }

    /**
     * @param container the container of this entry (cannot be <code>null</code>)
     */
    public void setContainer( final VdbEntryContainer container ) {
        this.container = Objects.requireNonNull( container, "container" );
    }

    /**
     * @param vdbName the value to use to set the VDB name (can be <code>null</code> or empty)
     */
    public void setVdbName( final String vdbName ) {
        this.vdbName = vdbName;
    }

    /**
     * @param vdbVersion the value to use to set the VDB version (can be <code>null</code> or empty)
     */
    public void setVdbVersion( final String vdbVersion ) {
        this.vdbVersion = vdbVersion;
    }

}
