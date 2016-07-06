/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of 
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 * 
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.modeshape.sequencer.dataservice;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a data service manifest data source entry.
 */
class DataSourceDescriptor {

    private final Set< String > driverPaths; // the archive resource paths of the drivers
    private String path; // the archive resource path of the data source
    private DataserviceImportVdb parent;

    DataSourceDescriptor( final DataserviceImportVdb vdb ) {
        this.parent = vdb;
        this.driverPaths = new HashSet<>( 3 );
    }

    /**
     * @param archivePath the driver archive resource path being added (cannot be empty)
     */
    public void addDriverPath( final String archivePath ) {
        this.driverPaths.add( archivePath );
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals( final Object obj ) {
        if ( ( obj == null ) || !getClass().equals( obj.getClass() ) ) {
            return false;
        }

        final DataSourceDescriptor that = ( DataSourceDescriptor )obj;

        if ( !Objects.equals( this.path, that.path ) ) {
            return false;
        }

        return Objects.deepEquals( this.driverPaths, that.driverPaths );
    }

    /**
     * @return the driver archive resource paths (never <code>null</code> but can be empty if none were added)
     */
    public Collection< String > getDriverPaths() {
        return this.driverPaths;
    }

    /**
     * @return the parent (never <code>null</code>)
     */
    public DataserviceImportVdb getParent() {
        return this.parent;
    }

    /**
     * @return the archive resource path of this data source (can be <code>null</code> if not set)
     */
    public String getPath() {
        return this.path;
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Objects.hash( this.path, this.driverPaths );
    }

    /**
     * @param archivePath the archive resource path of this data source (should not be empty)
     */
    public void setPath( final String archivePath ) {
        this.path = archivePath;
    }

}
