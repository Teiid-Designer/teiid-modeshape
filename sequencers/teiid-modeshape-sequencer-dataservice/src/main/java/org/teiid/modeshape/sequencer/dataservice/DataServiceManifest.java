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

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import org.modeshape.common.util.StringUtil;

/**
 * Represents a Data Service archive manifest file.
 */
public class DataServiceManifest implements VdbEntryContainer {

    /**
     * The date formatter for the last modified property.
     */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss" );

    /**
     * @param lastModifiedDate the string representation of the date being parsed (cannot be <code>null</code>)
     * @return the date (never <code>null</code>)
     * @throws DateTimeParseException if the input cannot be parsed
     */
    public static LocalDateTime parse( final String lastModifiedDate ) throws DateTimeParseException {
        return LocalDateTime.parse( Objects.requireNonNull( lastModifiedDate, "lastModifiedDate" ), DATE_FORMATTER );
    }

    static DataServiceManifest read( final InputStream stream ) throws Exception {
        return new DataServiceManifestReader().read( stream );
    }

    private final Collection< ConnectionEntry > dataSources = new ArrayList<>();
    private String description;
    private final Collection< DataServiceEntry > drivers = new ArrayList<>();
    private LocalDateTime lastModified;
    private final Collection< DataServiceEntry > metadata = new ArrayList<>();
    private String modifiedBy;
    private String name;
    private final Properties props = new Properties();
    private final Collection< DataServiceEntry > resources = new ArrayList<>();
    private ServiceVdbEntry serviceVdb;
    private final Collection< DataServiceEntry > udfs = new ArrayList<>();
    private final Collection< VdbEntry > vdbs = new ArrayList<>();

    /**
     * @param dataSourceEntry a connection entry (cannot be <code>null</code>)
     */
    public void addDataSource( final ConnectionEntry dataSourceEntry ) {
        this.dataSources.add( Objects.requireNonNull( dataSourceEntry, "dataSourceEntry" ) );
    }

    /**
     * @param driverEntry a driver entry (cannot be <code>null</code>)
     */
    public void addDriver( final DataServiceEntry driverEntry ) {
        this.drivers.add( Objects.requireNonNull( driverEntry, "driverEntry" ) );
    }

    /**
     * @param ddlEntry a DDL entry (cannot be <code>null</code>)
     */
    public void addMetadata( final DataServiceEntry ddlEntry ) {
        this.metadata.add( Objects.requireNonNull( ddlEntry, "ddlEntry" ) );
    }

    /**
     * @param resourceEntry a entry (cannot be <code>null</code>)
     */
    public void addResource( final DataServiceEntry resourceEntry ) {
        this.resources.add( Objects.requireNonNull( resourceEntry, "resourceEntry" ) );
    }

    /**
     * @param udfEntry a UDF entry (cannot be <code>null</code>)
     */
    public void addUdf( final DataServiceEntry udfEntry ) {
        this.udfs.add( Objects.requireNonNull( udfEntry, "udfEntry" ) );
    }

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.dataservice.VdbEntryContainer#addVdb(org.teiid.modeshape.sequencer.dataservice.VdbEntry)
     */
    @Override
    public void addVdb( final VdbEntry vdbEntry ) {
        this.vdbs.add( Objects.requireNonNull( vdbEntry, "vdbEntry" ) );
        vdbEntry.setContainer( this );
    }

    /**
     * @return the entry paths of the connection files (never <code>null</code> but can be empty)
     */
    public String[] getDataSourcePaths() {
        final DataServiceEntry[] entries = getDataSources();

        if ( entries.length == 0 ) {
            return DataServiceEntry.NO_PATHS;
        }

        return DataServiceEntry.getPaths( entries );
    }

    /**
     * @return the connection entries (never <code>null</code> but can be empty)
     */
    public ConnectionEntry[] getDataSources() {
        if ( this.dataSources.isEmpty() ) {
            return ConnectionEntry.NO_DATA_SOURCES;
        }

        return this.dataSources.toArray( new ConnectionEntry[ this.dataSources.size() ] );
    }

    /**
     * @return the data service description (can be <code>null</code> or empty)
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * @return the entry paths of the driver files (never <code>null</code> but can be empty)
     */
    public String[] getDriverPaths() {
        final DataServiceEntry[] entries = getDrivers();

        if ( entries.length == 0 ) {
            return DataServiceEntry.NO_PATHS;
        }

        return DataServiceEntry.getPaths( entries );
    }

    /**
     * @return the driver entries (never <code>null</code> but can be empty)
     */
    public DataServiceEntry[] getDrivers() {
        if ( this.drivers.isEmpty() ) {
            return ConnectionEntry.NO_DATA_SOURCES;
        }

        return this.drivers.toArray( new DataServiceEntry[ this.drivers.size() ] );
    }

    /**
     * @return the last time the manifest was modified (can be <code>null</code> or empty)
     */
    public LocalDateTime getLastModified() {
        return this.lastModified;
    }

    /**
     * @return the metadata entries (never <code>null</code> but can be empty)
     */
    public DataServiceEntry[] getMetadata() {
        if ( this.metadata.isEmpty() ) {
            return ConnectionEntry.NO_DATA_SOURCES;
        }

        return this.metadata.toArray( new DataServiceEntry[ this.metadata.size() ] );
    }

    /**
     * @return the entry paths of the metadata files (never <code>null</code> but can be empty)
     */
    public String[] getMetadataPaths() {
        final DataServiceEntry[] entries = getMetadata();

        if ( entries.length == 0 ) {
            return DataServiceEntry.NO_PATHS;
        }

        return DataServiceEntry.getPaths( entries );
    }

    /**
     * @return the name of the user who last modified the data service (can be <code>null</code> or empty)
     */
    public String getModifiedBy() {
        return this.modifiedBy;
    }

    /**
     * @return the Data Service name (can be <code>null</code> or empty)
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the custom properties (never <code>null</code> but can be empty)
     */
    public Properties getProperties() {
        return this.props;
    }

    /**
     * @param propName the name of the property whose value is being requested (cannot be <code>null</code>)
     * @return the property value or <code>null</code> if the specified property does not exist
     */
    public String getPropertyValue( final String propName ) {
        return this.props.getProperty( Objects.requireNonNull( propName, "propName" ) ); //$NON-NLS-1$
    }

    /**
     * @return the entry paths of the resource files (never <code>null</code> but can be empty)
     */
    public String[] getResourcePaths() {
        final DataServiceEntry[] entries = getResources();

        if ( entries.length == 0 ) {
            return DataServiceEntry.NO_PATHS;
        }

        return DataServiceEntry.getPaths( entries );
    }

    /**
     * @return the resource file entries (never <code>null</code> but can be empty)
     */
    public DataServiceEntry[] getResources() {
        if ( this.resources.isEmpty() ) {
            return ConnectionEntry.NO_DATA_SOURCES;
        }

        return this.resources.toArray( new DataServiceEntry[ this.resources.size() ] );
    }

    /**
     * @return the service VDB (can be <code>null</code>)
     */
    public ServiceVdbEntry getServiceVdb() {
        return this.serviceVdb;
    }

    /**
     * @return the archive path of the service VDB (can be <code>null</code> if there is no service VDB)
     */
    public String getServiceVdbPath() {
        if ( this.serviceVdb == null ) {
            return null;
        }

        return this.serviceVdb.getPath();
    }

    /**
     * @return the entry paths of the UDF entries (never <code>null</code> but can be empty)
     */
    public String[] getUdfPaths() {
        final DataServiceEntry[] entries = getUdfs();

        if ( entries.length == 0 ) {
            return DataServiceEntry.NO_PATHS;
        }

        return DataServiceEntry.getPaths( entries );
    }

    /**
     * @return the UDF file entries (never <code>null</code> but can be empty)
     */
    public DataServiceEntry[] getUdfs() {
        if ( this.udfs.isEmpty() ) {
            return ConnectionEntry.NO_DATA_SOURCES;
        }

        return this.udfs.toArray( new DataServiceEntry[ this.udfs.size() ] );
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
        if ( this.vdbs.isEmpty() ) {
            return VdbEntry.NO_VDBS;
        }

        return this.vdbs.toArray( new VdbEntry[ this.vdbs.size() ] );
    }

    /**
     * @param propName the name of the property whose existence is being requested (cannot be <code>null</code>)
     * @return <code>true</code> if the property exists
     */
    public boolean hasProperty( final String propName ) {
        return this.props.containsKey( Objects.requireNonNull( propName, "propname" ) );
    }

    /**
     * @param description the new description (can be <code>null</code> or empty)
     */
    public void setDescription( final String description ) {
        this.description = description;
    }

    /**
     * @param lastModified the new last modified date with the nanoseconds stripped off (can be <code>null</code> or empty)
     */
    public void setLastModified( final LocalDateTime lastModified ) {
        this.lastModified = ( ( lastModified == null ) ? lastModified : lastModified.withNano( 0 ) );
    }

    /**
     * @param modifiedBy the new name of the user who last modified the data service (can be <code>null</code> or empty)
     */
    public void setModifiedBy( final String modifiedBy ) {
        this.modifiedBy = modifiedBy;
    }

    /**
     * @param name the new name of the data service (cannot be <code>null</code>)
     */
    public void setName( final String name ) {
        this.name = Objects.requireNonNull( name, "name" ); //$NON-NLS-1$
    }

    /**
     * @param propName the name of the property being set (cannot be <code>null</code> or empty)
     * @param propValue the new value (can be <code>null</code> or empty if removing a property)
     * @throws RuntimeException if the property name is <code>null</code> or empty
     */
    public void setProperty( final String propName,
                             final String propValue ) {
        if ( StringUtil.isBlank( propName ) ) {
            throw new RuntimeException( TeiidI18n.propertyNameIsBlank.text() );
        }

        if ( ( propValue == null ) || propValue.isEmpty() ) {
            this.props.remove( propName );
        } else {
            this.props.put( propName, propValue );
        }
    }

    /**
     * @param serviceVdb the data service's service VDB (cannot be <code>null</code>)
     */
    public void setServiceVdb( final ServiceVdbEntry serviceVdb ) {
        this.serviceVdb = Objects.requireNonNull( serviceVdb, "serviceVdb" );
    }

}
