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
import java.util.Properties;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.StringUtil;

/**
 * Represents a connection definition.
 */
public class Connection {

    static final Logger LOGGER = Logger.getLogger( Connection.class );

    private String className;

    private String description;
    private String driverName;
    private String jndiName;
    private String name;
    private final Properties props = new Properties();
    private Type type;

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

        final Connection that = ( Connection )obj;
        return Objects.equals( this.name, that.name ) && Objects.equals( this.description, that.description )
               && Objects.equals( this.type, that.type ) && Objects.equals( this.jndiName, that.jndiName )
               && Objects.equals( this.driverName, that.driverName ) && Objects.equals( this.className, that.className )
               && Objects.equals( this.props, that.props );
    }

    /**
     * @return the driver class name (can be <code>null</code> or empty)
     */
    public String getClassName() {
        return this.className;
    }

    /**
     * @return the data service description (can be <code>null</code> or empty)
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * @return the driver name (can be <code>null</code> or empty)
     */
    public String getDriverName() {
        return this.driverName;
    }

    /**
     * @return the JNDI name (can be <code>null</code> or empty)
     */
    public String getJndiName() {
        return this.jndiName;
    }

    /**
     * @return the connection name (can be <code>null</code> or empty)
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
     * @return the connection type (can be <code>null</code>)
     */
    public Type getType() {
        return this.type;
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Objects.hash( this.name, this.description, this.type, this.jndiName, this.driverName, this.className, this.props );
    }

    /**
     * @param propName the name of the property whose existence is being requested (cannot be <code>null</code>)
     * @return <code>true</code> if the property exists
     */
    public boolean hasProperty( final String propName ) {
        return this.props.containsKey( Objects.requireNonNull( propName, "propname" ) );
    }

    /**
     * @param className the new name of the driver class (can be <code>null</code> or empty)
     */
    public void setClassName( final String className ) {
        this.className = className;
    }

    /**
     * @param description the new description (can be <code>null</code> or empty)
     */
    public void setDescription( final String description ) {
        this.description = description;
    }

    /**
     * @param driverName the new driver file name (can be <code>null</code> or empty)
     */
    public void setDriverName( final String driverName ) {
        this.driverName = driverName;
    }

    /**
     * @param jndiName the new JNDI name (can be <code>null</code> or empty)
     */
    public void setJndiName( final String jndiName ) {
        this.jndiName = jndiName;
    }

    /**
     * @param name the new name of the connection (can be <code>null</code> or empty)
     */
    public void setName( final String name ) {
        this.name = name;
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
     * @param type the new type (can be <code>null</code>)
     */
    public void setType( final Type type ) {
        this.type = type;
    }

    /**
     * Represents a connection type.
     */
    public enum Type {

        /**
         * A JDBC connection type.
         */
        JDBC,

        /**
         * A resource adapter connection.
         */
        RESOURCE
    }

}
