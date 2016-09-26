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

package org.teiid.modeshape.sequencer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.jcr.Node;
import org.teiid.modeshape.util.StringUtil;

/**
 * A collection of {@link Exporter#execute(Node, Options)} options. If an option is not found, the default value will be used.
 */
public class Options {

    /**
     * The default option value for the number of spaces for each indent level of the output. Value is {@value}.
     */
    public static final int DEFAULT_INDENT_AMOUNT = 4;

    /**
     * The default option value for if pretty printing the output is desired. Value is {@value}.
     */
    public static final boolean DEFAULT_PRETTY_PRINT = true;

    /**
     * A default property filter that filters out properties of these namespaces:
     * <p>
     * <ul>
     * <li>jcr</li>
     * <li>jdbc</li>
     * <li>med</li>
     * <li>mix</li>
     * <li>mmcore</li>
     * <li>mode</li>
     * <li>nt</li>
     * <li>relational</li>
     * <li>transformation</li>
     * <li>vdb</li>
     * <li>xmi</li>
     * </ul>
     *
     * @see PropertyFilter
     */
    public static final PropertyFilter DEFAULT_PROPERTY_FILTER = propertyName -> !propertyName.startsWith( "dv:" )
                                                                                 && !propertyName.startsWith( "jcr:" )
                                                                                 && !propertyName.startsWith( "jdbc:" )
                                                                                 && !propertyName.startsWith( "med:" )
                                                                                 && !propertyName.startsWith( "mix:" )
                                                                                 && !propertyName.startsWith( "mmcore:" )
                                                                                 && !propertyName.startsWith( "mode:" )
                                                                                 && !propertyName.startsWith( "nt:" )
                                                                                 && !propertyName.startsWith( "relational:" )
                                                                                 && !propertyName.startsWith( "transformation:" )
                                                                                 && !propertyName.startsWith( "vdb:" )
                                                                                 && !propertyName.startsWith( "xmi:" );

    @SuppressWarnings( "serial" )
    private static final Map< String, Object > DEFAULTS = Collections.unmodifiableMap( new HashMap< String, Object >() {
        {
            put( Options.INDENT_AMOUNT_PROPERTY, Options.DEFAULT_INDENT_AMOUNT );
            put( Options.PRETTY_PRINT_PROPERTY, Options.DEFAULT_PRETTY_PRINT );
            put( Options.PROPERTY_FILTER_PROPERTY, Options.DEFAULT_PROPERTY_FILTER );
        }
    } );

    /**
     * The option name for the number of spaces for each indent level of the output.
     */
    public static final String INDENT_AMOUNT_PROPERTY = "indent-amount";

    /**
     * The option name for if pretty printing the output is desired.
     */
    public static final String PRETTY_PRINT_PROPERTY = "pretty-print";

    /**
     * The option name for the properties filter.
     *
     * @see Options.PropertyFilter
     */
    public static final String PROPERTY_FILTER_PROPERTY = "property-filter";

    private final Map< String, Object > options = new HashMap<>( DEFAULTS );

    /**
     * @param name
     *        the option name (cannot be <code>null</code> or empty)
     * @return the value (can be <code>null</code> if not found)
     * @throws RuntimeException
     *         if option name is <code>null</code> or empty
     */
    public Object get( final String name ) throws RuntimeException {
        final Object value = this.options.get( StringUtil.requireNonEmpty( name, "name" ) );

        if ( value == null ) {
            return DEFAULTS.get( name );
        }

        return value;
    }

    /**
     * @param name
     *        the option name (cannot be <code>null</code> or empty)
     * @param defaultValue
     *        the value to return if the option is not found (cannot be <code>null</code>)
     * @return the value (never <code>null</code>)
     * @throws RuntimeException
     *         if option name or default value is <code>null</code> or empty
     */
    public Object get( final String name,
                       final Object defaultValue ) throws RuntimeException {
        final Object value = this.options.get( StringUtil.requireNonEmpty( name, "name" ) );
        return ( ( value == null ) ? Objects.requireNonNull( defaultValue, "defaultValue" ) : value );
    }

    /**
     * Removes the named option if the value is <code>null</code> or an empty <code>String</code>.
     *
     * @param name
     *        the option name (cannot be <code>null</code> or empty)
     * @param value
     *        the current value (can be <code>null</code>)
     * @return the previous value of the option (can be <code>null</code>)
     */
    public Object set( final String name,
                       final Object value ) {
        StringUtil.requireNonEmpty( name, "name" );

        if ( ( value == null )
             || ( ( value instanceof String )
                  && ( ( String )value ).isEmpty() ) ) {
            return this.options.remove( name );
        }

        return this.options.put( name, value );
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.options.toString();
    }

    /**
     * Filters the properties that will be exported.
     */
    @FunctionalInterface
    public interface PropertyFilter {

        /**
         * @param propertyName
         *        the name of the property being checked (cannot be <code>null</code> or empty)
         * @return <code>true</code> if the property should be exported
         */
        boolean accept( final String propertyName );

    }

}
