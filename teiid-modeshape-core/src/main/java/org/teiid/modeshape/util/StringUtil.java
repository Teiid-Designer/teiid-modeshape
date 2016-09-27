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

package org.teiid.modeshape.util;

import java.util.Objects;

/**
 * Utilities for {@link String strings}.
 */
public class StringUtil {

    /**
     * @param stringBeingTested the object being tested (cannot be <code>null</code> or empty)
     * @param variableName the name of the variable being tested (cannot be <code>null</code> or empty)
     * @return the object being tested (if non-empty)
     * @throws RuntimeException if object is <code>null</code> or empty
     */
    public static String requireNonEmpty( final String stringBeingTested,
                                          final String variableName ) throws RuntimeException {
        if ( Objects.requireNonNull( stringBeingTested, "variableName" ).isEmpty() ) {
            throw new RuntimeException( String.format( "The string parameter {0} was null or empty", variableName ) );
        }

        return stringBeingTested;
    }

    /**
     * Don't allow construction outside of this class.
     */
    private StringUtil() {
        // nothing to do
    }

}
