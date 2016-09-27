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

import javax.jcr.Node;

/**
 * The result of an {@link Exporter#execute(Node, Options) export} operation. A result object can be iterated over its data keys.
 */
public interface Result extends Iterable< String > {

    /**
     * @param key
     *        the type of data being requested (cannot be <code>null</code>)
     * @return the information, with the specified type, relating to the operation results (never <code>null</code>)
     */
    Object getData( final String key );

    /**
     * An unsuccessful export may not have a caught exception.
     * 
     * @return the error caught during the export operation (can be <code>null</code>)
     * @see #getErrorMessage()
     * @see #wasSuccessful()
     */
    Exception getError();

    /**
     * If the export was <strong>not</code> successful the error message will always have a value.
     * 
     * @return the error message (<code>null</code> when export was successful )
     * @see #wasSuccessful()
     */
    String getErrorMessage();

    /**
     * @return the options used during the export (never <code>null</code> but can be empty if all default values were used)
     */
    Options getOptions();

    /**
     * @return the results returned by the operation (can be <code>null</code>)
     */
    Object getOutcome();

    /**
     * @return the type returned by the {@link #getOutcome()} method (will be <code>null</code> until outcome is set)
     */
    Class< ? > getType();

    /**
     * @return <code>true</code> if there is no error and no error message
     */
    boolean wasSuccessful();

}
