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
package org.teiid.modeshape.sequencer.ddl.datatype;

import org.teiid.modeshape.sequencer.ddl.DdlConstants;

/**
 * A representation of SQL data types.
 */
public class DataType {

    public static final long DEFAULT_LENGTH = -1;
    public static final int DEFAULT_PRECISION = -1;
    public static final int DEFAULT_SCALE = -1;
    public static final int DEFAULT_ARRAY_DIMENSIONS = 0;

    private String name;
    private long length = DEFAULT_LENGTH;
    private int precision = DEFAULT_PRECISION;
    private int scale = DEFAULT_SCALE;
    private int arrayDimensions = DEFAULT_ARRAY_DIMENSIONS;
    private boolean autoIncrement = false;
    private boolean notNull = false;

    /**
     * The statement source.
     */
    private String source = "";

    public DataType() {
        super();
    }

    public DataType( String theName ) {
        super();
        this.name = theName;
    }

    public DataType( String name,
                     int length ) {
        super();
        this.name = name;
        this.length = length;
    }

    public DataType( String name,
                     int precision,
                     int scale ) {
        super();
        this.name = name;
        this.precision = precision;
        this.scale = scale;
    }

    public String getName() {
        return this.name;
    }

    public void setName( String value ) {
        this.name = value;
    }

    public void setLength( long value ) {
        this.length = value;
    }

    public long getLength() {
        return this.length;
    }

    public void setPrecision( int value ) {
        this.precision = value;
    }

    public int getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }

    public void setScale( int value ) {
        this.scale = value;
    }

    /**
     * @return array dimensions
     */
    public int getArrayDimensions() {
        return arrayDimensions;
    }

    /**
     * @param arrayDimensions
     */
    public void setArrayDimensions(int arrayDimensions) {
        this.arrayDimensions = arrayDimensions;
    }
    /**
     * @return auto increment
     */
    public boolean isAutoIncrement() {
        return this.autoIncrement;
    }

    /**
     * @param autoIncrement the autoIncrement flag to set
     */
    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    /**
     * @return not null flag
     */
    public boolean isNotNull() {
        return this.notNull;
    }

    /**
     * @param notNull the notNull flag to set
     */
    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(100);
        result.append("DataType()").append(" ").append(name);

        return result.toString();
    }

    /**
     * @param source
     */
    public void setSource( String source ) {
        if (source == null) {
            source = "";
        }
        this.source = source;
    }

    /**
     * @return source string
     */
    public String getSource() {
        return source;
    }

    /**
     * @param addSpaceBefore
     * @param value
     */
    public void appendSource( boolean addSpaceBefore,
                              String value ) {
        if (addSpaceBefore) {
            this.source = this.source + DdlConstants.SPACE;
        }
        this.source = this.source + value;
    }

    /**
     * @param addSpaceBefore
     * @param value
     * @param additionalStrs
     */
    public void appendSource( boolean addSpaceBefore,
                              String value,
                              String... additionalStrs ) {
        if (addSpaceBefore) {
            this.source = this.source + DdlConstants.SPACE;
        }
        this.source = this.source + value;
    }

}
