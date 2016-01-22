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
package org.teiid.modeshape.sequencer.ddl;

import org.modeshape.common.text.ParsingException;
import org.modeshape.common.text.Position;
import org.teiid.modeshape.sequencer.ddl.DdlConstants.Problems;

/**
 * A special form of {@link Problems} that is also a {@link ParsingException}.
 */
public class DdlParserProblem extends ParsingException implements DdlConstants.Problems {
    private static final long serialVersionUID = 2010539270968770893L;

    private int level = OK;
    private String unusedSource;

    public DdlParserProblem( Position position ) {
        super(position);

    }

    public DdlParserProblem( int level,
                             Position position,
                             String message,
                             Throwable cause ) {
        super(position, message, cause);
        this.level = level;
    }

    public DdlParserProblem( int level,
                             Position position,
                             String message ) {
        super(position, message);
        this.level = level;
    }

    /**
     * @return the unused statement string
     */
    public String getUnusedSource() {
        return this.unusedSource;
    }

    public void setUnusedSource( String unusedSource ) {
        if (unusedSource == null) {
            unusedSource = "";
        }
        this.unusedSource = unusedSource;
    }

    public void appendSource( boolean addSpaceBefore,
                              String value ) {
        if (addSpaceBefore) {
            this.unusedSource = this.unusedSource + DdlConstants.SPACE;
        }
        this.unusedSource = this.unusedSource + value;
    }

    public void appendSource( boolean addSpaceBefore,
                              String value,
                              String... additionalStrs ) {
        if (addSpaceBefore) {
            this.unusedSource = this.unusedSource + DdlConstants.SPACE;
        }
        this.unusedSource = this.unusedSource + value;
    }

    public int getLevel() {
        return level;
    }

    /**
     * @param level Sets level to the specified value.
     */
    public void setLevel( int level ) {
        this.level = level;
    }

    @Override
    public String toString() {
        if (this.level == WARNING) {
            return ("WARNING: " + super.toString());
        } else if (this.level == ERROR) {
            return ("ERROR: " + super.toString());
        }

        return super.toString();
    }

}
