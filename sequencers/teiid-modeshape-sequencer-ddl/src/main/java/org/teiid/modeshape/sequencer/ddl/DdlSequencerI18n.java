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

import org.modeshape.common.i18n.I18n;

/**
 * The internationalized string constants.
 */
public final class DdlSequencerI18n {

    public static I18n sequencerTaskName;
    public static I18n errorSequencingDdlContent;
    public static I18n errorParsingDdlContent;
    public static I18n unknownCreateStatement;
    public static I18n unusedTokensDiscovered;
    public static I18n unusedTokensParsingColumnsAndConstraints;
    public static I18n unusedTokensParsingColumnDefinition;
    public static I18n alterTableOptionNotFound;
    public static I18n unusedTokensParsingCreateIndex;
    public static I18n missingReturnTypeForFunction;
    public static I18n unsupportedProcedureParameterDeclaration;
    public static I18n errorInstantiatingParserForGrammarUsingDefaultClasspath;
    public static I18n errorInstantiatingParserForGrammarClasspath;
    public static I18n ddlNotScoredByParsers;
    public static I18n unknownParser;

    private DdlSequencerI18n() {
    }

    static {
        try {
            I18n.initialize(DdlSequencerI18n.class);
        } catch (final Exception err) {
            // CHECKSTYLE IGNORE check FOR NEXT 2 LINES
            System.err.println(err);
        }
    }
}
