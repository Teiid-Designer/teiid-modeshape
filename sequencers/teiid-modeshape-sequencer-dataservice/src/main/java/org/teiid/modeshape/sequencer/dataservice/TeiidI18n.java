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

import org.modeshape.common.i18n.I18n;

/**
 * The internationalized string constants for the <code>org.teiid.modeshape.sequencer.vdb*</code> packages.
 */
public final class TeiidI18n {

    public static I18n errorReadingDataserviceFile;
    public static I18n errorReadingDatasourceFile;
    public static I18n fileSequencingError;
    public static I18n missingDataServiceManifestFile;
    public static I18n noDatasourceFound;
    public static I18n unexpectedDeployPolicy;
    public static I18n vdbSequencingError;

    public static I18n dataServiceSchemaError;
    public static I18n dataserviceVdbAlreadySet;
    public static I18n dataserviceVdbSequencingError;
    public static I18n dataServiceXmlDeclarationNotParsed;
    public static I18n dataServiceXmlEntitySkipped;
    public static I18n dataSourceDriverSequencingError;
    public static I18n dataSourceNotSequenced;
    public static I18n dataSourceParserError;
    public static I18n dataSourceReadProblem;
    public static I18n dataSourceSchemaError;
    public static I18n dataSourceSequencingError;
    public static I18n dataSourceXmlDeclarationNotParsed;
    public static I18n dataSourceXmlEntitySkipped;
    public static I18n driverDataSourceNotFound;
    public static I18n importVdbNotSequenced;
    public static I18n pathAttributeNotFound;
    public static I18n propertyNameIsBlank;
    public static I18n serviceVdbNotFound;
    public static I18n serviceVdbNotSequenced;
    public static I18n unhandledDataServiceEndElement;
    public static I18n unhandledDataServiceStartElement;
    public static I18n unhandledDatasoureElement;
    public static I18n unhandledDatasoureEndElement;
    public static I18n unhandledDatasoureEventType;
    public static I18n unhandledDatasoureStartElement;
    public static I18n unhandledImportVdbElement;
    public static I18n unhandledImportVdbEndElement;
    public static I18n unhandledImportVdbEventType;
    public static I18n unhandledVdbFile;

    private TeiidI18n() {
    }

    static {
        try {
            I18n.initialize(TeiidI18n.class);
        } catch (final Exception err) {
            // CHECKSTYLE IGNORE check FOR NEXT 1 LINES
            System.err.println(err);
        }
    }
}
