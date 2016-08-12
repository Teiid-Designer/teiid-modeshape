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
package org.teiid.modeshape.sequencer.vdb;

import org.modeshape.common.i18n.I18n;

/**
 * The internationalized string constants for the <code>org.teiid.modeshape.sequencer.vdb*</code> packages.
 */
public final class TeiidI18n {
    public static I18n uuidNotValid;

    public static I18n modelSequencerTaskName;
    public static I18n vdbSequencerTaskName;
    public static I18n errorSequencingModelContent;
    public static I18n errorSequencingVdbContent;

    public static I18n readingEcoreFile;
    public static I18n errorReadingEcoreFile;
    public static I18n errorWritingCndFile;

    public static I18n errorReadingVdbFile;
    public static I18n errorClosingVdbFile;

    public static I18n invalidModelNodeType;
    public static I18n invalidNumberOfPropertyAttributes;
    public static I18n invalidVdbModelNodeType;
    public static I18n missingDataRoleName;
    public static I18n missingEntryPath;
    public static I18n missingImportVdbNameOrVersion;
    public static I18n missingModelNodeThatReferencesDdlFile;
    public static I18n missingPermissionResourceName;
    public static I18n missingPropertyNameOrValue;
    public static I18n missingTranslatorNameOrType;
    public static I18n missingVdbManifest;
    public static I18n missingVdbModel;
    public static I18n missingVdbName;

    public static I18n illegalUnresolvedReference;
    public static I18n namespaceUriNotFoundInRegistry;
    public static I18n invalidVdbVersion;

    public static I18n errorReadingMedMetaclassMappings;
    
    public static I18n ddlFileMissing;

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
