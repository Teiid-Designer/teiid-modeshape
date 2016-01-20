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
package org.teiid.modeshape.sequencer.vdb.lexicon;

import static org.teiid.modeshape.sequencer.vdb.lexicon.TransformLexicon.Namespace.PREFIX;

/**
 * Constants associated with the transformation namespace used in reading XMI models and writing JCR nodes.
 */
public interface TransformLexicon {

    /**
     * The URI and prefix constants of the transformaion namespace.
     */
    public interface Namespace {
        String PREFIX = "transform";
        String URI = "http://www.metamatrix.com/metamodels/Transformation";
    }

    /**
     * Constants associated with the transformation namespace that identify XMI model identifiers.
     */
    public interface ModelId {
        String ALIAS = "alias";
        String ALIASED_OBJECT = "aliasedObject";
        String ALIASES = "aliases";
        String DELETE_ALLOWED = "deleteAllowed";
        String DELETE_SQL = "deleteSql";
        String DELETE_SQL_DEFAULT = "deleteSql";
        String HELPER = "helper";
        String HREF = "href";
        String INPUTS = "inputs";
        String INSERT_ALLOWED = "insertAllowed";
        String INSERT_SQL = "insertSql";
        String INSERT_SQL_DEFAULT = "insertSql";
        String NESTED = "nested";
        String OUTPUTS = "outputs";
        String SELECT_SQL = "selectSql";
        String TARGET = "target";
        String TRANSFORMATION_CONTAINER = "TransformationContainer";
        String TRANSFORMATION_MAPPINGS = "transformationMappings";
        String UPDATE_ALLOWED = "updateAllowed";
        String UPDATE_SQL = "updateSql";
        String UPDATE_SQL_DEFAULT = "updateSql";
    }

    /**
     * JCR identifiers relating to the transformation namespace.
     */
    public interface JcrId {
        String ALIAS = PREFIX + ":alias";
        String DELETE_ALLOWED = PREFIX + ":deleteAllowed";
        String DELETE_SQL = PREFIX + ":deleteSql";
        String DELETE_SQL_DEFAULT = PREFIX + ":deleteSqlDefault";
        String INPUTS = PREFIX + ":inputs";
        String INSERT_ALLOWED = PREFIX + ":insertAllowed";
        String INSERT_SQL = PREFIX + ":insertSql";
        String INSERT_SQL_DEFAULT = PREFIX + ":insertSqlDefault";
        String OUTPUT_LOCKED = PREFIX + ":outputLocked";
        String SELECT_SQL = PREFIX + ":selectSql";
        String TRANSFORMED = PREFIX + ":transformed";
        String TRANSFORMED_FROM = PREFIX + ":transformedFrom";
        String TRANSFORMED_FROM_HREFS = PREFIX + ":transformedFromHrefs";
        String TRANSFORMED_FROM_XMI_UUIDS = PREFIX + ":transformedFromXmiUuids";
        String TRANSFORMED_FROM_NAMES = PREFIX + ":transformedFromNames";
        String UPDATE_ALLOWED = PREFIX + ":updateAllowed";
        String UPDATE_SQL = PREFIX + ":updateSql";
        String UPDATE_SQL_DEFAULT = PREFIX + ":updateSqlDefault";
        String WITH_SQL = PREFIX + ":withSql";
    }
}
