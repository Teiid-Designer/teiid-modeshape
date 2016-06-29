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
package org.teiid.modeshape.sequencer.dataservice.lexicon;

import static org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon.Namespace.PREFIX;

/**
 * Constants associated with the DataVirtualization namespace
 */
public interface DataVirtLexicon {

    /**
     * The URI and prefix constants of the DV namespace.
     */
    public interface Namespace {
        String PREFIX = "dv";
        String URI = "http://www.metamatrix.com/metamodels/DataVirtualization";
    }

    /**
     * JCR identifiers relating to the Dataservice.
     */
    public interface Dataservice {
        String DATASERVICE = PREFIX + ":dataService";
        String SERVICE_VDB = PREFIX + ":service-vdb";
        String VDBS = PREFIX + ":vdbs";
        String DATASOURCES = PREFIX + ":datasources";
        String DRIVERS = PREFIX + ":drivers";
    }

    /**
     * JCR identifiers relating to the Datasource.
     */
    public interface Datasource {
        String DATASOURCE = PREFIX + ":dataSource";
        String JDBC = PREFIX + ":jdbc";
        String PREVIEW = PREFIX + ":preview";
        String PROFILE_NAME = PREFIX + ":profileName";
        String JNDI_NAME = PREFIX + ":jndiName";
        String DRIVER_NAME = PREFIX + ":driverName";
        String CLASS_NAME = PREFIX + ":className";
    }
    
    /**
     * Constants associated with the DV namespace that identify manifest identifiers.
     */
    public interface ManifestIds {
        String DATASERVICE = "dataservice";
        String SERVICE_VDB = "service-vdb";
        String VDBS = "vdbs";
        String DATASOURCES = "datasources";
        String DRIVERS = "drivers";
        String DATASOURCE_SET = "dataSourceSet";
        String DATASOURCE = "dataSource";
        String PROPERTY = "property";
    }

}
