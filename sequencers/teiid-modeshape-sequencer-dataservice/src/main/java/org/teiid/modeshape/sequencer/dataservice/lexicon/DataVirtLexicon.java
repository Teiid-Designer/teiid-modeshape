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
        String SERVICE_VDB = PREFIX + ":serviceVdb";
    }
    
    /**
     * Manifest ids for the Dataservice.
     */
	public interface ManifestIds {
        String DATASERVICE = "dataservice";
        String SERVICE_VDB = "service-vdb";
	}
    
    /**
     * JCR identifiers relating to the Datasource.
     */
    public interface Datasource {
        String DATASOURCE = PREFIX + ":dataSource";
        String TYPE = PREFIX + ":type";
        String JNDI_NAME = PREFIX + ":jndiName";
        String DRIVER_NAME = PREFIX + ":driverName";
        String CLASS_NAME = PREFIX + ":className";
    }

    /**
     * xml ids for datasource.tds files.
     */
    public interface DatasourceXml {
        String DATASOURCE_SET = "dataSourceSet";
        String DATASOURCE = "dataSource";
        String PROPERTY = "property";
        String TYPE_ATTR = "type";
        String NAME_ATTR = "name";
        String JNDI_NAME_PROP = "jndiName";
        String DRIVER_NAME_PROP = "driverName";
        String CLASSNAME_PROP = "className";
    }
   
}
