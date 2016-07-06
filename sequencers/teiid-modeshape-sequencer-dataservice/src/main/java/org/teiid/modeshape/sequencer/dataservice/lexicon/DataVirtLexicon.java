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

        /**
         * The name of the node type.
         */
        String NODE_TYPE = PREFIX + ":dataService";

        /**
         * The name of the property whose value contains the reference to the service VDB.
         */
        String SERVICE_VDB = PREFIX + ":serviceVdb";

        /**
         * The name of the multi-valued property whose value contains the references to its contained VDBs.
         */
        String VDBS = PREFIX + ":vdbs";

    }
    
    /**
     * JCR identifiers relating to the Datasource.
     */
    public interface Datasource {

        /**
         * The name of the node type.
         */
        String NODE_TYPE = PREFIX + ":dataSource";

        /**
         * The name of the property whose value is the type of the data source.
         */
        String TYPE = PREFIX + ":type";

        /**
         * The name of the property whose value is the JNDI name of the data source.
         */
        String JNDI_NAME = PREFIX + ":jndiName";

        /**
         * The name of the property whose value is the Java class name of the driver.
         */
        String CLASS_NAME = PREFIX + ":className";

        /**
         * The name of the multi-valued property whose value contains the references to its driver files.
         */
        String DRIVERS = PREFIX + ":drivers";

    }
    
    public interface DataserviceVdb {

        /**
         * The name of the node type.
         */
        String NODE_TYPE = PREFIX + ":dataServiceVdb";

        /**
         * The name of the property whose value contains the reference to an import VDB.
         */
        String VDB = PREFIX + ":vdb";

        /**
         * The name of the property whose value contains the reference to its data source.
         */
        String DATA_SOURCE = PREFIX + ":dataSource";

    }
    
    public interface DatasourceDriver {

        /**
         * The name of the node type.
         */
        String NODE_TYPE = PREFIX + ":driver";

    }
    
    /**
     * Manifest IDs for the Dataservice archive.
     */
    public interface ManifestIds {

        /**
         * The XML tag for the data service element.
         */
        String DATASERVICE = "dataservice";

        /**
         * The XML tag for the data source element.
         */
        String DATASOURCE = "datasource";

        /**
         * The XML tag for the driver element.
         */
        String DRIVER = "driver";

        /**
         * The XML tag for the import VDB element.
         */
        String IMPORT_VDB = "import-vdb";

        /**
         * The name of the XML attribute containing the archive resource's path.
         */
        String PATH = "path";

        /**
         * The XML tag for the service VDB element.
         */
        String SERVICE_VDB = "service-vdb";
    }

    /**
     * XML IDs for datasource.tds files.
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
