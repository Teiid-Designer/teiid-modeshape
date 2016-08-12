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
     * JCR identifiers relating to a data service archive. Child nodes should be of type
     * {@link DataVirtLexicon.DataServiceEntry#NODE_TYPE}.
     */
    interface DataService {

        /**
         * The name of the data service archive node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":dataService";

        /**
         * The name of the property whose value contains a description of the data service archive. Value is {@value}.
         */
        String DESCRIPTION = PREFIX + ":description";

        /**
         * The name of the property whose value contains the date the archive was last modified. Value is {@value}.
         */
        String LAST_MODIFIED = PREFIX + ":lastModified";

        /**
         * The name of the property whose value contains the name of the user who last modified the archive. Value is {@value}.
         */
        String MODIFIED_BY = PREFIX + ":modifiedBy";

        /**
         * The name of the property whose value contains the data service name. Value is {@value}.
         */
        String NAME = PREFIX + ":dataServiceName";

    }

    /**
     * Manifest IDs for the data service archive manifest.
     */
    interface DataServiceManifestId {

        /**
         * The XML element tag for the collection of connection files. Value is {@value}.
         */
        String CONNECTIONS = "connections";

        /**
         * The XML element tag for a connection file. Value is {@value}.
         */
        String CONNECTION_FILE = "connection-file";

        /**
         * The XML element tag for the root data service. Value is {@value}.
         */
        String DATASERVICE = "dataservice";

        /**
         * The XML element tag for a DDL metadata file. Value is {@value}.
         */
        String DDL_FILE = "ddl-file";

        /**
         * The XML element tag for the collection of VDB dependencies. Value is {@value}.
         */
        String DEPENDENCIES = "dependencies";

        /**
         * The XML element tag for the data service description. Value is {@value}.
         */
        String DESCRIPTION = "description";

        /**
         * The XML element tag for a driver file. Value is {@value}.
         */
        String DRIVER_FILE = "driver-file";

        /**
         * The XML element tag for the collection of driver files. Value is {@value}.
         */
        String DRIVERS = "drivers";

        /**
         * The name of the XML attribute containing the name of the connection JNDI name. Value is {@value}.
         */
        String JNDI_NAME = "jndiName";

        /**
         * The XML element tag for the data service's last modified date. Value is {@value}.
         */
        String LAST_MODIFIED = "lastModified";

        /**
         * The XML element tag for the collection of metadata files. Value is {@value}.
         */
        String METADATA = "metadata";

        /**
         * The XML element tag for the name of the user who last modified the data service. Value is {@value}.
         */
        String MODIFIED_BY = "modifiedBy";

        /**
         * The name of the XML attribute containing the name of a generic property or an artifact. Value is {@value}.
         */
        String NAME = "name";

        /**
         * The name of the XML attribute containing the archive path of the resource. Value is {@value}.
         */
        String PATH = "path";

        /**
         * The XML element tag for a generic data service property. Value is {@value}.
         */
        String PROPERTY = "property";

        /**
         * The name of the XML attribute containing the publishing policy for the resource. Value is {@value}.
         */
        String PUBLISH = "publish";

        /**
         * The XML element tag for a miscellaneous resource file. Value is {@value}.
         */
        String RESOURCE_FILE = "resource-file";

        /**
         * The XML element tag for the collection of resource files. Value is {@value}.
         */
        String RESOURCES = "resources";

        /**
         * The XML element tag for a service VDB. Value is {@value}.
         */
        String SERVICE_VDB = "service-vdb-file";

        /**
         * The XML element tag for a UDF file. Value is {@value}.
         */
        String UDF_FILE = "udf-file";

        /**
         * The XML element tag for the collection of UDF files. Value is {@value}.
         */
        String UDFS = "udfs";

        /**
         * The XML element tag for a VDB dependency. Value is {@value}.
         */
        String VDB_FILE = "vdb-file";

        /**
         * The name of the XML attribute containing the VDB name. Value is {@value}.
         */
        String VDB_NAME = "vdbName";

        /**
         * The name of the XML attribute containing the VDB version. Value is {@value}.
         */
        String VDB_VERSION = "vdbVersion";

        /**
         * The XML element tag for a collection of VDB files not associated with the service VDB. Value is {@value}.
         */
        String VDBS = "vdbs";

    }

    /**
     * JCR identifiers relating to the data service entry node type.
     */
    interface DataServiceEntry {

        /**
         * The name of the node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":dataServiceEntry";

        /**
         * The name of the property whose value is the archive path of the entry. Value is {@value}.
         */
        String PATH = PREFIX + ":entryPath";

        /**
         * The name of the property whose value is a reference to a resource or a node that can export a resource. Value is
         * {@value}.
         */
        String SOURCE_RESOURCE = PREFIX + ":sourceResource";

    }

    /**
     * JCR identifiers relating to the connection.
     */
    interface Connection {

        /**
         * The name of the property whose value is the Java class name of the driver. Value is {@value}.
         */
        String CLASS_NAME = PREFIX + ":className";

        /**
         * The name of the property whose value is the name of the file where the driver class is found. Value is {@value}.
         */
        String DRIVER_NAME = PREFIX + ":driverName";

        /**
         * The name of the property whose value is the JNDI name of the connection. Value is {@value}.
         */
        String JNDI_NAME = PREFIX + ":jndiName";

        /**
         * The name of the connection node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":connection";

        /**
         * The name of the property whose value is the type of the connection. Value is {@value}.
         */
        String TYPE = PREFIX + ":type";

    }

    /**
     * JCR identifiers relating to a data service's entry for a connection.
     */
    interface ConnectionEntry {

        /**
         * The name of the connection entry node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":connectionEntry";

        /**
         * The name of the property whose value is the archive path of the entry. Value is {@value}.
         */
        String PATH = DataServiceEntry.PATH;

        /**
         * The name of the property whose value is the reference of the connection resource. Value is {@value}.
         */
        String CONNECTION_REF = DataServiceEntry.SOURCE_RESOURCE;

        /**
         * The name of the property whose value is the JNDI name of the connection. Value is {@value}.
         */
        String JDNI_NAME = PREFIX + ":jndiName";

    }

    /**
     * XML IDs for connection <code>-connection.xml</code> files.
     */
    interface ConnectionXmlId {

        /**
         * The XML element tag for the driver class name. Value is {@value}.
         */
        String CLASSNAME = "driver-class";

        /**
         * The XML element tag for the connection description. Value is {@value}.
         */
        String DESCRIPTION = "description";

        /**
         * The XML element tag for the driver file name. Value is {@value}.
         */
        String DRIVER_NAME = "driver-name";

        /**
         * The XML root element tag for a JDBC connection. Value is {@value}.
         */
        String JDBC_CONNECTION = "jdbc-connection";

        /**
         * The XML element tag for the JNDI name. Value is {@value}.
         */
        String JNDI_NAME = "jndi-name";

        /**
         * The XML attribute for the name of the connection. Value is {@value}.
         */
        String NAME_ATTR = "name";

        /**
         * The XML element tag for a generic property. Value is {@value}.
         */
        String PROPERTY = "property";

        /**
         * The XML root element tag for a resource adapter connection. Value is {@value}.
         */
        String RESOURCE_CONNECTION = "resource-connection";

    }

    /**
     * JCR identifiers relating to a data service entry for a driver file.
     */
    interface DriverEntry {

        /**
         * The name of the driver entry node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":driverEntry";

        /**
         * The name of the property whose value is the archive path of the entry. Value is {@value}.
         */
        String PATH = DataServiceEntry.PATH;

        /**
         * The name of the property whose value is the reference of the driver resource. Value is {@value}.
         */
        String DRIVER_REF = DataServiceEntry.SOURCE_RESOURCE;

    }

    /**
     * JCR identifiers relating to a driver file.
     */
    interface DriverFile {

        /**
         * The name of the driver file node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":driverFile";

    }

    /**
     * JCR identifiers relating to metadata files.
     */
    interface MetadaFile {

        /**
         * The name of the DDL metadata file node type. Value is {@value}.
         */
        String DDL_FILE_NODE_TYPE = PREFIX + ":ddlFile";

        /**
         * The name of the abstract metadata file node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":metadataFile";

    }

    /**
     * JCR identifiers relating to a data service entry for a metadata file.
     */
    interface MetadataEntry {

        /**
         * The name of the DDL file entry node type. Value is {@value}.
         */
        String DDL_FILE_NODE_TYPE = PREFIX + ":ddlEntry";

        /**
         * The name of the abstract metadata entry node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":metadataEntry";

        /**
         * The name of the property whose value is the path of the metadata entry. Value is {@value}.
         */
        String PATH = DataServiceEntry.PATH;

        /**
         * The name of the property whose value is the reference of the metadata file. Value is {@value}.
         */
        String METADATA_REF = DataServiceEntry.SOURCE_RESOURCE;

    }

    /**
     * The URI and prefix constants of the DV namespace.
     */
    interface Namespace {

        /**
         * The data virtualization namespace prefix. Value is {@value}.
         */
        String PREFIX = "dv";

        /**
         * The data virtualization namespace URI. Value is {@value}.
         */
        String URI = "http://www.jboss.org/dv/1.0";

    }

    /**
     * JCR identifiers relating to a data service entry for a resource file.
     */
    interface ResourceEntry {

        /**
         * The name of the resource file entry node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":resourceEntry";

        /**
         * The name of the UDF file entry node type. Value is {@value}.
         */
        String UDF_FILE_NODE_TYPE = PREFIX + ":udfEntry";

        /**
         * The name of the property whose value is the resource path of the entry. Value is {@value}.
         */
        String PATH = DataServiceEntry.PATH;

        /**
         * The name of the property whose value is the reference of the resource file. Value is {@value}.
         */
        String RESOURCE_REF = DataServiceEntry.SOURCE_RESOURCE;

    }

    /**
     * JCR identifiers relating to resource files.
     */
    interface ResourceFile {

        /**
         * The name of the generic resource file node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":resourceFile";

        /**
         * The name of the UDF file node type. Value is {@value}.
         */
        String UDF_FILE_NODE_TYPE = PREFIX + ":udfFile";

    }

    /**
     * JCR identifiers relating to a data service's entry for the service VDB.
     */
    interface ServiceVdbEntry {

        /**
         * The name of the service VDB entry node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":serviceVdbEntry";

        /**
         * The name of the property whose value is the archive path of the entry. Value is {@value}.
         */
        String PATH = DataServiceEntry.PATH;

        /**
         * The name of the property whose value is the reference of the driver resource. Value is {@value}.
         */
        String VDB_REF = DataServiceEntry.SOURCE_RESOURCE;

        /**
         * The name of the property whose value is the name of the VDB referenced by this entry. Value is {@value}.
         */
        String VDB_NAME = PREFIX + ":vdbName";

        /**
         * The name of the property whose value is the version of the VDB referenced by this entry. Value is {@value}.
         */
        String VDB_VERSION = PREFIX + ":vdbVersion";

    }

    /**
     * JCR identifiers relating to a data service's entry for VDBs that are dependencies of a service VDB.
     */
    interface VdbEntry {

        /**
         * The name of the import VDB entry node type. Value is {@value}.
         */
        String NODE_TYPE = PREFIX + ":vdbEntry";

        /**
         * The name of the property whose value is the archive path of the entry. Value is {@value}.
         */
        String PATH = DataServiceEntry.PATH;

        /**
         * The name of the property whose value is the reference of the driver resource. Value is {@value}.
         */
        String VDB_REF = DataServiceEntry.SOURCE_RESOURCE;

        /**
         * The name of the property whose value is the name of the VDB referenced by this entry. Value is {@value}.
         */
        String VDB_NAME = PREFIX + ":vdbName";

        /**
         * The name of the property whose value is the version of the VDB referenced by this entry. Value is {@value}.
         */
        String VDB_VERSION = PREFIX + ":vdbVersion";

    }

}
