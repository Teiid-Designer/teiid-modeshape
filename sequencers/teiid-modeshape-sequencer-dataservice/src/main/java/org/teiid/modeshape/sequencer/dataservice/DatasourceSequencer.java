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

import java.io.IOException;
import java.io.InputStream;

import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;

import org.modeshape.common.annotation.ThreadSafe;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.modeshape.jcr.api.sequencer.Sequencer.Context;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon.Datasource;
import org.teiid.modeshape.sequencer.vdb.TeiidI18n;
import org.teiid.modeshape.sequencer.vdb.VdbModel;

/**
 * A sequencer of Teiid XMI model files.
 */
@ThreadSafe
public class DatasourceSequencer extends Sequencer {

    private static final String[] DATASOURCE_FILE_EXTENSIONS = { ".tds" };
    private static final Logger LOGGER = Logger.getLogger(DatasourceSequencer.class);


    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#execute(javax.jcr.Property, javax.jcr.Node,
     *      org.modeshape.jcr.api.sequencer.Sequencer.Context)
     */
    @Override
    public boolean execute( final Property inputProperty,
                            final Node outputNode,
                            final Context context ) throws Exception {
        final Binary binaryValue = inputProperty.getBinary();
        CheckArg.isNotNull(binaryValue, "binary");

        try (InputStream datasourceStream = binaryValue.getStream()) {
        	DataserviceDatasource datasource = readDatasource(binaryValue, datasourceStream, outputNode, context);
            if (datasource == null) {
                throw new Exception("DatasourceSequencer.execute failed. The xml cannot be read.");
            }
        } catch (final Exception e) {
//            throw new RuntimeException(TeiidI18n.errorReadingDatasourceFile.text(inputProperty.getPath(), e.getMessage()), e);
            throw new RuntimeException("Error reading file");
        }
        return true;
    }
    
    /**
     * @param resourceName the name of the resource being checked (cannot be <code>null</code>)
     * @return <code>true</code> if the resource has a model file extension
     */
    public boolean hasDatasourceFileExtension( final String resourceName ) {
        for (final String extension : DATASOURCE_FILE_EXTENSIONS) {
            if (resourceName.endsWith(extension)) {
                return true;
            }
        }

        // not a model file
        return false;
    }

    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#initialize(javax.jcr.NamespaceRegistry,
     *      org.modeshape.jcr.api.nodetype.NodeTypeManager)
     */
    @Override
    public void initialize( final NamespaceRegistry registry,
                            final NodeTypeManager nodeTypeManager ) throws RepositoryException, IOException {
        LOGGER.debug("enter initialize");
//      registry.registerNamespace(VdbLexicon.Namespace.PREFIX, VdbLexicon.Namespace.URI);
//      registry.registerNamespace(XmiLexicon.Namespace.PREFIX, XmiLexicon.Namespace.URI);
//      registry.registerNamespace(CoreLexicon.Namespace.PREFIX, CoreLexicon.Namespace.URI);
      registerNodeTypes("xmi.cnd", nodeTypeManager, true);
      LOGGER.debug("xmi.cnd loaded");

      registerNodeTypes("med.cnd", nodeTypeManager, true);
      LOGGER.debug("med.cnd loaded");

      registerNodeTypes("mmcore.cnd", nodeTypeManager, true);
      LOGGER.debug("mmcore.cnd loaded");

      registerNodeTypes("vdb.cnd", nodeTypeManager, true);
      LOGGER.debug("vdb.cnd loaded");

      registerNodeTypes("dv.cnd", nodeTypeManager, true);
      LOGGER.debug("dv.cnd loaded");

      LOGGER.debug("exit initialize");
    }

    protected DataserviceDatasource readDatasource(Binary binaryValue, InputStream inputStream, Node outputNode, Context context) throws Exception {
    	DataserviceDatasource datasource;
        LOGGER.debug("----before reading datasource");

        datasource = DataserviceDatasource.read(inputStream, context);
        assert (datasource != null) : "datasource is null";

        // Create the output node for the Datasource ...
        outputNode.setPrimaryType(DataVirtLexicon.Datasource.DATASOURCE);
        outputNode.addMixin(JcrConstants.MIX_REFERENCEABLE);

        LOGGER.debug(">>>>done reading datasource xml\n\n");
        return datasource;
    }

}
