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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import org.modeshape.common.annotation.ThreadSafe;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;
import org.modeshape.common.util.StringUtil;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;
import org.teiid.modeshape.sequencer.dataservice.lexicon.VdbLexicon;

/**
 * A sequencer of Teiid Dataservice files.
 */
@ThreadSafe
public class DataserviceSequencer extends Sequencer {

    protected static final Logger LOGGER = Logger.getLogger(DataserviceSequencer.class);
    private static final String MANIFEST_FILE = "META-INF/dataservice.xml";
    private static final String VDBS = "vdbs";
    private static final String DATASOURCES = "datasources";
    private static final String DRIVERS = "drivers";
    private static final String VDB_SUFFIX = "-vdb.xml";
    private static final String DATASOURCE_SUFFIX = ".xml";

    private VdbDynamicSequencer vdbSequencer; // constructed during initialize method
    private DatasourceSequencer datasourceSequencer;  // constructed during initialize method
    
    /**
     * @see org.modeshape.jcr.api.sequencer.Sequencer#execute(javax.jcr.Property, javax.jcr.Node,
     *      org.modeshape.jcr.api.sequencer.Sequencer.Context)
     */
    @Override
    public boolean execute( final Property inputProperty,
                            final Node outputNode,
                            final Context context ) throws Exception {
        LOGGER.debug("DataserviceSequencer.execute called:outputNode name='{0}', path='{1}'", outputNode.getName(), outputNode.getPath());

        final Binary binaryValue = inputProperty.getBinary();
        CheckArg.isNotNull(binaryValue, "binary");

        try (final ZipInputStream dataserviceStream = new ZipInputStream(binaryValue.getStream())) {
            DataserviceManifest manifest = null;
            ZipEntry entry = null;
            while ((entry = dataserviceStream.getNextEntry()) != null) {
                String entryName = entry.getName();

                if (entry.isDirectory()) {
                    LOGGER.debug("----ignoring directory '{0}'", entryName);
                } else if ( entryName.endsWith(MANIFEST_FILE))  {
                    manifest = readManifest(binaryValue, dataserviceStream, outputNode, context);
                } else if ( entryName.startsWith(VDBS) && entryName.endsWith(VDB_SUFFIX) ) {
                    LOGGER.debug("----before sequencing vdb '{0}'", entryName);

                    final Node vdbNode = outputNode.addNode(entryName, VdbLexicon.Vdb.VIRTUAL_DATABASE);
                	boolean sequenced = this.vdbSequencer.execute(inputProperty, vdbNode, context);
                	
                    if (!sequenced) {
                        LOGGER.debug(">>>>Vdb NOT sequenced '{0}'\n\n", entryName);
                    } else {
                        LOGGER.debug(">>>>done sequencing Vdb '{0}'\n\n", entryName);
                    }
                } else if ( entryName.startsWith(DATASOURCES) && this.datasourceSequencer.hasDatasourceFileExtension(entryName) ) {
                    LOGGER.debug("----before reading datasource '{0}'", entryName);

                    final Node datasourceNode = outputNode.addNode(entryName, DataVirtLexicon.Datasource.DATASOURCE);
                    
                    final boolean sequenced = this.datasourceSequencer.execute(inputProperty, datasourceNode, context);

                    if (!sequenced) {
                    	datasourceNode.remove();
                        LOGGER.debug(">>>>datasource NOT sequenced '{0}'\n\n", entryName);
                    } else {
                        LOGGER.debug(">>>>done sequencing datasource '{0}'\n\n", entryName);
                    }
                } else if ( entryName.startsWith(DRIVERS) ) {
                    LOGGER.debug("----Processing Driver '{0}'", entryName);
                } else {
                    LOGGER.debug("----ignoring resource '{0}'", entryName);
                }
            }

            return true;
        } catch (final Exception e) {
            throw new RuntimeException(TeiidI18n.errorReadingDataserviceFile.text(inputProperty.getPath(), e.getMessage()), e);
        }
    }

    protected DataserviceManifest readManifest(Binary binaryValue, InputStream inputStream, Node outputNode, Context context) throws Exception {
        DataserviceManifest manifest;
        LOGGER.debug("----before reading manifest xml");

        manifest = DataserviceManifest.read(inputStream, context);
        assert (manifest != null) : "manifest is null";

        // Create the output node for the VDB ...
        outputNode.setPrimaryType(DataVirtLexicon.Dataservice.DATASERVICE);
        outputNode.addMixin(JcrConstants.MIX_REFERENCEABLE);
        outputNode.setProperty(DataVirtLexicon.Dataservice.SERVICE_VDB, manifest.getServiceVdbName());
        outputNode.setProperty(DataVirtLexicon.Dataservice.VDBS, manifest.getVdbNames());
        outputNode.setProperty(DataVirtLexicon.Dataservice.DATASOURCES, manifest.getDatasourceNames());
        outputNode.setProperty(DataVirtLexicon.Dataservice.DRIVERS, manifest.getDriverNames());

        LOGGER.debug(">>>>done reading manifest xml\n\n");
        return manifest;
    }

    /**
     * @throws IOException
     * @see org.modeshape.jcr.api.sequencer.Sequencer#initialize(javax.jcr.NamespaceRegistry,
     *      org.modeshape.jcr.api.nodetype.NodeTypeManager)
     */
    @Override
    public void initialize( final NamespaceRegistry registry,
                            final NodeTypeManager nodeTypeManager ) throws RepositoryException, IOException {
        LOGGER.debug("enter initialize");
//        registry.registerNamespace(VdbLexicon.Namespace.PREFIX, VdbLexicon.Namespace.URI);
//        registry.registerNamespace(XmiLexicon.Namespace.PREFIX, XmiLexicon.Namespace.URI);
//        registry.registerNamespace(CoreLexicon.Namespace.PREFIX, CoreLexicon.Namespace.URI);
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

        this.vdbSequencer = new VdbDynamicSequencer();
        this.vdbSequencer.initialize(registry, nodeTypeManager);

        this.datasourceSequencer = new DatasourceSequencer();
        this.datasourceSequencer.initialize(registry, nodeTypeManager);

        LOGGER.debug("exit initialize");
    }

    /**
     * Sets a property value only if the value is not <code>null</code> and not empty.
     *
     * @param node the node whose property is being set (cannot be <code>null</code>)
     * @param name the property name (cannot be <code>null</code>)
     * @param value the property value (can be <code>null</code> or empty)
     * @throws Exception if an error occurs setting the node property
     */
    private void setProperty( final Node node,
                              final String name,
                              final String value ) throws Exception {
        assert (node != null);
        assert (!StringUtil.isBlank(name));

        if (!StringUtil.isBlank(value)) {
            node.setProperty(name, value);
        }
    }
}
