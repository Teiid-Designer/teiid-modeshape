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
package org.teiid.modeshape.sequencer.vdb.model;

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
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.teiid.modeshape.sequencer.vdb.TeiidI18n;
import org.teiid.modeshape.sequencer.vdb.VdbModel;
import org.teiid.modeshape.sequencer.vdb.VdbSequencer;
import org.teiid.modeshape.sequencer.vdb.lexicon.CoreLexicon;
import org.teiid.modeshape.sequencer.vdb.lexicon.DiagramLexicon;
import org.teiid.modeshape.sequencer.vdb.lexicon.RelationalLexicon;
import org.teiid.modeshape.sequencer.vdb.lexicon.VdbLexicon;

/**
 * A sequencer of Teiid XMI model files.
 */
@ThreadSafe
public class ModelSequencer extends Sequencer {

    private static final String[] MODEL_FILE_EXTENSIONS = { ".xmi" };
    private static final Logger LOGGER = Logger.getLogger(ModelSequencer.class);

    /**
     * @param modelReader the reader who processed the model file (cannot be <code>null</code>)
     * @return <code>true</code> if the model process by the reader should be sequenced
     */
    public static boolean shouldSequence( final ModelReader modelReader ) {
        assert (modelReader != null);

        final String modelType = modelReader.getModelType();
        final boolean validModelType = CoreLexicon.ModelType.PHYSICAL.equalsIgnoreCase(modelType)
                || CoreLexicon.ModelType.VIRTUAL.equalsIgnoreCase(modelType);
        return (validModelType && RelationalLexicon.Namespace.URI.equals(modelReader.getPrimaryMetamodelUri()));
    }

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
        outputNode.addMixin(CoreLexicon.JcrId.MODEL);

        InputStream modelStream = null;
        
        try {
            modelStream = binaryValue.getStream();
            return sequenceModel(modelStream, outputNode, outputNode.getPath(), null, new ReferenceResolver(), context);
        } finally {
            if (modelStream != null) {
                modelStream.close();
            }
        }
    }

    /**
     * @param resourceName the name of the resource being checked (cannot be <code>null</code>)
     * @return <code>true</code> if the resource has a model file extension
     */
    public boolean hasModelFileExtension( final String resourceName ) {
        for (final String extension : MODEL_FILE_EXTENSIONS) {
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

        Class<?> vdbSeqClass = VdbSequencer.class;
        super.registerNodeTypes(vdbSeqClass.getResourceAsStream("xmi.cnd"), nodeTypeManager, true);
        LOGGER.debug("xmi.cnd loaded");

        super.registerNodeTypes(vdbSeqClass.getResourceAsStream("med.cnd"), nodeTypeManager, true);
        LOGGER.debug("med.cnd loaded");

        super.registerNodeTypes(vdbSeqClass.getResourceAsStream("mmcore.cnd"), nodeTypeManager, true);
        LOGGER.debug("mmcore.cnd loaded");

        super.registerNodeTypes(vdbSeqClass.getResourceAsStream("jdbc.cnd"), nodeTypeManager, true);
        LOGGER.debug("jdbc.cnd loaded");

        super.registerNodeTypes(vdbSeqClass.getResourceAsStream("relational.cnd"), nodeTypeManager, true);
        LOGGER.debug("relational.cnd loaded");

        super.registerNodeTypes(vdbSeqClass.getResourceAsStream("transformation.cnd"), nodeTypeManager, true);
        LOGGER.debug("transformation.cnd loaded");

        // Register some of the namespaces we'll need ...
        registerNamespace(DiagramLexicon.Namespace.PREFIX, DiagramLexicon.Namespace.URI, registry);

        LOGGER.debug("exit initialize");
    }

    /**
     * The method that performs the sequencing.
     *
     * @param modelStream the input stream of the model file (cannot be <code>null</code>)
     * @param modelOutputNode the root node of the model being sequenced (cannot be <code>null</code>)
     * @param modelPath the model path including the model name (cannot be <code>null</code> or empty)
     * @param vdbModel the VDB model associated with the input stream (cannot be <code>null</code>)
     * @param resolver a {@link ReferenceResolver} instance; may not be {@code null}
     * @param context the sequencer context (cannot be <code>null</code>)  @return <code>true</code> if the model file input stream was successfully sequenced
     * @return <code>true</code> if the model was sequenced successfully
     * @throws Exception if there is a problem during sequencing
     */
    private boolean sequenceModel( final InputStream modelStream,
                                   final Node modelOutputNode,
                                   final String modelPath,
                                   final VdbModel vdbModel,
                                   final ReferenceResolver resolver, 
                                   final Context context ) throws Exception {
        assert (modelStream != null);
        assert (modelOutputNode != null);
        assert (context != null);
        assert (modelOutputNode.isNodeType(CoreLexicon.JcrId.MODEL));

        LOGGER.debug("sequenceModel:model node path='{0}', model path='{1}', vdb model='{2}'",
                     modelOutputNode.getPath(),
                     modelPath,
                     vdbModel);

        final NamespaceRegistry registry = modelOutputNode.getSession().getWorkspace().getNamespaceRegistry();
        final ModelReader modelReader = new ModelReader(modelPath, resolver, registry);
        modelReader.readModel(modelStream);

        if (shouldSequence(modelReader)) {
            final ModelNodeWriter nodeWriter = new ModelNodeWriter(modelOutputNode, modelReader, resolver, vdbModel,
                                                                   context);
            return nodeWriter.write();
        }

        // stream was not sequenced
        LOGGER.debug("sequenceModel:model not sequenced at path '{0}'", modelPath);
        return false;
    }

    /**
     * Used only by the VDB sequencer to sequence a model file contained in a VDB.
     *
     * @param modelStream the input stream of the model file (cannot be <code>null</code>)
     * @param modelOutputNode the root node of the model being sequenced (cannot be <code>null</code>)
     * @param vdbModel the VDB model associated with the input stream (cannot be <code>null</code>)
     * @param resolver a {@link ReferenceResolver} instance; may not be {@code null}
     * @param context the sequencer context (cannot be <code>null</code>)
     * @return <code>true</code> if the model file input stream was successfully sequenced
     * @throws Exception if there is a problem during sequencing or node does not have a VDB model primary type
     */
    public boolean sequenceVdbModel( final InputStream modelStream,
                                     final Node modelOutputNode,
                                     final VdbModel vdbModel,
                                     final ReferenceResolver resolver,
                                     final Context context ) throws Exception {
        CheckArg.isNotNull(modelStream, "modelStream");
        CheckArg.isNotNull(modelOutputNode, "modelOutputNode");
        CheckArg.isNotNull(vdbModel, "vdbModel");

        if (!modelOutputNode.isNodeType(VdbLexicon.Model.MODEL)) {
            throw new RuntimeException(TeiidI18n.invalidVdbModelNodeType.text(modelOutputNode.getPath()));
        }

        return sequenceModel(modelStream, modelOutputNode, vdbModel.getPathInVdb(), vdbModel, resolver, context);
    }
}
