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

import java.io.InputStream;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import org.modeshape.common.util.CheckArg;
import org.teiid.modeshape.sequencer.vdb.lexicon.VdbLexicon;

/**
 * The Dynamic Vdb Sequencer that reads dynamics VDB files defined wholly by DDL
 * in an xml file
 */
public class VdbDynamicSequencer extends VdbSequencer {

    @Override
    public boolean execute(Property inputProperty, Node outputNode, Context context) throws Exception {
        LOGGER.debug("VdbDynamicSequencer.execute called:outputNode name='{0}', path='{1}'", outputNode.getName(), outputNode.getPath());

        final Binary binaryValue = inputProperty.getBinary();
        CheckArg.isNotNull(binaryValue, "binary");

        try (final InputStream stream = binaryValue.getStream()) {

            VdbManifest manifest = readManifest(binaryValue, stream, outputNode, context);
            if (manifest == null) {
                throw new Exception("VdbDynamicSequencer.execute failed. The xml cannot be read.");
            }
        } catch (final Exception e) {
            throw new RuntimeException(TeiidI18n.errorReadingVdbFile.text(inputProperty.getPath(), e.getMessage()), e);
        }

        return true;
    }
    
    /**
     * @param vdbStream the VDB input stream (cannot be <code>null</code>)
     * @param vdbOutputNode the root node of the VDB being sequenced (cannot be <code>null</code>)
     * @return <code>true</code> if the VDB input stream was successfully sequenced
     * @throws Exception if there is a problem during sequencing or node does not have a VDB primary type
     */
    public boolean sequenceVdb( final InputStream vdbStream,
                                final Node vdbOutputNode ) throws Exception {
        CheckArg.isNotNull( vdbStream, "vdbStream" );

        if ( !vdbOutputNode.isNodeType( VdbLexicon.Vdb.VIRTUAL_DATABASE ) ) {
            throw new RuntimeException( TeiidI18n.invalidVdbModelNodeType.text( vdbOutputNode.getPath() ) );
        }

        try {
            final VdbManifest manifest = readManifest( null, vdbStream, vdbOutputNode, null );

            if ( manifest == null ) {
                throw new Exception( "VdbDynamicSequencer.execute failed. The xml cannot be read." );
            }
        } catch ( final Exception e ) {
            throw new RuntimeException( TeiidI18n.errorReadingVdbFile.text( vdbOutputNode.getPath(), e.getMessage() ), e );
        }

        return true;
    }
    
}
