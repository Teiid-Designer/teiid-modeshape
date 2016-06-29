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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.InputStream;

import javax.jcr.Node;

import org.junit.Test;
import org.teiid.modeshape.sequencer.AbstractSequencerTest;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;

/**
 *
 */
public class DataserviceSequencerTest extends AbstractSequencerTest {

    @Override
    protected InputStream getRepositoryConfigStream() {
        return resourceStream("config/repo-config.json");
    }

    @Test
    public void shouldHaveValidCnds() throws Exception {
        registerNodeTypes("org/teiid/modeshape/sequencer/dataservice/xmi.cnd");
        registerNodeTypes("org/teiid/modeshape/sequencer/dataservice/jdbc.cnd");
        registerNodeTypes("org/teiid/modeshape/sequencer/dataservice/mmcore.cnd");
        registerNodeTypes("org/teiid/modeshape/sequencer/dataservice/relational.cnd");
        registerNodeTypes("org/teiid/modeshape/sequencer/dataservice/transformation.cnd");
        registerNodeTypes("org/teiid/modeshape/sequencer/dataservice/vdb.cnd");
        registerNodeTypes("org/teiid/modeshape/sequencer/dataservice/dv.cnd");
    }

    @Test
    public void shouldSequenceDataservice() throws Exception {
        createNodeWithContentFromFile("MyDataservice.zip", "dataservice/sample-ds.zip");
        Node outputNode = getOutputNode(this.rootNode, "dataservices/MyDataservice.zip");
        assertNotNull(outputNode);
        assertThat(outputNode.getPrimaryNodeType().getName(), is(DataVirtLexicon.Dataservice.DATASERVICE));

        // check properties
        assertThat(outputNode.getProperty(DataVirtLexicon.Dataservice.SERVICE_VDB).getString(), is("myService-vdb.xml"));

//
//        { // check child node translators
//            Node translatorsGroupNode = outputNode.getNode(VdbLexicon.Vdb.TRANSLATORS);
//            assertNotNull(translatorsGroupNode);
//            assertThat(translatorsGroupNode.getPrimaryNodeType().getName(), is(VdbLexicon.Vdb.TRANSLATORS));
//            assertThat(translatorsGroupNode.getNodes().getSize(), is(1L));
//
//            // check translator
//            Node translatorNode = translatorsGroupNode.getNode("MyBooks_mysql5");
//            assertNotNull(translatorNode);
//            assertThat(translatorNode.getPrimaryNodeType().getName(), is(VdbLexicon.Translator.TRANSLATOR));
//            assertThat(translatorNode.getProperty(VdbLexicon.Translator.TYPE).getString(), is("mysql5"));
//            assertThat(translatorNode.getProperty(VdbLexicon.Translator.DESCRIPTION).getString(),
//                       is("This is a translator description"));
//
//            { // check translator Teiid properties
//                assertThat(translatorNode.getProperty("nameInSource").getString(), is("bogusName"));
//                assertThat(translatorNode.getProperty("supportsUpdate").getBoolean(), is(true));
//            }
//        }

    }

}
