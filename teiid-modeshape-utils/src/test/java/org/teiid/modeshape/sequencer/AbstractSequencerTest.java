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
package org.teiid.modeshape.sequencer;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.modeshape.jcr.api.observation.Event.Sequencing.NODE_SEQUENCED;
import static org.modeshape.jcr.api.observation.Event.Sequencing.NODE_SEQUENCING_FAILURE;
import static org.modeshape.jcr.api.observation.Event.Sequencing.OUTPUT_PATH;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SELECTED_PATH;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SEQUENCED_NODE_ID;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SEQUENCED_NODE_PATH;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SEQUENCER_NAME;
import static org.modeshape.jcr.api.observation.Event.Sequencing.USER_ID;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Workspace;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.modeshape.common.logging.Logger;
import org.modeshape.jcr.Environment;
import org.modeshape.jcr.JcrI18n;
import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.JcrSession;
import org.modeshape.jcr.LocalEnvironment;
import org.modeshape.jcr.ModeShapeEngine;
import org.modeshape.jcr.ModeShapeEngine.State;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.JcrTools;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.observation.Event;

/**
 * Class which serves as base for various sequencer unit tests. In addition to this, it uses the sequencing events fired by
 * ModeShape's {@link javax.jcr.observation.ObservationManager} to perform various assertions and therefore, acts as a test for
 * those as well.
 */
public abstract class AbstractSequencerTest {

    protected static ModeShapeEngine _engine;
    protected static final int DEFAULT_WAIT_TIME_SECONDS = 15;
    protected static final String REPO_NAME = "teiid-modeshape-sequencer-test-repository";

    protected static final boolean START_REPO_AUTOMATICALLY = true;

    @BeforeClass
    public static void createEngine() {
        _engine = new ModeShapeEngine();
        _engine.start();
    }

    protected RepositoryConfiguration config;
    protected Environment environment;
    private final Logger logger = Logger.getLogger( getClass() );
    private ObservationManager observationManager;
    protected JcrRepository repository;
    protected Node rootNode;
    protected JcrSession session;
    protected JcrTools tools;

    /**
     * A [node path, latch] map which is used to block tests waiting for sequenced output, until either the node has been
     * sequenced or a timeout occurs
     */
    private final ConcurrentHashMap< String, CountDownLatch > nodeSequencedLatches = new ConcurrentHashMap< String, CountDownLatch >();

    /**
     * A [node path, node instance] map which is populated by the listener, once each sequencing event is received
     */
    private final Map< String, Node > sequencedNodes = new HashMap< String, Node >();

    /**
     * A [sequenced node path, event] map which will hold all the received sequencing events, both in failure and non-failure
     * cases, using the path of the sequenced node as key.
     */
    protected final ConcurrentHashMap< String, Event > sequencingEvents = new ConcurrentHashMap< String, Event >();

    /**
     * A [node path, latch] map which is used to block tests waiting for a sequencing failure, until either the failure has
     * occurred or a timeout occurs
     */
    private final ConcurrentHashMap< String, CountDownLatch > sequencingFailureLatches = new ConcurrentHashMap< String, CountDownLatch >();

    protected void addSequencingListeners( final JcrSession session ) throws RepositoryException {
        this.observationManager.addEventListener( new SequencingListener(), NODE_SEQUENCED, null, true, null, null, false );
        this.observationManager.addEventListener( new SequencingFailureListener(),
                                                  NODE_SEQUENCING_FAILURE,
                                                  null,
                                                  true,
                                                  null,
                                                  null,
                                                  false );
    }

    @After
    public void afterEach() throws Exception {
        for ( final EventListenerIterator it = this.observationManager.getRegisteredEventListeners(); it.hasNext(); ) {
            this.observationManager.removeEventListener( it.nextEventListener() );
        }

        stopRepository();
        cleanupData();
    }

    @Before
    public void beforeEach() throws Exception {
        if ( START_REPO_AUTOMATICALLY ) {
            startRepository();
        }

        this.tools = new JcrTools();
        this.rootNode = this.session.getRootNode();
        addSequencingListeners( this.session );
    }

    private void cleanupData() {
        this.sequencedNodes.clear();
        this.sequencingEvents.clear();
        this.nodeSequencedLatches.clear();
        this.sequencingFailureLatches.clear();
    }

    /**
     * Creates a nt:file node, under the root node, at the given path and with the jcr:data property pointing at the filepath.
     *
     * @param nodeRelativePath the path under the root node, where the nt:file will be created.
     * @param filePath a path relative to {@link Class#getResourceAsStream(String)} where a file is expected at runtime
     * @return the new node
     * @throws RepositoryException if anything fails
     */
    protected Node createNodeWithContentFromFile( final String nodeRelativePath,
                                                  final String filePath ) throws RepositoryException {
        Node parent = this.rootNode;

        for ( final String pathSegment : nodeRelativePath.split( "/" ) ) {
            parent = parent.addNode( pathSegment );
        }

        final Node content = parent.addNode( JcrConstants.JCR_CONTENT );
        content.setProperty( JcrConstants.JCR_DATA,
                             ( ( javax.jcr.Session )this.session ).getValueFactory().createBinary( resourceStream( filePath ) ) );

        this.session.save();
        return parent;
    }

    private RepositoryConfiguration createRepositoryConfiguration( final String repositoryName ) throws Exception {
        return RepositoryConfiguration.read( getRepositoryConfigStream(), repositoryName ).with( this.environment );
    }

    private void createWaitingLatchIfNecessary( final String expectedPath,
                                                final ConcurrentHashMap< String, CountDownLatch > latchesMap ) {
        latchesMap.putIfAbsent( expectedPath, new CountDownLatch( 1 ) );
    }

    /**
     * Retrieves a sequenced node using 5 seconds as maximum wait time.
     *
     * @param parentNode an existing {@link Node}
     * @param relativePath the path under the parent node at which the sequenced node is expected to appear (note that this must
     *        be the path to the "new" node, always.
     * @return either the sequenced node or null, if something has failed.
     * @throws Exception if anything unexpected happens
     * @see AbstractSequencerTest#getOutputNode(javax.jcr.Node, String, int)
     */
    protected Node getOutputNode( final Node parentNode,
                                  final String relativePath ) throws Exception {
        return this.getOutputNode( parentNode, relativePath, DEFAULT_WAIT_TIME_SECONDS );
    }

    /**
     * Attempts to retrieve a node (which is expected to have been sequenced) under an existing parent node at a relative path.
     * The sequenced node "appears" when the {@link SequencingListener} is notified of the sequencing process. The thread which
     * calls this method either returns immediately if the node has already been sequenced, or waits a number of seconds for it to
     * become available.
     *
     * @param parentNode an existing {@link Node}
     * @param relativePath the path under the parent node at which the sequenced node is expected to appear (note that this must
     *        be the path to the "new" node, always.
     * @param waitTimeSeconds the max number of seconds to wait.
     * @return either the sequenced node or null, if something has failed.
     * @throws Exception if anything unexpected happens
     * @throws java.lang.AssertionError if the specified period of time has elapsed, but not enough sequencing events were
     *         received
     */
    protected Node getOutputNode( final Node parentNode,
                                  final String relativePath,
                                  final int waitTimeSeconds ) throws Exception {
        final String parentNodePath = parentNode.getPath();
        final String expectedPath = parentNodePath.endsWith( "/" ) ? parentNodePath + relativePath : parentNodePath + "/"
                                                                                                     + relativePath;

        return getOutputNode( expectedPath, waitTimeSeconds );
    }

    /**
     * Retrieves a new node under the given path, as a result of sequecing, or returns null if the given timeout occurs.
     *
     * @param expectedPath
     * @param waitTimeSeconds
     * @return the output node
     * @throws InterruptedException
     */
    protected Node getOutputNode( final String expectedPath,
                                  final int waitTimeSeconds ) throws InterruptedException {
        if ( !this.sequencedNodes.containsKey( expectedPath ) ) {
            createWaitingLatchIfNecessary( expectedPath, this.nodeSequencedLatches );
            this.logger.debug( "Waiting for sequenced node at: " + expectedPath );

            final CountDownLatch countDownLatch = this.nodeSequencedLatches.get( expectedPath );
            countDownLatch.await( waitTimeSeconds, TimeUnit.SECONDS );
        }

        this.nodeSequencedLatches.remove( expectedPath );
        return this.sequencedNodes.remove( expectedPath );
    }

    /**
     * Returns an input stream to a JSON file which will be used to configure the repository. By default, this is
     * config/repo-config.json
     *
     * @return an {@code InputStream} instance
     */
    protected InputStream getRepositoryConfigStream() {
        return resourceStream( "config/repo-config.json" );
    }

    protected void killRepository( final JcrRepository repository ) {
        try {
            if ( repository.getState() != State.RUNNING ) {
                return;
            }

            _engine.undeploy( REPO_NAME );
        } catch ( final Throwable t ) {
            this.logger.error( JcrI18n.errorKillingRepository, repository.getName(), t.getMessage() );
        }
    }

    /**
     * Register the node types in the CND file at the given location on the classpath.
     *
     * @param resourceName the name of the CND file on the classpath
     * @throws RepositoryException if there is a problem registering the node types
     * @throws IOException if the CND file could not be read
     */
    protected void registerNodeTypes( final String resourceName ) throws RepositoryException, IOException {
        final InputStream stream = resourceStream( resourceName );
        assertThat( stream, is( notNullValue() ) );

        final Workspace workspace = this.session.getWorkspace();
        final NodeTypeManager ntMgr = ( NodeTypeManager )workspace.getNodeTypeManager();
        ntMgr.registerNodeTypes( stream, true );
    }

    /**
     * Utility method to get the resource on the classpath given by the supplied name
     *
     * @param name the name (or path) of the classpath resource
     * @return the input stream to the content; may be null if the resource does not exist
     */
    protected InputStream resourceStream( final String name ) {
        return getClass().getClassLoader().getResourceAsStream( name );
    }

    protected void smokeCheckSequencingEvent( final Event event,
                                              final int expectedEventType,
                                              final String... expectedEventInfoKeys ) throws RepositoryException {
        assertEquals( event.getType(), expectedEventType );

        final Map< ?, ? > info = event.getInfo();
        assertNotNull( info );

        for ( final String extraInfoKey : expectedEventInfoKeys ) {
            assertNotNull( info.get( extraInfoKey ) );
        }
    }

    protected void startRepository() throws Exception {
        this.environment = new LocalEnvironment();
        this.config = createRepositoryConfiguration( REPO_NAME );
        this.repository = _engine.deploy( this.config );
        this.session = this.repository.login();
        this.observationManager = ( ( Workspace )this.session.getWorkspace() ).getObservationManager();
    }

    protected void stopRepository() throws Exception {
        try {
            try {
                if ( ( this.session != null ) && this.session.isLive() ) {
                    this.session.logout();
                }
            } finally {
                killRepository( this.repository );
            }
        } finally {
            this.repository = null;
            this.config = null;
            this.environment.shutdown();
            this.environment = null;
        }
    }

    protected final class SequencingFailureListener implements EventListener {
        @SuppressWarnings( "synthetic-access" )
        @Override
        public void onEvent( final EventIterator events ) {
            while ( events.hasNext() ) {
                try {
                    final Event event = ( Event )events.nextEvent();
                    smokeCheckSequencingEvent( event,
                                               NODE_SEQUENCING_FAILURE,
                                               SEQUENCED_NODE_ID,
                                               SEQUENCED_NODE_PATH,
                                               Event.Sequencing.SEQUENCING_FAILURE_CAUSE,
                                               OUTPUT_PATH,
                                               SELECTED_PATH,
                                               SEQUENCER_NAME,
                                               USER_ID );
                    final String nodePath = event.getPath();

                    AbstractSequencerTest.this.sequencingEvents.putIfAbsent( nodePath, event );
                    createWaitingLatchIfNecessary( nodePath, AbstractSequencerTest.this.sequencingFailureLatches );
                    AbstractSequencerTest.this.sequencingFailureLatches.get( nodePath ).countDown();
                } catch ( final Exception e ) {
                    throw new RuntimeException( e );
                }
            }
        }
    }

    protected final class SequencingListener implements EventListener {
        @SuppressWarnings( "synthetic-access" )
        @Override
        public void onEvent( final EventIterator events ) {
            while ( events.hasNext() ) {
                try {
                    final Event event = ( Event )events.nextEvent();
                    smokeCheckSequencingEvent( event,
                                               NODE_SEQUENCED,
                                               SEQUENCED_NODE_ID,
                                               SEQUENCED_NODE_PATH,
                                               OUTPUT_PATH,
                                               SELECTED_PATH,
                                               SEQUENCER_NAME,
                                               USER_ID );
                    AbstractSequencerTest.this.sequencingEvents.putIfAbsent( ( String )event.getInfo().get( SEQUENCED_NODE_PATH ),
                                                                             event );

                    final String nodePath = event.getPath();
                    AbstractSequencerTest.this.logger.debug( "New sequenced node at: " + nodePath );
                    AbstractSequencerTest.this.sequencedNodes.put( nodePath,
                                                                   AbstractSequencerTest.this.session.getNode( nodePath ) );

                    // signal the node is available
                    createWaitingLatchIfNecessary( nodePath, AbstractSequencerTest.this.nodeSequencedLatches );
                    AbstractSequencerTest.this.nodeSequencedLatches.get( nodePath ).countDown();
                } catch ( final Exception e ) {
                    throw new RuntimeException( e );
                }
            }
        }
    }

}