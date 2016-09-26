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

package org.teiid.modeshape.sequencer.internal;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import javax.jcr.Node;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.StringUtil;
import org.teiid.modeshape.core.CoreI18n;
import org.teiid.modeshape.sequencer.Exporter;
import org.teiid.modeshape.sequencer.Options;
import org.teiid.modeshape.sequencer.Result;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * A base implementation of an exporter.
 */
public abstract class AbstractExporter implements Exporter {

    private static final Logger LOGGER = Logger.getLogger( AbstractExporter.class );

    /**
     * An empty array of nodes.
     */
    protected static final Node[] NO_NODES = new Node[ 0 ];

    /**
     * @param nodeBeingExported
     *        the node being exported (never <code>null</code>)
     * @param options
     *        the export options (never <code>null</code>)
     * @param result
     *        the object to set results on (never <code>null</code>)
     */
    protected abstract void doExport( final Node nodeBeingExported,
                                      final Options options,
                                      final ResultImpl result );

    /**
     * {@inheritDoc}
     *
     * @see org.teiid.modeshape.sequencer.Exporter#execute(javax.jcr.Node, org.teiid.modeshape.sequencer.Options)
     */
    @Override
    public final Result execute( final Node nodeBeingExported,
                                 final Options exportOptions ) {
        final long start = System.currentTimeMillis();
        final Options options = ( ( exportOptions == null ) ? new Options() : exportOptions );
        ResultImpl result = null;
        String nodePath = null;

        try {
            nodePath = Objects.requireNonNull( nodeBeingExported, "nodeBeingExported" ).getPath();
            result = new ResultImpl( nodePath, options );
        } catch ( final Exception e ) {
            if ( result == null ) {
                result = new ResultImpl();
            }

            result.setError( CoreI18n.errorConstructingExportResult.text( getClass().getName() ), e );
            return result;
        }

        try {
            LOGGER.debug( "Starting export of node {0} by exporter {1}", nodePath, getClass().getSimpleName() );
            doExport( nodeBeingExported, options, result );
        } catch ( final Exception e ) {
            result.setError( CoreI18n.errorDuringExport.text( getClass().getName() ), e );
        } finally {
            LOGGER.debug( "Finished export of node {0} by exporter {1} in {2}ms and success = {3}",
                          ( nodePath == null ) ? "null node path" : nodePath,
                          getClass().getSimpleName(),
                          ( System.currentTimeMillis()
                            - start ),
                          result.wasSuccessful() );
        }

        return result;
    }

    protected String getIndentAmount( final Options options ) {
        assert ( options != null );
        final Object value = options.get( Options.INDENT_AMOUNT_PROPERTY, Options.DEFAULT_INDENT_AMOUNT );

        if ( !( value instanceof Integer ) ) {
            return Integer.toString( Options.DEFAULT_INDENT_AMOUNT );
        }

        return Integer.toString( ( Integer )value );
    }

    protected Options.PropertyFilter getPropertyFilter( final Options options ) {
        assert ( options != null );
        final Object value = options.get( Options.PROPERTY_FILTER_PROPERTY, Options.DEFAULT_PROPERTY_FILTER );

        if ( !( value instanceof Options.PropertyFilter ) ) {
            return Options.DEFAULT_PROPERTY_FILTER;
        }

        return ( Options.PropertyFilter )value;
    }

    protected boolean isPrettyPrint( final Options options ) {
        assert ( options != null );
        final Object value = options.get( Options.PRETTY_PRINT_PROPERTY, Options.DEFAULT_PRETTY_PRINT );

        if ( !( value instanceof Boolean ) ) {
            return Options.DEFAULT_PRETTY_PRINT;
        }

        return ( Boolean )value;
    }

    protected Document parseXmlFile( final String xml ) throws Exception {
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder db = dbf.newDocumentBuilder();
        final InputSource is = new InputSource( new StringReader( xml ) );
        return db.parse( is );
    }

    protected String prettyPrint( final String xml,
                                  final Options options ) throws Exception {
        assert ( options != null );
        final Document document = parseXmlFile( xml );
        final TransformerFactory factory = TransformerFactory.newInstance();

        final Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty( OutputKeys.ENCODING, "UTF-8" );
        transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );
        transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
        transformer.setOutputProperty( "{http://xml.apache.org/xslt}indent-amount", getIndentAmount( options ) );

        final DOMSource source = new DOMSource( document );
        final StringWriter output = new StringWriter();
        final StreamResult result = new StreamResult( output );
        transformer.transform( source, result );

        return output.toString();
    }

    protected class ResultImpl implements Result {

        /**
         * The data key whose value is the class name of the exporter. Used only be the framework.
         */
        public static final String EXPORTER = "exporter.exporter";

        /**
         * The data key whose value are the options used by the exporter. Used only be the framework.
         */
        public static final String OPTIONS = "exporter.options";

        /**
         * The data key whose value is the results of the export. Value must be set by the exporter.
         * 
         * @see #setOutcome(Object, Class)
         */
        public static final String OUTCOME = "exporter.outcome";

        /**
         * The data key whose value is the absolute path of the node being exported. Used only be the framework.
         */
        public static final String PATH_EXPORTED_NODE = "exporter.exported-node-path";

        /**
         * The data key whose value is the type of the results. Must be set by the exporter.
         */
        public static final String TYPE = "exporter.outcome-type";

        private final Map< String, Object > data;
        private Exception error = null;
        private String errorMsg = null;
        private Class< ? > type = null;

        protected ResultImpl() {
            this.data = new HashMap<>();
            this.data.put( EXPORTER, getClass().getName() );
        }

        protected ResultImpl( final String pathOfExportedNode,
                              final Options options ) {
            this();
            this.data.put( PATH_EXPORTED_NODE, pathOfExportedNode );
            this.data.put( OPTIONS, options );
        }

        /**
         * {@inheritDoc}
         *
         * @see org.teiid.modeshape.sequencer.Result#getData(java.lang.String)
         */
        @Override
        public Object getData( final String key ) {
            return this.data.get( key );
        }

        /**
         * {@inheritDoc}
         *
         * @see org.teiid.modeshape.sequencer.Result#getError()
         */
        @Override
        public Exception getError() {
            return this.error;
        }

        /**
         * {@inheritDoc}
         *
         * @see org.teiid.modeshape.sequencer.Result#getErrorMessage()
         */
        @Override
        public String getErrorMessage() {
            if ( StringUtil.isBlank( this.errorMsg ) ) {
                return ( ( this.error == null ) ? null : this.error.getLocalizedMessage() );
            }

            return this.errorMsg;
        }

        /**
         * {@inheritDoc}
         *
         * @see org.teiid.modeshape.sequencer.Result#getOptions()
         */
        @Override
        public Options getOptions() {
            return ( Options )this.data.get( OPTIONS );
        }

        /**
         * {@inheritDoc}
         *
         * @see org.teiid.modeshape.sequencer.Result#getOutcome()
         */
        @Override
        public Object getOutcome() {
            return this.data.get( OUTCOME );
        }

        /**
         * {@inheritDoc}
         *
         * @see org.teiid.modeshape.sequencer.Result#getType()
         */
        @Override
        public Class< ? > getType() {
            return this.type;
        }

        /**
         * {@inheritDoc}
         *
         * @see java.lang.Iterable#iterator()
         */
        @Override
        public Iterator< String > iterator() {
            return this.data.keySet().iterator();
        }

        /**
         * @param key
         *        the data key (cannot be <code>null</code> or empty)
         * @param value
         *        the data value (can be <code>null</code> if removing current data)
         * @return the previous value (can be <code>null</code>)
         */
        public Object setData( final String key,
                               final Object value ) {
            if ( EXPORTER.equals( key )
                 || OPTIONS.equals( org.teiid.modeshape.util.StringUtil.requireNonEmpty( key, "key" ) )
                 || PATH_EXPORTED_NODE.equals( key )
                 || TYPE.equals( key ) ) {
                throw new RuntimeException( CoreI18n.unmodifiableResultData.text( key ) );
            }

            if ( value == null ) {
                return this.data.remove( key );
            }

            return this.data.put( key, value );
        }

        /**
         * @param message
         *        the error message (can be <code>null</code> or empty)
         * @param error
         *        the error (can be <code>null</code>)
         */
        public void setError( final String message,
                              final Exception error ) {
            this.error = error;
            this.errorMsg = message;
        }

        /**
         * @param outcome
         *        the outcome of the export (cannot be <code>null</code>)
         * @param outcomeType
         *        the type of the outcome (cannot be <code>null</code>)
         */
        public void setOutcome( final Object outcome,
                                final Class< ? > outcomeType ) {
            this.data.put( OUTCOME, Objects.requireNonNull( outcome, "outcome" ) );
            this.type = Objects.requireNonNull( outcomeType, "outcomeType" );
        }

        /**
         * {@inheritDoc}
         *
         * @see org.teiid.modeshape.sequencer.Result#wasSuccessful()
         */
        @Override
        public boolean wasSuccessful() {
            return ( ( this.error == null )
                     && StringUtil.isBlank( this.errorMsg ) );
        }

    }

}
