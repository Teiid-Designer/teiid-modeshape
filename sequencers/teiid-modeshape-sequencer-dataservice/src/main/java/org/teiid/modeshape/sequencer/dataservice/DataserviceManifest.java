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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import org.modeshape.common.logging.Logger;
import org.modeshape.common.util.CheckArg;
import org.teiid.modeshape.sequencer.dataservice.lexicon.DataVirtLexicon;

/**
 * The POJO for the Dataservice manifest file.
 */
public class DataserviceManifest implements Comparable< DataserviceManifest > {

    static final Logger LOGGER = Logger.getLogger( DataserviceManifest.class );

    public static DataserviceManifest read( final InputStream stream ) throws Exception {
        return new Reader().read( stream );
    }

    private DataserviceServiceVdb serviceVdb;
    private Collection< DataserviceImportVdb > vdbs = new ArrayList<>();

    void addImportVdb( final DataserviceImportVdb importVdb ) {
        this.vdbs.add( importVdb );
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo( final DataserviceManifest that ) {
        CheckArg.isNotNull( that, "that" );

        if ( this == that ) {
            return 0;
        }

        int result = this.serviceVdb.getPath().compareTo( that.serviceVdb.getPath() );

        if ( result != 0 ) {
            return result;
        }

        result = Integer.compare( this.vdbs.size(), that.vdbs.size() );

        if ( result != 0 ) {
            return result;
        }

        if ( this.vdbs.containsAll( that.vdbs ) ) {
            return 0;
        }

        // not sure what to do here
        return Long.compare( this.vdbs.hashCode(), that.vdbs.hashCode() );
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals( final Object obj ) {
        if ( ( obj == null ) || !getClass().equals( obj.getClass() ) ) {
            return false;
        }

        final DataserviceManifest that = ( DataserviceManifest )obj;
        return ( Objects.equals( this.serviceVdb, that.serviceVdb ) && Objects.deepEquals( this.vdbs, that.vdbs ) );
    }

    Collection< DataserviceImportVdb > getImportVdbs() {
        return this.vdbs;
    }

    DataserviceServiceVdb getServiceVdb() {
        return this.serviceVdb;
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Objects.hash( this.serviceVdb, this.vdbs );
    }

    void setServiceVdb( final DataserviceServiceVdb serviceVdb ) throws Exception {
        if ( this.serviceVdb != null ) {
            throw new Exception( TeiidI18n.dataserviceVdbAlreadySest.text() );
        }

        this.serviceVdb = serviceVdb;
    }

    /**
     * Dataservice Manifest Reader.
     */
    protected static class Reader {

        /**
         * @param streamReader the reader of the stream being processed (cannot be <code>null</code>)
         * @return the value of the path attribute (never empty)
         * @throws Exception if path attribute is not found
         */
        private String findPathAttribute( final XMLStreamReader streamReader ) throws Exception {
            for ( int i = 0, size = streamReader.getAttributeCount(); i < size; ++i ) {
                final QName name = streamReader.getAttributeName( i );

                if ( name.getLocalPart().equals( DataVirtLexicon.ManifestIds.PATH ) ) {
                    return streamReader.getAttributeValue( i );
                }
            }

            throw new Exception( TeiidI18n.pathAttributeNotFound.text( streamReader.getLocalName() ) );
        }

        private DataserviceManifest parseDataservice( final XMLStreamReader streamReader ) throws Exception {
            assert DataVirtLexicon.ManifestIds.DATASERVICE.equals( streamReader.getLocalName() );
            
            final DataserviceManifest manifest = new DataserviceManifest();
            boolean foundServiceVdb = false;

            while ( streamReader.hasNext() ) {
                streamReader.next(); // point to next element

                if ( streamReader.isStartElement() ) {
                    final String elementName = streamReader.getLocalName();

                    if ( DataVirtLexicon.ManifestIds.SERVICE_VDB.equals( elementName ) ) {
                        foundServiceVdb = true;
                        final DataserviceServiceVdb serviceVdb = parseServiceVdb( manifest, streamReader );
                        manifest.setServiceVdb( serviceVdb );
                    } else if ( DataVirtLexicon.ManifestIds.IMPORT_VDB.equals( elementName ) ) {
                        final DataserviceImportVdb importVdb = parseImportVdb( manifest, streamReader );
                        manifest.addImportVdb( importVdb );
                    } else {
                        LOGGER.debug( "**** unexpected Dataservice element={0}", elementName );
                    }
                } else if ( streamReader.isEndElement() ) {
                    final String elementName = streamReader.getLocalName();

                    if ( DataVirtLexicon.ManifestIds.DATASERVICE.equals( elementName ) ) {
                        break; // done
                    }

                    if ( DataVirtLexicon.ManifestIds.SERVICE_VDB.equals( elementName ) ) {
                        continue;
                    }

                    if ( DataVirtLexicon.ManifestIds.IMPORT_VDB.equals( elementName ) ) {
                        continue;
                    }

                    throw new Exception( TeiidI18n.unhandledDataserviceEndElement.text( elementName ) );
                }
            }

            if ( !foundServiceVdb ) {
                throw new Exception( TeiidI18n.serviceVdbNotFound.text() );
            }
            
            return manifest;
        }

        private DataSourceDescriptor parsedDatasource( final DataserviceImportVdb importVdb,
                                                       final XMLStreamReader streamReader ) throws Exception {
            assert DataVirtLexicon.ManifestIds.DATASOURCE.equals( streamReader.getLocalName() );
            final DataSourceDescriptor ds = new DataSourceDescriptor( importVdb );
            ds.setPath( findPathAttribute( streamReader ) ); // will throw exception if path not found

            while ( streamReader.hasNext() ) {
                final int eventType = streamReader.next();

                if ( streamReader.isStartElement() ) {
                    final String elementName = streamReader.getLocalName();

                    if ( DataVirtLexicon.ManifestIds.DRIVER.equals( elementName ) ) {
                        ds.addDriverPath( findPathAttribute( streamReader ) );
                    } else {
                        throw new Exception( TeiidI18n.unhandledDatasoureElement.text( elementName ) );
                    }
                } else if ( streamReader.isEndElement() ) {
                    final String elementName = streamReader.getLocalName();

                    if ( DataVirtLexicon.ManifestIds.DATASOURCE.equals( elementName ) ) {
                        break; // done
                    }

                    if ( DataVirtLexicon.ManifestIds.DRIVER.equals( elementName ) ) {
                        continue;
                    }

                    throw new Exception( TeiidI18n.unhandledDatasoureEndElement.text( eventType ) );
                } else if ( streamReader.isCharacters() ) {
                    continue;
                } else {
                    throw new Exception( TeiidI18n.unhandledDatasoureEventType.text( eventType ) );
                }
            }

            // make sure there is at least one driver path
            if ( ds.getDriverPaths().size() == 0 ) {
                throw new Exception( TeiidI18n.notDriverPathsFound.text( ds.getPath() ) );
            }

            return ds;
        }

        private DataserviceImportVdb parseImportVdb( final DataserviceManifest manifest,
                                                     final XMLStreamReader streamReader ) throws Exception {
            assert DataVirtLexicon.ManifestIds.IMPORT_VDB.equals( streamReader.getLocalName() );

            final DataserviceImportVdb importVdb = new DataserviceImportVdb( manifest );
            importVdb.setPath( findPathAttribute( streamReader ) );
            
            boolean foundDataSource = false;

            while ( streamReader.hasNext() ) {
                final int eventType = streamReader.next();

                if ( streamReader.isStartElement() ) {
                    final String elementName = streamReader.getLocalName();

                    if ( DataVirtLexicon.ManifestIds.DATASOURCE.equals( elementName ) ) {
                        foundDataSource = true;
                        final DataSourceDescriptor ds = parsedDatasource( importVdb, streamReader );
                        importVdb.setDatasource( ds );
                    } else {
                        throw new Exception( TeiidI18n.unhandledImportVdbElement.text( elementName ) );
                    }
                } else if ( streamReader.isEndElement() ) {
                    final String elementName = streamReader.getLocalName();

                    if ( DataVirtLexicon.ManifestIds.IMPORT_VDB.equals( elementName ) ) {
                        break; // done
                    }

                    if ( DataVirtLexicon.ManifestIds.DATASOURCE.equals( elementName ) ) {
                        continue;
                    }

                    throw new Exception( TeiidI18n.unhandledImportVdbEndElement.text( elementName ) );
                } else if ( streamReader.isCharacters() ) {
                    continue;
                } else {
                    throw new Exception( TeiidI18n.unhandledImportVdbEventType.text( eventType ) );
                }
            }

            if ( !foundDataSource ) {
                throw new Exception( TeiidI18n.dataSourceNotFound.text( importVdb.getPath() ) );
            }
            
            return importVdb;
        }

        private DataserviceServiceVdb parseServiceVdb( final DataserviceManifest manifest,
                                                       final XMLStreamReader streamReader ) throws Exception {
            assert DataVirtLexicon.ManifestIds.SERVICE_VDB.equals( streamReader.getLocalName() );

            final DataserviceServiceVdb serviceVdb = new DataserviceServiceVdb( manifest );
            serviceVdb.setPath( findPathAttribute( streamReader ) );

            return serviceVdb;
        }

        public DataserviceManifest read( final InputStream stream ) throws Exception {
            DataserviceManifest manifest = null;
            final XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader streamReader = null;

            try {
                streamReader = factory.createXMLStreamReader( stream );

                if ( streamReader.hasNext() ) {
                    if ( streamReader.next() == XMLStreamConstants.START_ELEMENT ) {
                        final String elementName = streamReader.getLocalName();

                        if ( DataVirtLexicon.ManifestIds.DATASERVICE.equals( elementName ) ) {
                            manifest = parseDataservice( streamReader );
                            assert ( manifest != null ) : "manifest is null";
                        } else {
                            LOGGER.debug( "**** unhandled vdb read element ****" );
                        }
                    }
                }
            } finally {
                if ( streamReader != null ) streamReader.close();
            }

            return manifest;
        }
    }
}
