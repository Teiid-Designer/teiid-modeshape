/*
 * JBoss, Home of Professional Open Source.
*
* See the LEGAL.txt file distributed with this work for information regarding copyright ownership and licensing.
*
* See the AUTHORS.txt file distributed with this work for a full listing of individual contributors.
*/
package org.teiid.modeshape.sequencer.dataservice;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents an entry in a Data Service archive.
 */
public class DataServiceEntry {

    static final String[] NO_PATHS = new String[ 0 ];

    /**
     * @param entries the entries whose paths are being requested (cannot be <code>null</code>)
     * @return the paths of the specified entries (never <code>null</code> but can be empty)
     */
    static String[] getPaths( final DataServiceEntry[] entries ) {
        return Arrays.asList( Objects.requireNonNull( entries,
                                                      "entries" ) ).stream().map( DataServiceEntry::getPath ).toArray( String[]::new );
    }

    private DeployPolicy deployPolicy = DeployPolicy.DEFAULT;
    private String path;

    /**
     * @return the deploy/upload/save policy or the default value if not set (never <code>null</code>)
     */
    public DeployPolicy getDeployPolicy() {
        return this.deployPolicy;
    }

    /**
     * @return the entry path's last segment (can be <code>null</code> or empty)
     */
    public String getEntryName() {
        final String path = getPath();

        if ( ( path == null ) || path.isEmpty() ) {
            return null;
        }

        final int index = path.lastIndexOf( '/' );

        if ( index == -1 ) {
            return path;
        }

        return path.substring( index + 1 );
    }

    /**
     * @return the entry path (can be <code>null</code> or empty)
     */
    public String getPath() {
        return this.path;
    }

    /**
     * @param policy the deploy policy (can be <code>null</code> if default policy is wanted)
     * @see DeployPolicy#DEFAULT
     */
    public void setDeployPolicy( final DeployPolicy policy ) {
        this.deployPolicy = ( ( policy == null ) ? DeployPolicy.DEFAULT : policy );
    }

    /**
     * @param path the path within the archive (can be <code>null</code> or empty)
     */
    public void setPath( final String path ) {
        this.path = path;
    }

    /**
     * Indicates how the entry should uploaded and/or deployed.
     */
    public enum DeployPolicy {

        /**
         * Always deploy the file.
         */
        ALWAYS( "always" ),

        /**
         * Only deploy the file if not already deployed.
         */
        IF_MISSING( "ifMissing" ),

        /**
         * Never deploy the file.
         */
        NEVER( "never" );

        /**
         * The default deployment method.
         *
         * @see #IF_MISSING
         */
        public static final DeployPolicy DEFAULT = IF_MISSING;

        /**
         * @param xml the XML value whose type is being requested (can be <code>null</code> or empty)
         * @return the appropriate type or the default value if not found
         * @see #DEFAULT
         */
        public static DeployPolicy fromXml( final String xml ) {
            for ( final DeployPolicy type : values() ) {
                if ( type.xml.equals( xml ) ) {
                    return type;
                }
            }

            return DEFAULT;
        }

        private final String xml;

        private DeployPolicy( final String xmlValue ) {
            this.xml = xmlValue;
        }

        /**
         * @return the value appropriate for an XML document (never <code>null</code> or empty)
         */
        public String toXml() {
            return this.xml;
        }

    }

}
