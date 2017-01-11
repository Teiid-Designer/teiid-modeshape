/*
 * JBoss, Home of Professional Open Source.
*
* See the LEGAL.txt file distributed with this work for information regarding copyright ownership and licensing.
*
* See the AUTHORS.txt file distributed with this work for a full listing of individual contributors.
*/
package org.teiid.modeshape.sequencer.dataservice;

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
    	    final String[] paths = new String[ entries.length ];
    	    int i = 0;
    	    
    	    for ( final DataServiceEntry entry : entries ) {
    	    	    paths[ i++ ] = entry.getPath();
    	    }

    	    return paths;
    }

    private PublishPolicy publishPolicy;
    private String path;

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
     * @return the publish/deploy/upload/save policy(can be <code>null</code>)
     */
    public PublishPolicy getPublishPolicy() {
        return this.publishPolicy;
    }

    /**
     * @param path the path within the archive (can be <code>null</code> or empty)
     */
    public void setPath( final String path ) {
        this.path = path;
    }

    /**
     * @param policy the publish policy (can be <code>null</code> if default policy is wanted)
     * @see PublishPolicy#DEFAULT
     */
    public void setPublishPolicy( final PublishPolicy policy ) {
        this.publishPolicy = policy;
    }

    /**
     * Indicates how the entry should uploaded and/or deployed.
     */
    public enum PublishPolicy {

        /**
         * Always publish the file.
         */
        ALWAYS( "always" ),

        /**
         * Only publish if not already published.
         */
        IF_MISSING( "ifMissing" ),

        /**
         * Never publish the file.
         */
        NEVER( "never" );

        /**
         * The default policy.
         *
         * @see #IF_MISSING
         */
        public static final PublishPolicy DEFAULT = IF_MISSING;

        /**
         * @param xml the XML value whose type is being requested (can be <code>null</code> or empty)
         * @return the appropriate type or <code>null</code> if not found
         */
        public static PublishPolicy fromXml( final String xml ) {
            for ( final PublishPolicy type : values() ) {
                if ( type.xml.equals( xml ) ) {
                    return type;
                }
            }

            return null;
        }

        private final String xml;

        private PublishPolicy( final String xmlValue ) {
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
