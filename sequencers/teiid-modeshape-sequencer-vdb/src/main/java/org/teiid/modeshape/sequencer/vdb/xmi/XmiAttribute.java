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
package org.teiid.modeshape.sequencer.vdb.xmi;

import org.modeshape.common.util.HashCode;
import org.modeshape.common.util.StringUtil;

/**
 * An attribute of an XMI element.
 */
public class XmiAttribute extends XmiBasePart implements XmiDescendent {

    private XmiElement parent;

    /**
     * @param name the attribute name (cannot be <code>null</code> or empty)
     */
    public XmiAttribute( String name ) {
        super(name);
    }

    /**
     * @see org.teiid.modeshape.sequencer.vdb.xmi.XmiBasePart#equals(java.lang.Object)
     */
    @Override
    public boolean equals( final Object obj ) {
        if (!super.equals(obj)) {
            return false;
        }

        if (!(obj instanceof XmiAttribute)) {
            return false;
        }

        final XmiAttribute that = (XmiAttribute)obj;

        // compare parent
        return (this.parent == that.parent);
    }

    /**
     * @see org.teiid.modeshape.sequencer.vdb.xmi.XmiBasePart#getNamespacePrefix()
     */
    @Override
    public String getNamespacePrefix() {
        final String prefix = super.getNamespacePrefix();

        if (StringUtil.isBlank(prefix)) {
            final XmiElement parent = getParent();

            if (parent != null) {
                return parent.getNamespacePrefix();
            }
        }

        return prefix;
    }

    /**
     * @see org.teiid.modeshape.sequencer.vdb.xmi.XmiBasePart#getNamespaceUri()
     */
    @Override
    public String getNamespaceUri() {
        final String uri = super.getNamespaceUri();

        if (StringUtil.isBlank(uri)) {
            final XmiElement parent = getParent();

            if (parent != null) {
                return parent.getNamespaceUri();
            }
        }

        return uri;
    }

    /**
     * @see org.teiid.modeshape.sequencer.vdb.xmi.XmiDescendent#getParent()
     */
    @Override
    public XmiElement getParent() {
        return this.parent;
    }

    /**
     * @see org.teiid.modeshape.sequencer.vdb.xmi.XmiBasePart#hashCode()
     */
    @Override
    public int hashCode() {
        return HashCode.compute(super.hashCode(), this.parent);
    }

    /**
     * @see org.teiid.modeshape.sequencer.vdb.xmi.XmiDescendent#setParent(org.teiid.modeshape.sequencer.vdb.xmi.XmiElement)
     */
    @Override
    public void setParent( final XmiElement parent ) {
        this.parent = parent;
    }
}
