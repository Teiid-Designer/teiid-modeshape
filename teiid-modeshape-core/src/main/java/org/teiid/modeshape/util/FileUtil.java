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
package org.teiid.modeshape.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class FileUtil {
    /**
     * The default buffer size for working with files. Defaults to {@value}.
     */
    public static int DEFAULT_BUFFER_SIZE = 8092;

    /**
     * Writes an <code>InputStream</code> to a file.
     * 
     * @param is the stream being written to the file (cannot be <code>null</code>)
     * @param f the file being written to (cannot be <code>null</code>)
     * @throws IOException if an IO error occurs
     */
    public static void write( final InputStream is, 
                              final File f ) throws IOException {
        f.delete();
        final File parentDir = f.getParentFile();

        if ( parentDir != null ) parentDir.mkdirs();

        FileOutputStream fio = null;
        BufferedOutputStream bos = null;

        try {
            fio = new FileOutputStream( f );
            bos = new BufferedOutputStream( fio );

            if ( DEFAULT_BUFFER_SIZE > 0 ) {
                final byte[] buff = new byte[ DEFAULT_BUFFER_SIZE ];
                int bytesRead;

                // Simple read/write loop.
                while ( -1 != ( bytesRead = is.read( buff, 0, buff.length ) ) ) {
                    bos.write( buff, 0, bytesRead );
                }
            }

            bos.flush();
        } finally {
            if ( bos != null ) bos.close();
            if ( fio != null ) fio.close();
        }
    }

    /**
     * Don't allow construction outside of this class.
     */
    private FileUtil() {
       // nothing to do	
    }

}
