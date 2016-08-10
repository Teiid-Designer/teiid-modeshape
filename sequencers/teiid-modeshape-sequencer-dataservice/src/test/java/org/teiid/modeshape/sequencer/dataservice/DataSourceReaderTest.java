/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.modeshape.sequencer.dataservice;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import java.io.InputStream;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.teiid.modeshape.sequencer.dataservice.DataSource.Type;

public final class DataSourceReaderTest {

    private DataSourceReader reader;

    @Before
    public void constructReader() throws Exception {
        this.reader = new DataSourceReader();
    }

    private InputStream getStream( final String filePath ) {
        return getClass().getClassLoader().getResourceAsStream( filePath );
    }

    @Test( expected = Exception.class )
    public void shouldFailReadingDataSourceWithoutClassName() throws Exception {
        this.reader.read( getStream( "datasource/missingClassNameDatasource.tds" ) );
    }

    @Test( expected = Exception.class )
    public void shouldFailReadingDataSourceWithoutDriverName() throws Exception {
        this.reader.read( getStream( "datasource/missingDriverNameDatasource.tds" ) );
    }

    @Test( expected = Exception.class )
    public void shouldFailReadingDataSourceWithoutJndiName() throws Exception {
        this.reader.read( getStream( "datasource/missingJndiNameDatasource.tds" ) );
    }

    @Test
    public void shouldParseDataSourceWithoutProperties() throws Exception {
        final DataSource ds = this.reader.read( getStream( "datasource/noPropsDatasource.tds" ) );
        assertThat( ds, is( notNullValue() ) );
        assertThat( ds.getName(), is( "noPropsSource" ) );
        assertThat( ds.getDriverName(), is( "dsDriver" ) );
        assertThat( ds.getJndiName(), is( "java:/jdbcSource" ) );
        assertThat( ds.getClassName(), is( nullValue() ) );
        assertThat( ds.getType(), is( Type.JDBC ) );
        assertThat( ds.getProperties().isEmpty(), is( true ) );
    }

    @Test
    public void shouldParseJdbcDataSource() throws Exception {
        final DataSource ds = this.reader.read( getStream( "datasource/jdbcDatasource.tds" ) );
        assertThat( ds, is( notNullValue() ) );
        assertThat( ds.getName(), is( "jdbcSource" ) );
        assertThat( ds.getDriverName(), is( "dsDriver" ) );
        assertThat( ds.getJndiName(), is( "java:/jdbcSource" ) );
        assertThat( ds.getClassName(), is( nullValue() ) );
        assertThat( ds.getType(), is( Type.JDBC ) );
        assertThat( ds.getProperties().size(), is( 4 ) );

        final Properties props = ds.getProperties();
        assertThat( props.getProperty( "prop1" ), is( "one" ) );
        assertThat( props.getProperty( "prop2" ), is( "two" ) );
        assertThat( props.getProperty( "prop3" ), is( "three" ) );
        assertThat( props.getProperty( "prop4" ), is( "four" ) );
    }

    @Test
    public void shouldParseResourceAdapterDataSource() throws Exception {
        final DataSource ds = this.reader.read( getStream( "datasource/raDatasource.tds" ) );
        assertThat( ds, is( notNullValue() ) );
        assertThat( ds.getName(), is( "raSource" ) );
        assertThat( ds.getDriverName(), is( "dsDriver" ) );
        assertThat( ds.getJndiName(), is( "java:/jndiName" ) );
        assertThat( ds.getClassName(), is( "dsClassname" ) );
        assertThat( ds.getType(), is( Type.RESOURCE ) );
        assertThat( ds.getProperties().size(), is( 2 ) );

        final Properties props = ds.getProperties();
        assertThat( props.getProperty( "prop1" ), is( "prop1Value" ) );
        assertThat( props.getProperty( "prop2" ), is( "prop2Value" ) );
    }

}
