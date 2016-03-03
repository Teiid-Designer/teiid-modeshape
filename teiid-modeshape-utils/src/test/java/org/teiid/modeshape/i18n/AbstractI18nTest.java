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
package org.teiid.modeshape.i18n;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Locale;
import java.util.Set;
import org.junit.Test;
import org.modeshape.common.annotation.Category;
import org.modeshape.common.annotation.Description;
import org.modeshape.common.annotation.Label;
import org.modeshape.common.i18n.I18n;

/**
 * @author John Verhaeg
 */
public abstract class AbstractI18nTest {

    protected static final String[] ANNOTATION_NAMES = { "Description", "Category", "Label" };

    private final Class< ? > i18nClass;

    protected AbstractI18nTest( final Class< ? > i18nClass ) {
        this.i18nClass = i18nClass;
    }

    @Test
    public void shouldNotHaveProblems() throws Exception {
        for ( final Field fld : this.i18nClass.getDeclaredFields() ) {
            if ( ( fld.getType() == I18n.class ) && ( ( fld.getModifiers() & Modifier.PUBLIC ) == Modifier.PUBLIC )
                 && ( ( fld.getModifiers() & Modifier.STATIC ) == Modifier.STATIC )
                 && ( ( fld.getModifiers() & Modifier.FINAL ) != Modifier.FINAL ) ) {
                final I18n i18n = ( I18n )fld.get( null );
                if ( i18n.hasProblem() ) {
                    fail( i18n.problem() );
                }
            }
        }
        // Check for global problems after checking field problems since global problems are detected lazily upon field usage
        final Set< Locale > locales = I18n.getLocalizationProblemLocales( this.i18nClass );
        if ( !locales.isEmpty() ) {
            for ( final Locale locale : locales ) {
                final Set< String > problems = I18n.getLocalizationProblems( this.i18nClass, locale );
                try {
                    assertThat( problems.isEmpty(), is( true ) );
                } catch ( final AssertionError error ) {
                    fail( problems.iterator().next() );
                }
            }
        }
    }

    protected void verifyI18nForAnnotation( final Annotation annotation,
                                            final Object annotatedObject ) throws Exception {
        String i18nIdentifier;
        Class< ? > i18nClass;
        if ( annotation instanceof Category ) {
            final Category cat = ( Category )annotation;
            i18nClass = cat.i18n();
            i18nIdentifier = cat.value();
        } else if ( annotation instanceof Description ) {
            final Description desc = ( Description )annotation;
            i18nClass = desc.i18n();
            i18nIdentifier = desc.value();
        } else if ( annotation instanceof Label ) {
            final Label label = ( Label )annotation;
            i18nClass = label.i18n();
            i18nIdentifier = label.value();
        } else {
            return;
        }
        assertThat( i18nClass, is( notNullValue() ) );
        assertThat( i18nIdentifier, is( notNullValue() ) );
        try {
            final Field fld = i18nClass.getField( i18nIdentifier );
            assertThat( fld, is( notNullValue() ) );
            // Now check the I18n field ...
            if ( ( fld.getType() == I18n.class ) && ( ( fld.getModifiers() & Modifier.PUBLIC ) == Modifier.PUBLIC )
                 && ( ( fld.getModifiers() & Modifier.STATIC ) == Modifier.STATIC )
                 && ( ( fld.getModifiers() & Modifier.FINAL ) != Modifier.FINAL ) ) {
                final I18n i18n = ( I18n )fld.get( null );
                if ( i18n.hasProblem() ) {
                    fail( i18n.problem() );
                }
            }
        } catch ( final NoSuchFieldException e ) {
            fail( "Missing I18n field on " + i18nClass.getName() + " for " + annotation + " on " + annotatedObject );
        }
    }

    /**
     * Utility method that can be used to verify that an I18n field exists for all of the I18n-related annotations on the supplied
     * object. I18n-related annotations include {@link Description}, {@link Label}, and {@link Category}.
     *
     * @param annotated the object that has field or method annotations
     * @throws Exception if there is a problem
     */
    protected void verifyI18nForAnnotationsOnObject( final Object annotated ) throws Exception {
        // Check the known annotations that work with I18ns ...
        final Class< ? > clazz = annotated.getClass();
        for ( final Field field : clazz.getDeclaredFields() ) {
            for ( final Annotation annotation : field.getAnnotations() ) {
                verifyI18nForAnnotation( annotation, field );
            }
        }
        for ( final Method method : clazz.getDeclaredMethods() ) {
            for ( final Annotation annotation : method.getAnnotations() ) {
                verifyI18nForAnnotation( annotation, method );
            }
        }
    }
}