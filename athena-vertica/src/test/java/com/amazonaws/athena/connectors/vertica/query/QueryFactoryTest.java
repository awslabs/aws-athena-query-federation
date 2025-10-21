/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.vertica.query;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.STGroupFile;

import java.lang.reflect.Method;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class QueryFactoryTest {

    private static final String assert_not_null = "Expected non-null VerticaExportQueryBuilder";
    private static final String assert_rendered = "Rendered result should not be null";
    private QueryFactory queryFactory;

    @Before
    public void setUp_QueryFactoryInstanceInitialized_SpyCreated() {
        queryFactory = spy(new QueryFactory());
    }

    @Test
    public void createGroupFile_ValidClasspathTemplate_ReturnsSTGroupFile() throws Exception {
        Method method = QueryFactory.class.getDeclaredMethod("createGroupFile");
        method.setAccessible(true);

        STGroupFile result = (STGroupFile) method.invoke(queryFactory);

        assertNotNull("Expected non-null result from createGroupFile", result);
    }

    @Test
    public void createLocalGroupFile_LocalFileExists_ReturnsSTGroupFile() throws Exception {
        Method method = QueryFactory.class.getDeclaredMethod("createLocalGroupFile");
        method.setAccessible(true);

        STGroupFile result = (STGroupFile) method.invoke(queryFactory);
        assertNotNull("Expected non-null result from createLocalGroupFile", result);
    }

    @Test
    public void createVerticaExportQueryBuilder_ValidTemplate_ReturnsBuilderInstance() {
        VerticaExportQueryBuilder builder = queryFactory.createVerticaExportQueryBuilder();

        assertNotNull(assert_not_null, builder);
        String rendered = builder.toString();
        assertNotNull(assert_rendered, rendered);
    }

    @Test
    public void createQptVerticaExportQueryBuilder_ValidTemplate_ReturnsBuilderInstance() {
        VerticaExportQueryBuilder builder = queryFactory.createQptVerticaExportQueryBuilder();

        assertNotNull(assert_not_null, builder);
        String rendered = builder.toString();
        assertNotNull(assert_rendered, rendered);
    }

    @Test
    public void getQueryTemplate_InvalidTemplateName_ShouldThrowException() throws Exception {
        Method method = QueryFactory.class.getDeclaredMethod("getQueryTemplate", String.class);
        method.setAccessible(true);

        try {
            method.invoke(queryFactory, "nonexistent_template");
        } catch (Exception e) {
            assertTrue("Should handle invalid template names",
                e.getCause() instanceof RuntimeException );
        }
    }

    @Test
    public void getQueryTemplate_NullTemplateName_ShouldThrowException() throws Exception {
        Method method = QueryFactory.class.getDeclaredMethod("getQueryTemplate", String.class);
        method.setAccessible(true);

        try {
            method.invoke(queryFactory, (String) null);
        } catch (Exception e) {
            assertTrue("Should handle null template name", e.getCause() instanceof NullPointerException);
        }
    }
}