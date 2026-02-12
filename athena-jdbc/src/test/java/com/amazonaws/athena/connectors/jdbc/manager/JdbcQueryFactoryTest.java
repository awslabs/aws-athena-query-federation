/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.manager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.lang.reflect.Method;

@RunWith(MockitoJUnitRunner.class)
public class JdbcQueryFactoryTest
{
    private static final String VALID_TEMPLATE_NAME = "test_template";
    private JdbcQueryFactory queryFactory;

    @Before
    public void setUp()
    {
        queryFactory = new JdbcQueryFactory("JdbcBase.stg");
    }
    
    @Test
    public void getQueryTemplate_ValidTemplateName_ReturnsST()
    {
        ST template = queryFactory.getQueryTemplate(VALID_TEMPLATE_NAME);
        
        Assert.assertNotNull("Template should not be null", template);
    }

    @Test
    public void getQueryTemplate_InvalidTemplateName_ReturnsNull()
    {
        ST template = queryFactory.getQueryTemplate("nonexistent_template");
        
        Assert.assertNull("Template should be null for invalid name", template);
    }

    @Test
    public void getQueryTemplate_NullTemplateName_ReturnsNull()
    {
        ST template = queryFactory.getQueryTemplate(null);
        
        Assert.assertNull("Template should be null for null name", template);
    }
    
    @Test
    public void createGroupFile_ValidClasspathTemplate_ReturnsSTGroupFile() throws Exception
    {
        Method method = JdbcQueryFactory.class.getDeclaredMethod("createGroupFile");
        method.setAccessible(true);

        STGroupFile result = (STGroupFile) method.invoke(queryFactory);

        Assert.assertNotNull("Expected non-null result from createGroupFile", result);
    }

    @Test
    public void createLocalGroupFile_LocalFileExists_ReturnsSTGroupFile() throws Exception
    {
        Method method = JdbcQueryFactory.class.getDeclaredMethod("createLocalGroupFile");
        method.setAccessible(true);

        STGroupFile result = (STGroupFile) method.invoke(queryFactory);
        Assert.assertNotNull("Expected non-null result from createLocalGroupFile", result);
    }
    
    @Test
    public void getQueryTemplate_DifferentTemplateNames_ReturnsDifferentTemplates()
    {
        ST template1 = queryFactory.getQueryTemplate(VALID_TEMPLATE_NAME);
        ST template2 = queryFactory.getQueryTemplate("comparison_predicate");

        // This test verifies the factory can handle different template names
        Assert.assertNotNull("Factory should handle different template names", queryFactory);
        // Both templates should exist and be non-null
        Assert.assertNotNull("First template should not be null", template1);
        Assert.assertNotNull("Second template should not be null", template2);
    }
    
    @Test
    public void getQueryTemplate_ValidPredicateTemplate_ReturnsTemplate()
    {
        ST template = queryFactory.getQueryTemplate("in_predicate");
        
        Assert.assertNotNull("IN predicate template should not be null", template);
    }

    @Test
    public void getQueryTemplate_ValidNullPredicateTemplate_ReturnsTemplate()
    {
        ST template = queryFactory.getQueryTemplate("null_predicate");
        
        Assert.assertNotNull("NULL predicate template should not be null", template);
    }

    @Test
    public void getQueryTemplate_ValidRangePredicateTemplate_ReturnsTemplate()
    {
        ST template = queryFactory.getQueryTemplate("range_predicate");
        
        Assert.assertNotNull("Range predicate template should not be null", template);
    }

    @Test
    public void getQueryTemplate_ValidOrPredicateTemplate_ReturnsTemplate()
    {
        ST template = queryFactory.getQueryTemplate("or_predicate");
        
        Assert.assertNotNull("OR predicate template should not be null", template);
    }
}

