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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TemplateBasedJdbcFederationExpressionParserTest
{
    private JdbcQueryFactory queryFactory;
    private TemplateBasedJdbcFederationExpressionParser parser;
    private BlockAllocator allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
        queryFactory = mock(JdbcQueryFactory.class);
        parser = new TemplateBasedJdbcFederationExpressionParser("\"")
        {
            @Override
            protected JdbcQueryFactory getQueryFactory()
            {
                return queryFactory;
            }
        };

        // Setup mock templates
        when(queryFactory.getQueryTemplate("comma_separated_list_with_parentheses"))
                .thenAnswer(inv -> new ST("(<items; separator=\", \">)"));
        when(queryFactory.getQueryTemplate("join_expression"))
                .thenAnswer(inv -> new ST("<items; separator=separator>"));
        when(queryFactory.getQueryTemplate("in_expression"))
                .thenAnswer(inv -> new ST("<column> IN <values>"));
        when(queryFactory.getQueryTemplate("like_expression"))
                .thenAnswer(inv -> new ST("(<column> LIKE <pattern>)"));
        when(queryFactory.getQueryTemplate("null_predicate"))
                .thenAnswer(inv -> new ST("<columnName> <if(isNull)>IS NULL<else>IS NOT NULL<endif>"));
        when(queryFactory.getQueryTemplate("unary_operator"))
                .thenAnswer(inv -> new ST("<operator><operand>"));
        when(queryFactory.getQueryTemplate("function_call_2args"))
                .thenAnswer(inv -> new ST("<functionName>(<arg1>, <arg2>)"));
        when(queryFactory.getQueryTemplate("is_distinct_from"))
                .thenAnswer(inv -> new ST("<left> IS DISTINCT FROM <right>"));
        when(queryFactory.getQueryTemplate("comma_separated_list"))
                .thenAnswer(inv -> new ST("<items; separator=\", \">"));
    }

    @Test
    public void mapFunctionToDataSourceSyntax_AddFunction_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(),
                new ArrowType.Int(32, true),
                List.of("a", "b")
        );
        Assert.assertEquals("a + b", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_InPredicate_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.IN_PREDICATE_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("col1", "(1, 2, 3)")
        );
        Assert.assertEquals("col1 IN (1, 2, 3)", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_IsNull_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.IS_NULL_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("col1")
        );
        Assert.assertEquals("col1 IS NULL", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_NotFunction_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.NOT_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("condition")
        );
        Assert.assertEquals("NOT condition", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_NullIf_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.NULLIF_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("a", "b")
        );
        Assert.assertEquals("NULLIF(a, b)", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_LikeFunction_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.LIKE_PATTERN_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("col1", "'pattern%'")
        );
        Assert.assertEquals("(col1 LIKE 'pattern%')", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_GreaterEqual_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("col1", "10")
        );
        Assert.assertEquals("col1 >= 10", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_ArithmeticFunctions_ReturnCorrectSql()
    {
        Assert.assertEquals("a - b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.SUBTRACT_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a * b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.MULTIPLY_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a / b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.DIVIDE_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a % b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.MODULUS_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("-a", parser.mapFunctionToDataSourceSyntax(StandardFunctions.NEGATE_FUNCTION_NAME.getFunctionName(), null, List.of("a")));
    }

    @Test
    public void mapFunctionToDataSourceSyntax_ComparisonFunctions_ReturnCorrectSql()
    {
        Assert.assertEquals("a = b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a <> b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a > b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a < b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a <= b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
    }

    @Test
    public void mapFunctionToDataSourceSyntax_LogicalFunctions_ReturnCorrectSql()
    {
        Assert.assertEquals("a AND b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.AND_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
        Assert.assertEquals("a OR b", parser.mapFunctionToDataSourceSyntax(StandardFunctions.OR_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b")));
    }

    @Test
    public void mapFunctionToDataSourceSyntax_IsDistinctFrom_ReturnsCorrectSql()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("col1", "col2")
        );
        Assert.assertEquals("col1 IS DISTINCT FROM col2", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_ComplexNested_ReturnsCorrectSql()
    {
        // Combined logical and comparison
        String result = parser.mapFunctionToDataSourceSyntax(
                StandardFunctions.AND_FUNCTION_NAME.getFunctionName(),
                null,
                List.of("(age > 18)", "(salary < 50000)")
        );
        Assert.assertEquals("(age > 18) AND (salary < 50000)", result);
    }

    @Test(expected = AthenaConnectorException.class)
    public void mapFunctionToDataSourceSyntax_NullArguments_ThrowsException()
    {
        parser.mapFunctionToDataSourceSyntax(StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(), null, null);
    }

    @Test(expected = AthenaConnectorException.class)
    public void mapFunctionToDataSourceSyntax_EmptyArguments_ThrowsException()
    {
        parser.mapFunctionToDataSourceSyntax(StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(), null, List.of());
    }

    @Test(expected = AthenaConnectorException.class)
    public void mapFunctionToDataSourceSyntax_InvalidArgCountUnary_ThrowsException()
    {
        parser.mapFunctionToDataSourceSyntax(StandardFunctions.IS_NULL_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b"));
    }

    @Test(expected = AthenaConnectorException.class)
    public void mapFunctionToDataSourceSyntax_InvalidArgCountBinary_ThrowsException()
    {
        parser.mapFunctionToDataSourceSyntax(StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(), null, List.of("a"));
    }

    @Test(expected = RuntimeException.class)
    public void mapFunctionToDataSourceSyntax_MissingTemplate_ThrowsException()
    {
        when(queryFactory.getQueryTemplate("join_expression")).thenReturn(null);
        parser.mapFunctionToDataSourceSyntax(StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(), null, List.of("a", "b"));
    }

    @Test
    public void parseVariableExpression_StandardExpression_ReturnsQuotedColumn()
    {
        VariableExpression expr = new VariableExpression("col1", new ArrowType.Int(32, true));
        String result = parser.parseVariableExpression(expr);
        Assert.assertEquals("\"col1\"", result);
    }

    @Test
    public void parseConstantExpression_SingleValue_ReturnsPlaceholderAndAccumulatesValue()
    {
        ArrowType type = new ArrowType.Int(32, true);
        Block block = BlockUtils.newBlock(allocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, type, 10);
        ConstantExpression constantExpression = new ConstantExpression(block, type);
        List<TypeAndValue> accumulator = new ArrayList<>();

        String result = parser.parseConstantExpression(constantExpression, accumulator);

        Assert.assertEquals("?", result);
        Assert.assertEquals(1, accumulator.size());
        Assert.assertEquals(10, accumulator.get(0).getValue());
    }

    @Test
    public void writeArrayConstructorClause_MultipleArguments_ReturnsFormattedArray()
    {
        String result = parser.writeArrayConstructorClause(
                new ArrowType.Int(32, true),
                List.of("1", "2", "3")
        );
        Assert.assertEquals("(1, 2, 3)", result);
    }
}
