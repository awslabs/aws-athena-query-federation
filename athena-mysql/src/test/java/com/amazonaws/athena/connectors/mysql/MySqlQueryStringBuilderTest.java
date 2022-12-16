/*-
 * #%L
 * athena-mysql
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.mysql;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.functions.StandardFunctions;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Joiner;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

/**
 * Test is realy a JDBC package test. But the JdbcSplitQueryBuilder class is abstract and we can only instantiate it, and therefore test it, from a subclass's
 * implementation. And to avoid a circular dependency (like importing MySQL classes into the JDBC module), we just let this live in the mysql module.
 */
public class MySqlQueryStringBuilderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlQueryStringBuilderTest.class);
    private static final String QUOTE_CHAR = MySqlRecordHandler.MYSQL_QUOTE_CHARACTER;

    BlockAllocator blockAllocator;
    ArrowType intType = new ArrowType.Int(32, true);
    JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    @Before
    public void setup() {
        blockAllocator = new BlockAllocatorImpl();
        jdbcSplitQueryBuilder = new MySqlQueryStringBuilder("`");
    }

    private ConstantExpression buildIntConstantExpression()
    {
        Block b = BlockUtils.newBlock(blockAllocator, "dummyColumn", new ArrowType.Int(32, true), List.of(10));
        ConstantExpression intConstantExpression = new ConstantExpression(b, new ArrowType.Int(32, true));
        return intConstantExpression;
    }

    @Test
    public void testParseConstantExpression()
    {
        ConstantExpression ten = buildIntConstantExpression();
        assertEquals(jdbcSplitQueryBuilder.parseConstantExpression(ten, QUOTE_CHAR), "10");
    }


    @Test
    public void testParseConstantListOfInts()
    {
        ConstantExpression listOfNums = new ConstantExpression(
            BlockUtils.newBlock(blockAllocator, "dummyColumn", new ArrowType.Int(32, true),
            List.of(25, 10, 5, 1)), new ArrowType.Int(32, true)
        );
        assertEquals(jdbcSplitQueryBuilder.parseConstantExpression(listOfNums, QUOTE_CHAR), "25,10,5,1");
    }

    @Test
    public void testParseConstantListOfStrings()
    {
        Collection<Object> rawStrings = List.of("fed", "er", "ation");
        ConstantExpression listOfStrings = new ConstantExpression(
            BlockUtils.newBlock(blockAllocator, "dummyColumn", new ArrowType.Utf8(),
            rawStrings), new ArrowType.Utf8()
        );

        List<String> quotedStrings = rawStrings.stream().map(str -> QUOTE_CHAR + str + QUOTE_CHAR).collect(Collectors.toList());
        String expected = Joiner.on(",").join(quotedStrings);
        String actual = jdbcSplitQueryBuilder.parseConstantExpression(listOfStrings, QUOTE_CHAR);
        LOGGER.error("Formed expected string {}", expected);
        LOGGER.error("Found actual string {}", actual);
        
        assertEquals(expected, actual);
    }


    @Test
    public void testParseVariableExpression()
    {
        VariableExpression colThree = new VariableExpression("colThree", intType);
        assertEquals(jdbcSplitQueryBuilder.parseVariableExpression(colThree), "colThree");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSqlForComplexExpressionContent_InvalidUnaryInput()
    {
        FunctionName functionName = StandardFunctions.NEGATE_FUNCTION_NAME.getFunctionName();
        jdbcSplitQueryBuilder.mapFunctionToDataSourceSyntax(functionName, intType, List.of("1", "2"));
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_UnaryFunction()
    {
        FunctionName negateFunction = StandardFunctions.NEGATE_FUNCTION_NAME.getFunctionName();
        String negateClause = jdbcSplitQueryBuilder.mapFunctionToDataSourceSyntax(negateFunction, intType, List.of("110"));
        assertEquals(negateClause, "(-110)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSqlForComplexExpressionContent_InvalidBinaryInput()
    {
        FunctionName functionName = StandardFunctions.ADD_FUNCTION_NAME.getFunctionName();
        jdbcSplitQueryBuilder.mapFunctionToDataSourceSyntax(functionName, intType, List.of("1"));
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_BinaryFunction()
    {
        FunctionName subFunction = StandardFunctions.SUBTRACT_FUNCTION_NAME.getFunctionName();
        String subClause = jdbcSplitQueryBuilder.mapFunctionToDataSourceSyntax(subFunction, intType, List.of("col1", "10"));
        assertEquals(subClause, "(col1 - 10)");
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_VarargFunction()
    {
        FunctionName inFunction = StandardFunctions.IN_PREDICATE_FUNCTION_NAME.getFunctionName();
        String inClause = jdbcSplitQueryBuilder.mapFunctionToDataSourceSyntax(inFunction, intType, List.of("coinValueColumn", "25,10,5,1"));
        assertEquals(inClause, "(coinValueColumn IN (25,10,5,1))");
    }
    
    @Test
    public void testComplexExpressions_Simple()
    {
        // colOne + colThree < 10
        VariableExpression colOne = new VariableExpression("colOne", intType);
        VariableExpression colThree = new VariableExpression("colThree", intType);
        List<FederationExpression> addArguments = List.of(colOne, colThree);
        FederationExpression addFunctionCall = new FunctionCallExpression(
            Types.MinorType.FLOAT8.getType(),
            StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(),
            addArguments);
        
        ConstantExpression ten = buildIntConstantExpression();
        List<FederationExpression> ltArguments = List.of(addFunctionCall, ten);

        FederationExpression fullExpression = new FunctionCallExpression(
            ArrowType.Bool.INSTANCE,
            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(),
            ltArguments);

        assertEquals("((colOne + colThree) < 10)", jdbcSplitQueryBuilder.parseFunctionCallExpression((FunctionCallExpression) fullExpression, QUOTE_CHAR));
    }

    // (colOne + colTwo > colThree) AND (colFour IN ("banana", "dragonfruit"))
    // OR
    // colFour <> "fruit"
    @Test
    public void testComplexExpressions_Deep()
    {
        // colOne + colTwo
        FederationExpression colOne = new VariableExpression("colOne", intType);
        FederationExpression colTwo = new VariableExpression("colTwo", intType);
        FederationExpression addFunction = new FunctionCallExpression(
            Types.MinorType.INT.getType(),
            StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(),
            List.of(colOne, colTwo)
            );

        // (colOne + colTwo) > colThree
        FederationExpression colThree = new VariableExpression("colThree", intType);
        FederationExpression gtFunction = new FunctionCallExpression(
            ArrowType.Bool.INSTANCE,
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(),
            List.of(addFunction, colThree)
        );
        

        // colFour IN ("banana", "dragonfruit")
        FederationExpression colFour = new VariableExpression("colFour", new ArrowType.Utf8());
        FederationExpression fruitList = new ConstantExpression(
            BlockUtils.newBlock(blockAllocator, "dummyColumn", new ArrowType.Utf8(),
            List.of("banana", "dragonfruit")), new ArrowType.Utf8()
        );
        FederationExpression inFunction = new FunctionCallExpression(
            ArrowType.Bool.INSTANCE,
            StandardFunctions.IN_PREDICATE_FUNCTION_NAME.getFunctionName(),
            List.of(colFour, fruitList)
        );

        // (colOne + colTwo > colThree) AND (colFour IN ("banana", "dragonfruit"))
        FederationExpression andFunction = new FunctionCallExpression(
            ArrowType.Bool.INSTANCE,
            StandardFunctions.AND_FUNCTION_NAME.getFunctionName(),
            List.of(gtFunction, inFunction)
        );

        FederationExpression fruitConstant = new ConstantExpression(
            BlockUtils.newBlock(blockAllocator, "anotherDummyColumn", new ArrowType.Utf8(), List.of("fruit")),
            new ArrowType.Utf8()
        );
        FederationExpression notFunction = new FunctionCallExpression(
            ArrowType.Bool.INSTANCE,
            StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME.getFunctionName(),
            List.of(colFour, fruitConstant)
        );

        FederationExpression orFunction = new FunctionCallExpression(
            ArrowType.Bool.INSTANCE,
            StandardFunctions.OR_FUNCTION_NAME.getFunctionName(),
            List.of(andFunction, notFunction)
        );

        String fullClause = jdbcSplitQueryBuilder.parseFunctionCallExpression((FunctionCallExpression) orFunction, QUOTE_CHAR);
        // actual is ((((colOne + colTwo) > colThree) AND (colFour IN (banana))) OR (colFour <> fruit))
        String expected = "((((colOne + colTwo) > colThree) AND (colFour IN (" + QUOTE_CHAR + "banana" + QUOTE_CHAR + "," + QUOTE_CHAR + "dragonfruit" + QUOTE_CHAR + "))) OR (colFour <> "+ QUOTE_CHAR + "fruit" + QUOTE_CHAR + "))";
        assertEquals(expected, fullClause);
    }
}
