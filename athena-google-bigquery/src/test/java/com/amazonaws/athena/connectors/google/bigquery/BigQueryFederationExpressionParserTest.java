/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;
import static org.junit.Assert.assertEquals;

public class BigQueryFederationExpressionParserTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryFederationExpressionParserTest.class);
    private static final String COLUMN_QUOTE_CHAR = "\"";
    private static final String CONSTANT_QUOTE_CHAR = "'";

    BlockAllocator blockAllocator;
    ArrowType intType = new ArrowType.Int(32, true);
    BigQueryFederationExpressionParser bigQueryExpressionParser;

    @Before
    public void setup() {
        blockAllocator = new BlockAllocatorImpl();
        bigQueryExpressionParser = new BigQueryFederationExpressionParser();
    }

    private ConstantExpression buildIntConstantExpression()
    {
        Block b = BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, new ArrowType.Int(32, true), ImmutableList.of(10));
        ConstantExpression intConstantExpression = new ConstantExpression(b, new ArrowType.Int(32, true));
        return intConstantExpression;
    }

    @Test
    public void testParseConstantExpression()
    {
        ConstantExpression ten = buildIntConstantExpression();
        assertEquals(bigQueryExpressionParser.parseConstantExpression(ten), "10");
    }


    @Test
    public void testParseConstantListOfInts()
    {
        ConstantExpression listOfNums = new ConstantExpression(
                BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, new ArrowType.Int(32, true),
                        ImmutableList.of(25, 10, 5, 1)), new ArrowType.Int(32, true)
        );
        assertEquals(bigQueryExpressionParser.parseConstantExpression(listOfNums), "25,10,5,1");
    }

    @Test
    public void testParseConstantListOfStrings()
    {
        Collection<Object> rawStrings = ImmutableList.of("fed", "er", "ation");
        ConstantExpression listOfStrings = new ConstantExpression(
                BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, new ArrowType.Utf8(),
                        rawStrings), new ArrowType.Utf8()
        );

        List<String> quotedStrings = rawStrings.stream().map(str -> CONSTANT_QUOTE_CHAR + str + CONSTANT_QUOTE_CHAR).collect(Collectors.toList());
        String expected = Joiner.on(",").join(quotedStrings);
        String actual = bigQueryExpressionParser.parseConstantExpression(listOfStrings);
        assertEquals(expected, actual);
    }


    @Test
    public void testParseVariableExpression()
    {
        VariableExpression colThree = new VariableExpression("colThree", intType);
        assertEquals(bigQueryExpressionParser.parseVariableExpression(colThree),  "colThree" );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSqlForComplexExpressionContent_InvalidUnaryInput()
    {
        FunctionName functionName = StandardFunctions.NEGATE_FUNCTION_NAME.getFunctionName();
        bigQueryExpressionParser.mapFunctionToDataSourceSyntax(functionName, intType, ImmutableList.of("1", "2"));
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_UnaryFunction()
    {
        FunctionName negateFunction = StandardFunctions.NEGATE_FUNCTION_NAME.getFunctionName();
        String negateClause = bigQueryExpressionParser.mapFunctionToDataSourceSyntax(negateFunction, intType, ImmutableList.of("110"));
        assertEquals(negateClause, "(-110)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSqlForComplexExpressionContent_InvalidBinaryInput()
    {
        FunctionName functionName = StandardFunctions.ADD_FUNCTION_NAME.getFunctionName();
        bigQueryExpressionParser.mapFunctionToDataSourceSyntax(functionName, intType, ImmutableList.of("1"));
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_BinaryFunction()
    {
        FunctionName subFunction = StandardFunctions.SUBTRACT_FUNCTION_NAME.getFunctionName();
        String subClause = bigQueryExpressionParser.mapFunctionToDataSourceSyntax(subFunction, intType, ImmutableList.of("`col1`", "10"));
        assertEquals(subClause, "(`col1` - 10)");
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_VarargFunction()
    {
        FunctionName inFunction = StandardFunctions.IN_PREDICATE_FUNCTION_NAME.getFunctionName();
        FunctionName arrayFunction = StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME.getFunctionName();
        String arrayClause = bigQueryExpressionParser.mapFunctionToDataSourceSyntax(arrayFunction, intType, ImmutableList.of("25", "10", "5", "1"));
        assertEquals("(25, 10, 5, 1)", arrayClause);
        String inClause = bigQueryExpressionParser.mapFunctionToDataSourceSyntax(inFunction, intType, ImmutableList.of("`coinValueColumn`", arrayClause));
        assertEquals(inClause, "(`coinValueColumn` IN (25, 10, 5, 1))");
    }

    @Test
    public void testComplexExpressions_Simple()
    {
        // colOne + colThree < 10
        VariableExpression colOne = new VariableExpression("colOne", intType);
        VariableExpression colThree = new VariableExpression("colThree", intType);
        List<FederationExpression> addArguments = ImmutableList.of(colOne, colThree);
        FederationExpression addFunctionCall = new FunctionCallExpression(
                Types.MinorType.FLOAT8.getType(),
                StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(),
                addArguments);

        ConstantExpression ten = buildIntConstantExpression();
        List<FederationExpression> ltArguments = ImmutableList.of(addFunctionCall, ten);

        FederationExpression fullExpression = new FunctionCallExpression(
                ArrowType.Bool.INSTANCE,
                StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(),
                ltArguments);

        assertEquals("((colOne + colThree) < 10)", bigQueryExpressionParser.parseFunctionCallExpression((FunctionCallExpression) fullExpression));
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
                ImmutableList.of(colOne, colTwo)
        );

        // (colOne + colTwo) > colThree
        FederationExpression colThree = new VariableExpression("colThree", intType);
        FederationExpression gtFunction = new FunctionCallExpression(
                ArrowType.Bool.INSTANCE,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(addFunction, colThree)
        );


        // colFour IN ("banana", "dragonfruit")
        FederationExpression colFour = new VariableExpression("colFour", ArrowType.Utf8.INSTANCE);
        FederationExpression fruitList = new FunctionCallExpression(
                ArrowType.Utf8.INSTANCE,
                StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(
                        new ConstantExpression(BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, ArrowType.Utf8.INSTANCE,
                                ImmutableList.of("banana", "dragonfruit")), ArrowType.Utf8.INSTANCE)
                )
        );
        FederationExpression inFunction = new FunctionCallExpression(
                ArrowType.Bool.INSTANCE,
                StandardFunctions.IN_PREDICATE_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(colFour, fruitList)
        );

        // (colOne + colTwo > colThree) AND (colFour IN ("banana", "dragonfruit"))
        FederationExpression andFunction = new FunctionCallExpression(
                ArrowType.Bool.INSTANCE,
                StandardFunctions.AND_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(gtFunction, inFunction)
        );

        FederationExpression fruitConstant = new ConstantExpression(
                BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, new ArrowType.Utf8(), ImmutableList.of("fruit")),
                new ArrowType.Utf8()
        );
        FederationExpression notFunction = new FunctionCallExpression(
                ArrowType.Bool.INSTANCE,
                StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(colFour, fruitConstant)
        );

        FederationExpression orFunction = new FunctionCallExpression(
                ArrowType.Bool.INSTANCE,
                StandardFunctions.OR_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(andFunction, notFunction)
        );

        String fullClause = bigQueryExpressionParser.parseFunctionCallExpression((FunctionCallExpression) orFunction);
        // actual is ((((colOne + colTwo) > colThree) AND (colFour IN (banana, dragonfruit))) OR (colFour <> fruit))
        String expected = "((((" + quoteColumn("colOne") + " + " + quoteColumn("colTwo") + ") > "
                + quoteColumn("colThree") + ") AND (" + quoteColumn("colFour") +
                " IN (" + quoteConstant("banana") + "," + quoteConstant("dragonfruit") + "))) OR (" + quoteColumn("colFour") + " <> " + quoteConstant("fruit") + "))";
        assertEquals(expected, fullClause);
    }

    @Test
    public void testComplexExpressions_Simple_With_Date()
    {
        // colOne + colThree < 10
        VariableExpression colOne = new VariableExpression("colOne", new ArrowType.Date(DateUnit.DAY));

        Block b = BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, new ArrowType.Date(DateUnit.DAY),
                ImmutableList.of(TimeUnit.MILLISECONDS.toDays(Date.valueOf("2020-01-05").getTime())));
        ConstantExpression dateConstantExpression = new ConstantExpression(b, new ArrowType.Int(32, true));


        FederationExpression gtFunctionCall = new FunctionCallExpression(
                Types.MinorType.DATEDAY.getType(),
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(colOne, dateConstantExpression));

        String s = bigQueryExpressionParser.parseFunctionCallExpression((FunctionCallExpression) gtFunctionCall);
        assertEquals("(colOne > '2020-01-05')", bigQueryExpressionParser.parseFunctionCallExpression((FunctionCallExpression) gtFunctionCall));
    }

    private String quoteColumn(String columnName)
    {
        return columnName;
    }

    private String quoteConstant(String constant)
    {
        return CONSTANT_QUOTE_CHAR + constant + CONSTANT_QUOTE_CHAR;
    }
}
