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

import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;
import static org.junit.Assert.assertEquals;

// TODO in the future - create a base FederationExpressionParser test class.
public class MySqlFederationExpressionParserTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlFederationExpressionParserTest.class);
    private static final String COLUMN_QUOTE_CHAR = MySqlRecordHandler.MYSQL_QUOTE_CHARACTER;
    private static final String CONSTANT_QUOTE_CHAR = "'";

    BlockAllocator blockAllocator;
    ArrowType intType = new ArrowType.Int(32, true);
    FederationExpressionParser federationExpressionParser;

    @Before
    public void setup() {
        blockAllocator = new BlockAllocatorImpl();
        federationExpressionParser = new MySqlFederationExpressionParser("`");
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
        List<TypeAndValue> accumulator = new ArrayList<>();
        ConstantExpression ten = buildIntConstantExpression();
        assertEquals(federationExpressionParser.parseConstantExpression(ten, accumulator), "?");
        assertEquals(1, accumulator.size());
        assertEquals(accumulator.get(0).getType(), Types.MinorType.INT.getType());
        assertEquals(accumulator.get(0).getValue(), 10);
    }


    @Test
    public void testParseConstantListOfInts()
    {
        List<TypeAndValue> accumulator = new ArrayList<>();
        ConstantExpression listOfNums = new ConstantExpression(
            BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, new ArrowType.Int(32, true),
                    ImmutableList.of(25, 10, 5, 1)
            )
                , new ArrowType.Int(32, true)
        );

        List<Integer> expectedValue = ImmutableList.of(25, 10, 5, 1);
        assertEquals(federationExpressionParser.parseConstantExpression(listOfNums, accumulator), "?,?,?,?");
        assertEquals(expectedValue.size(), accumulator.size());
        for(int i = 0; i < expectedValue.size(); i++) {
            assertEquals(accumulator.get(i).getType(), Types.MinorType.INT.getType());
            assertEquals(accumulator.get(i).getValue(), expectedValue.get(i));
        }
    }

    @Test
    public void testParseConstantListOfStrings()
    {
        List<TypeAndValue> accumulator = new ArrayList<>();
        List<Object> rawStrings = ImmutableList.of("fed", "er", "ation");
        ConstantExpression listOfStrings = new ConstantExpression(
            BlockUtils.newBlock(blockAllocator, DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME, new ArrowType.Utf8(),
            rawStrings), new ArrowType.Utf8()
        );

        String expected = Joiner.on(",").join(Collections.nCopies(3, "?"));
        String actual = federationExpressionParser.parseConstantExpression(listOfStrings, accumulator);
        assertEquals(expected, actual);
        assertEquals(rawStrings.size(), accumulator.size());
        for(int i = 0; i < rawStrings.size(); i++) {
            assertEquals(accumulator.get(i).getType(), ArrowType.Utf8.INSTANCE);
            assertEquals(String.valueOf(accumulator.get(i).getValue()), rawStrings.get(i));
        }
    }


    @Test
    public void testParseVariableExpression()
    {
        VariableExpression colThree = new VariableExpression("colThree", intType);
        assertEquals(federationExpressionParser.parseVariableExpression(colThree), COLUMN_QUOTE_CHAR + "colThree" + COLUMN_QUOTE_CHAR);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSqlForComplexExpressionContent_InvalidUnaryInput()
    {
        FunctionName functionName = StandardFunctions.NEGATE_FUNCTION_NAME.getFunctionName();
        federationExpressionParser.mapFunctionToDataSourceSyntax(functionName, intType, ImmutableList.of("1", "2"));
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_UnaryFunction()
    {
        FunctionName negateFunction = StandardFunctions.NEGATE_FUNCTION_NAME.getFunctionName();
        String negateClause = federationExpressionParser.mapFunctionToDataSourceSyntax(negateFunction, intType, ImmutableList.of("110"));
        assertEquals(negateClause, "(-110)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSqlForComplexExpressionContent_InvalidBinaryInput()
    {
        FunctionName functionName = StandardFunctions.ADD_FUNCTION_NAME.getFunctionName();
        federationExpressionParser.mapFunctionToDataSourceSyntax(functionName, intType, ImmutableList.of("1"));
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_BinaryFunction()
    {
        FunctionName subFunction = StandardFunctions.SUBTRACT_FUNCTION_NAME.getFunctionName();
        String subClause = federationExpressionParser.mapFunctionToDataSourceSyntax(subFunction, intType, ImmutableList.of("`col1`", "10"));
        assertEquals(subClause, "(`col1` - 10)");
    }

    @Test
    public void testCreateSqlForComplexExpressionContent_VarargFunction()
    {
        FunctionName inFunction = StandardFunctions.IN_PREDICATE_FUNCTION_NAME.getFunctionName();
        FunctionName arrayFunction = StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME.getFunctionName();
        String arrayClause = federationExpressionParser.mapFunctionToDataSourceSyntax(arrayFunction, intType, ImmutableList.of("25", "10", "5", "1"));
        assertEquals("(25, 10, 5, 1)", arrayClause);
        String inClause = federationExpressionParser.mapFunctionToDataSourceSyntax(inFunction, intType, ImmutableList.of("`coinValueColumn`", arrayClause));
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
        List<TypeAndValue> actualAccumulators = new ArrayList<>();
        assertEquals("((" + quoteColumn("colOne") + " + "  + quoteColumn("colThree") + ") < ?)", federationExpressionParser.parseFunctionCallExpression((FunctionCallExpression) fullExpression, actualAccumulators));
        assertEquals(actualAccumulators.size(), 1);
        assertEquals((int) actualAccumulators.get(0).getValue(), 10);
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

        List<TypeAndValue> actualAccumulators = new ArrayList<>();
        String fullClause = federationExpressionParser.parseFunctionCallExpression((FunctionCallExpression) orFunction, actualAccumulators);
        // SQL is ((((colOne + colTwo) > colThree) AND (colFour IN (banana, dragonfruit))) OR (colFour <> fruit))
        List<TypeAndValue> expectedAccumulators = ImmutableList.of(
                new TypeAndValue(Types.MinorType.VARCHAR.getType(), "banana"),
                new TypeAndValue(Types.MinorType.VARCHAR.getType(), "dragonfruit"),
                new TypeAndValue(Types.MinorType.VARCHAR.getType(), "fruit")
        );

        // actual prepare statement is ((((colOne + colTwo) > colThree) AND (colFour IN (?, ?))) OR (colFour <> ?))
        String expected = "((((" + quoteColumn("colOne") + " + " + quoteColumn("colTwo") + ") > "
                                  + quoteColumn("colThree") + ") AND (" + quoteColumn("colFour") +
                                  " IN (?,?))) OR (" + quoteColumn("colFour") + " <> ?))";
        assertEquals(expected, fullClause);

        for (int i = 0; i < expectedAccumulators.size(); i ++){
            assertEquals(actualAccumulators.get(i).getType(), expectedAccumulators.get(i).getType());
            assertEquals(String.valueOf(actualAccumulators.get(i).getValue()), expectedAccumulators.get(i).getValue());
        }
    }

    private String quoteColumn(String columnName)
    {
        return COLUMN_QUOTE_CHAR + columnName + COLUMN_QUOTE_CHAR;
    }

    private String quoteConstant(String constant)
    {
        return CONSTANT_QUOTE_CHAR + constant + CONSTANT_QUOTE_CHAR;
    }
}
