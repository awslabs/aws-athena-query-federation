/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.validation;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.base.Splitter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.validation.ConnectorValidator.BLOCK_ALLOCATOR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class provides the ability to transform a simple constraint grammar into an instance of {@link Constraints}.
 */
public class ConstraintParser
{
  private static final Splitter CONSTRAINT_SPLITTER = Splitter.on(',');

  private ConstraintParser()
  {
    // Intentionally left blank.
  }

  private enum LogicalOperator
  {
    EQ("=", 0)
    {
      ValueSet createValueSet(ArrowType type, Object operand)
      {
        if (operand == null) {
          return SortedRangeSet.newBuilder(type, true).build();
        }
        return SortedRangeSet.of(false, Range.equal(BLOCK_ALLOCATOR, type, operand));
      }
    },
    NEQ("!=", 1)
    {
      ValueSet createValueSet(ArrowType type, Object operand)
      {
        return EQ.createValueSet(type, operand).complement(BLOCK_ALLOCATOR);
      }
    },
    GT(">", 0)
    {
      ValueSet createValueSet(ArrowType type, Object operand)
      {
        return SortedRangeSet.of(false, Range.greaterThan(BLOCK_ALLOCATOR, type, operand));
      }
    },
    GTE(">=", 1)
    {
      ValueSet createValueSet(ArrowType type, Object operand)
      {
        return SortedRangeSet.of(false, Range.greaterThanOrEqual(BLOCK_ALLOCATOR, type, operand));
      }
    },
    LT("<", 0)
    {
      ValueSet createValueSet(ArrowType type, Object operand)
      {
        return SortedRangeSet.of(false, Range.lessThan(BLOCK_ALLOCATOR, type, operand));
      }
    },
    LTE("<=", 1)
    {
      ValueSet createValueSet(ArrowType type, Object operand)
      {
        return SortedRangeSet.of(false, Range.lessThanOrEqual(BLOCK_ALLOCATOR, type, operand));
      }
    };

    private final String operator;
    private final int rank;

    LogicalOperator(String operator, int rank)
    {
      this.operator = operator;
      this.rank = rank;
    }

    public String getOperator()
    {
      return operator;
    }

    public int getRank()
    {
      return rank;
    }

    abstract ValueSet createValueSet(ArrowType type, Object operand);
  }

  /**
   * This method takes in a table schema and a String representing the set of
   * simple contraints to be ANDed together and applied to that table.
   *
   * @param schema The schema of the table in question
   * @param input A comma-separated constraint String in the form of {field_name}{operator}{value}.
   *              The operators must be one of those available in {@link LogicalOperator}.
   *              Currently, we only support Boolean, Integer, Floating Point, Decimal, and String operands
   *              for this validator's constraints.
   * @return a {@link Constraints} object populated from the input string, un-constrained if input is not present
   */
  public static Constraints parseConstraints(Schema schema, Optional<String> input)
  {
    if (!input.isPresent() || input.get().trim().isEmpty()) {
      return new Constraints(Collections.EMPTY_MAP, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT);
    }

    Map<String, ArrowType> fieldTypes = schema.getFields().stream()
                                                .collect(Collectors.toMap(Field::getName, Field::getType));

    Map<String, ValueSet> constraints = new HashMap<>();
    Iterable<String> constraintStrings = CONSTRAINT_SPLITTER.split(input.get());
    constraintStrings.forEach(str -> parseAndAddConstraint(fieldTypes, constraints, str));
    return new Constraints(constraints, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT);
  }

  private static void parseAndAddConstraint(Map<String, ArrowType> fieldTypes,
                                            Map<String, ValueSet> constraints,
                                            String constraintString)
  {
    LogicalOperator matchedOperator = null;
    int bestMatchRank = Integer.MIN_VALUE;
    for (LogicalOperator operator : LogicalOperator.values()) {
      if (constraintString.contains(operator.getOperator()) && operator.getRank() > bestMatchRank) {
        matchedOperator = operator;
        bestMatchRank = operator.getRank();
      }
    }
    checkState(matchedOperator != null,
               String.format("No operators found in constraint string '%s'!"
                                     + " Allowable operators are %s", constraintString,
                             Arrays.stream(LogicalOperator.values())
                                     .map(LogicalOperator::getOperator)
                                     .collect(Collectors.toList())));

    String[] operands = constraintString.split(matchedOperator.getOperator());
    String fieldName = operands[0].trim();

    if (fieldName == null) {
      throw new IllegalArgumentException(
              String.format("Constraint segment %s could not be parsed into a valid expression!"
                                    + " Please use the form <field><operator><value> for each constraint.",
                            constraintString));
    }

    // If there is no reference value for this field, then we treat the right operand as null.
    Object correctlyTypedOperand = null;
    if (operands.length > 1) {
      checkArgument(operands.length == 2,
                    String.format("Constraint argument %s contains multiple occurrences of operator %s",
                                  constraintString, matchedOperator.getOperator()));
      correctlyTypedOperand = tryParseOperand(fieldTypes.get(fieldName), operands[1]);
    }

    constraints.put(fieldName, matchedOperator.createValueSet(fieldTypes.get(fieldName), correctlyTypedOperand));
  }

  /*
   * Currently, we only support Boolean, Integer, Floating Point, Decimal, and String operands.
   */
  private static Object tryParseOperand(ArrowType type, String operand)
  {
    switch (type.getTypeID()) {
      case Bool:
        return Boolean.valueOf(operand);
      case FloatingPoint:
      case Decimal:
        try {
          return Float.valueOf(operand);
        }
        catch (NumberFormatException floatEx) {
          return Double.valueOf(operand);
        }
      case Int:
        try {
          return Integer.valueOf(operand);
        }
        catch (NumberFormatException floatEx) {
          return Long.valueOf(operand);
        }
      default:
        // For anything else, we try passing the operand as provided.
        return operand;
    }
  }
}
