/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds Delta Share jsonPredicateHints from Athena constraints and substrait plans for server-side filtering.
 */
public class DeltaSharePredicateUtils
{
    private static final Logger logger = LoggerFactory.getLogger(DeltaSharePredicateUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Extracts filter predicates from substrait plan.
     */
    public static Map<String, List<ColumnPredicate>> buildFilterPredicatesFromPlan(Plan plan)
    {
        try {
            SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
            
            if (substraitRelModel.getFilterRel() == null) {
                logger.info("No filter relation found in substrait plan");
                return new HashMap<>();
            }
            
            List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
            List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);
            
            return SubstraitFunctionParser.getColumnPredicatesMap(
                extensionDeclarations, 
                substraitRelModel.getFilterRel().getCondition(), 
                tableColumns);
                
        } catch (Exception e) {
            logger.warn("Failed to extract predicates from substrait plan: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * Builds jsonPredicateHints from column predicates for partition columns.
     */
    public static String buildJsonPredicateHints(Map<String, List<ColumnPredicate>> filterPredicates, 
                                                 List<String> partitionColumns)
    {
        if (filterPredicates.isEmpty() || partitionColumns.isEmpty()) {
            return null;
        }

        try {
            List<ObjectNode> columnConditions = new ArrayList<>();

            for (String partitionCol : partitionColumns) {
                List<ColumnPredicate> predicates = filterPredicates.get(partitionCol);
                if (predicates != null && !predicates.isEmpty()) {
                    
                    if (predicates.size() == 1) {
                        ObjectNode condition = buildPredicateCondition(partitionCol, predicates.get(0));
                        if (condition != null) {
                            columnConditions.add(condition);
                        }
                    } else {
                        List<ObjectNode> sameColumnConditions = new ArrayList<>();
                        for (ColumnPredicate predicate : predicates) {
                            ObjectNode condition = buildPredicateCondition(partitionCol, predicate);
                            if (condition != null) {
                                sameColumnConditions.add(condition);
                            }
                        }
                        
                        if (sameColumnConditions.size() == 1) {
                            columnConditions.add(sameColumnConditions.get(0));
                        } else if (sameColumnConditions.size() > 1) {
                            ObjectNode orNode = objectMapper.createObjectNode();
                            orNode.put("op", "or");
                            ArrayNode orChildren = objectMapper.createArrayNode();
                            sameColumnConditions.forEach(orChildren::add);
                            orNode.set("children", orChildren);
                            columnConditions.add(orNode);
                        }
                    }
                }
            }

            if (columnConditions.isEmpty()) {
                return null;
            }

            if (columnConditions.size() == 1) {
                return objectMapper.writeValueAsString(columnConditions.get(0));
            } else {
                ObjectNode andNode = objectMapper.createObjectNode();
                andNode.put("op", "and");
                ArrayNode childrenArray = objectMapper.createArrayNode();
                columnConditions.forEach(childrenArray::add);
                andNode.set("children", childrenArray);
                
                return objectMapper.writeValueAsString(andNode);
            }

        } catch (Exception e) {
            logger.error("Failed to build jsonPredicateHints from column predicates: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Builds jsonPredicateHints from Athena ValueSets for partition columns.
     */
    public static String buildJsonPredicateHintsFromValueSets(Map<String, ValueSet> summary, 
                                                              List<String> partitionColumns)
    {
        if (summary.isEmpty() || partitionColumns.isEmpty()) {
            return null;
        }

        try {
            List<ObjectNode> conditions = new ArrayList<>();

            for (String partitionCol : partitionColumns) {
                ValueSet valueSet = summary.get(partitionCol);
                if (valueSet != null) {
                    ObjectNode condition = buildValueSetCondition(partitionCol, valueSet);
                    if (condition != null) {
                        conditions.add(condition);
                    }
                }
            }

            if (conditions.isEmpty()) {
                return null;
            }

            if (conditions.size() == 1) {
                return objectMapper.writeValueAsString(conditions.get(0));
            } else {
                ObjectNode andNode = objectMapper.createObjectNode();
                andNode.put("op", "and");
                ArrayNode childrenArray = objectMapper.createArrayNode();
                conditions.forEach(childrenArray::add);
                andNode.set("children", childrenArray);
                
                return objectMapper.writeValueAsString(andNode);
            }

        } catch (Exception e) {
            logger.error("Failed to build jsonPredicateHints from value sets: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Builds a predicate condition from a ColumnPredicate.
     */
    private static ObjectNode buildPredicateCondition(String columnName, ColumnPredicate predicate)
    {
        try {
            ObjectNode condition = objectMapper.createObjectNode();
            
            String operation = mapPredicateOperation(predicate);
            if (operation == null) {
                return null;
            }

            condition.put("op", operation);
            
            ArrayNode children = objectMapper.createArrayNode();
            
            ObjectNode columnNode = objectMapper.createObjectNode();
            columnNode.put("op", "column");
            columnNode.put("name", columnName);
            columnNode.put("valueType", mapValueType(predicate.getArrowType()));
            children.add(columnNode);
            
            ObjectNode literalNode = objectMapper.createObjectNode();
            literalNode.put("op", "literal");
            literalNode.set("value", objectMapper.valueToTree(predicate.getValue()));
            literalNode.put("valueType", mapValueType(predicate.getArrowType()));
            children.add(literalNode);
            
            condition.set("children", children);
            return condition;

        } catch (Exception e) {
            logger.warn("Failed to build predicate condition for column {}: {}", columnName, e.getMessage());
            return null;
        }
    }

    /**
     * Builds a condition from an Athena ValueSet.
     */
    private static ObjectNode buildValueSetCondition(String columnName, ValueSet valueSet)
    {
        try {
            if (valueSet.isSingleValue()) {
                ObjectNode condition = objectMapper.createObjectNode();
                condition.put("op", "equal");
                
                ArrayNode children = objectMapper.createArrayNode();
                
                ObjectNode columnNode = objectMapper.createObjectNode();
                columnNode.put("op", "column");
                columnNode.put("name", columnName);
                columnNode.put("valueType", mapValueType(valueSet.getType()));
                children.add(columnNode);
                
                ObjectNode literalNode = objectMapper.createObjectNode();
                literalNode.put("op", "literal");
                literalNode.set("value", objectMapper.valueToTree(valueSet.getSingleValue()));
                literalNode.put("valueType", mapValueType(valueSet.getType()));
                children.add(literalNode);
                
                condition.set("children", children);
                return condition;
            }

            if (!valueSet.isNone() && !valueSet.isAll()) {
                logger.info("Skipping complex ValueSet for column {}", columnName);
                return null;
            }

            return null;

        } catch (Exception e) {
            logger.warn("Failed to build condition from ValueSet for column {}: {}", columnName, e.getMessage());
            return null;
        }
    }

    /**
     * Maps ColumnPredicate operations to Delta Share operations.
     */
    private static String mapPredicateOperation(ColumnPredicate predicate)
    {
        String operatorName = predicate.getOperator().toString();
        
        switch (operatorName) {
            case "EQUAL":
                return "equal";
            case "NOT_EQUAL":
                return "not_equal";
            case "LESS_THAN":
                return "less_than";
            case "LESS_THAN_OR_EQUAL":
                return "less_than_or_equal";
            case "GREATER_THAN":
                return "greater_than";
            case "GREATER_THAN_OR_EQUAL":
                return "greater_than_or_equal";
            case "IS_NULL":
                return "is_null";
            case "IS_NOT_NULL":
                return "is_not_null";
            default:
                logger.warn("Unsupported operator for Delta Share: {}", predicate.getOperator());
                return null;
        }
    }

    /**
     * Maps Arrow types to Delta Share value types.
     */
    private static String mapValueType(org.apache.arrow.vector.types.pojo.ArrowType arrowType)
    {
        if (arrowType == null) {
            return "string";
        }

        String typeString = arrowType.toString().toLowerCase();
        
        if (typeString.contains("int")) {
            return "int";
        } else if (typeString.contains("float") || typeString.contains("double")) {
            return "double";
        } else if (typeString.contains("bool")) {
            return "boolean";
        } else if (typeString.contains("date")) {
            return "date";
        } else if (typeString.contains("timestamp")) {
            return "timestamp";
        } else {
            return "string";
        }
    }

}
