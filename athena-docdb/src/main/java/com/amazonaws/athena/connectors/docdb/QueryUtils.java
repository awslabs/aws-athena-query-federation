/*-
 * #%L
 * athena-mongodb
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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @note Portions of this file are attributable to:
 * https://github.com/prestodb/presto/blob/master/presto-mongodb/src/main/java/com/facebook/presto/mongodb/MongoSession.java
 */
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.stream.Collectors.toList;

/**
 * Collection of helper methods which build Documents for use in DocumentDB queries, including:
 * 1. Projections
 * 2. Predicates
 * 3. Queries (a collection of predicates)
 */
public final class QueryUtils
{
    private static final String OR_OP = "$or";
    private static final String AND_OP = "$and";
    private static final String NOT_OP = "$not";
    private static final String NOR_OP = "$nor";

    private static final String EQ_OP = "$eq";
    private static final String NOT_EQ_OP = "$ne";
    private static final String EXISTS_OP = "$exists";
    private static final String GTE_OP = "$gte";
    private static final String GT_OP = "$gt";
    private static final String LT_OP = "$lt";
    private static final String LTE_OP = "$lte";
    private static final String IN_OP = "$in";
    private static final String NOTIN_OP = "$nin";
    private static final String COLUMN_NAME_ID = "_id";
    private static final Logger log = LoggerFactory.getLogger(QueryUtils.class);

    private QueryUtils()
    {
    }

    /**
     * Given a Schema create a projection document which can be used to request only specific Document fields
     * from DocumentDB.
     *
     * @param schema The schema containing the requested projection.
     * @return A Document matching the requested field projections.
     */
    public static Document makeProjection(Schema schema)
    {
        Document output = new Document();
        for (Field field : schema.getFields()) {
            output.append(field.getName(), 1);
        }
        return output;
    }

    /**
     * Given a set of Constraints and the projection Schema, create the Query Document that can be used to
     * push predicates into DocumentDB.
     *
     * @param schema The schema containing the requested projection.
     * @param constraintSummary The set of constraints to apply to the query.
     * @return The Document to use as the query.
     */
    public static Document makeQuery(Schema schema, Map<String, ValueSet> constraintSummary)
    {
        Document query = new Document();
        for (Map.Entry<String, ValueSet> entry : constraintSummary.entrySet()) {
            Document doc = makePredicate(schema.findField(entry.getKey()), entry.getValue());
            if (doc != null) {
                query.putAll(doc);
            }
        }

        return query;
    }

    /**
     * Converts a single field constraint into a Document for use in a DocumentDB query.
     *
     * @param field The field for the given ValueSet constraint.
     * @param constraint The constraint to apply to the given field.
     * @return A Document describing the constraint for pushing down into DocumentDB.
     */
    public static Document makePredicate(Field field, ValueSet constraint)
    {
        String name = field.getName();

        if (constraint.isNone()) {
            return documentOf(name, isNullPredicate());
        }

        if (constraint.isAll()) {
            return documentOf(name, isNotNullPredicate());
        }

        if (constraint.isNullAllowed()) {
            //TODO: support nulls mixed with discrete value constraints
            return null;
        }

        if (constraint instanceof EquatableValueSet) {
            Block block = ((EquatableValueSet) constraint).getValues();
            List<Object> singleValues = new ArrayList<>();

            FieldReader fieldReader = block.getFieldReaders().get(0);
            for (int i = 0; i < block.getRowCount(); i++) {
                Document nextEqVal = new Document();
                fieldReader.setPosition(i);
                Object value = fieldReader.readObject();
                nextEqVal.put(EQ_OP, convert(value));
                singleValues.add(singleValues);
            }

            return orPredicate(singleValues.stream()
                    .map(next -> new Document(name, next))
                    .collect(toList()));
        }

        List<Object> singleValues = new ArrayList<>();
        List<Document> disjuncts = new ArrayList<>();
        for (Range range : constraint.getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(convert(range.getSingleValue()));
            }
            else {
                Document rangeConjuncts = new Document();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.put(GT_OP, convert(range.getLow().getValue()));
                            break;
                        case EXACTLY:
                            rangeConjuncts.put(GTE_OP, convert(range.getLow().getValue()));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.put(LTE_OP, convert(range.getHigh().getValue()));
                            break;
                        case BELOW:
                            rangeConjuncts.put(LT_OP, convert(range.getHigh().getValue()));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                verify(!rangeConjuncts.isEmpty());
                disjuncts.add(rangeConjuncts);
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            Object value = singleValues.get(0);
            if (name.equals(COLUMN_NAME_ID)) {
                ObjectId objectId = new ObjectId(value.toString());
                disjuncts.add(documentOf(EQ_OP, objectId));
            }
            else {
                disjuncts.add(documentOf(EQ_OP, value));
            }
        }
        else if (singleValues.size() > 1) {
            if (name.equals(COLUMN_NAME_ID)) {
                List<ObjectId> objectIdList = singleValues.stream()
                        .map(obj -> new ObjectId(obj.toString()))
                        .collect(Collectors.toList());
                disjuncts.add(documentOf(IN_OP, objectIdList));
            }
            else {
                disjuncts.add(documentOf(IN_OP, singleValues));
            }
        }

        return orPredicate(disjuncts.stream()
                .map(disjunct -> new Document(name, disjunct))
                .collect(toList()));
    }

    /**
     * Parses DocDB/MongoDB Json Filter/Projection to confirm its valid and convert it to Doc
     * @param filter json's based filter
     * @return Document
     */
    public static Document parseFilter(String filter)
    {
        try {
            return Document.parse(filter);
        }
        catch (JsonParseException e) {
            throw new IllegalArgumentException("Can't parse 'filter' argument as json", e);
        }
    }

    /**
     * Parses Substrait plan and extracts filter predicates per column
     */
    public static Map<String, List<ColumnPredicate>> buildFilterPredicatesFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return new HashMap<>();
        }

        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
        if (substraitRelModel.getFilterRel() == null) {
            return new HashMap<>();
        }

        List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);

        return SubstraitFunctionParser.getColumnPredicatesMap(
                extensionDeclarations,
                substraitRelModel.getFilterRel().getCondition(),
                tableColumns);
    }

    /**
     * Enhanced query builder that tries tree-based approach first, then falls back to flattened approach
     * Example: "job_title IN ('A', 'B') OR job_title < 'C'" → {"$or": [{"job_title": {"$in": ["A", "B"]}}, {"job_title": {"$lt": "C"}}]}
     */
    public static Document makeEnhancedQueryFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return new Document();
        }

        // Extract Substrait relation model from the plan to access filter conditions
        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
        if (substraitRelModel.getFilterRel() == null) {
            return new Document();
        }

        final List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        final List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);

        // Try tree-based approach first to preserve AND/OR logical structure
        // This handles cases like "A OR B OR C" correctly as OR operations
        try {
            final LogicalExpression logicalExpr = SubstraitFunctionParser.parseLogicalExpression(
                    extensionDeclarations,
                    substraitRelModel.getFilterRel().getCondition(),
                    tableColumns);

            if (logicalExpr != null) {
                // Successfully parsed expression tree - convert to MongoDB query
                return makeQueryFromLogicalExpression(logicalExpr);
            }
        }
        catch (Exception e) {
            log.warn("Tree-based parsing failed - fall back to flattened approach  {}", e.getMessage());
        }

        // Fall back to existing flattened approach for backward compatibility
        // This maintains support for edge cases where tree-based parsing might fail
        final Map<String, List<ColumnPredicate>> predicates = SubstraitFunctionParser.getColumnPredicatesMap(
                extensionDeclarations,
                substraitRelModel.getFilterRel().getCondition(),
                tableColumns);
        return makeQueryFromPlan(predicates);
    }

    /**
     * Converts Substrait column predicates to MongoDB filter Document
     */
    public static Document makeQueryFromPlan(Map<String, List<ColumnPredicate>> predicates)
    {
        if (Objects.isNull(predicates)) {
            return new Document();
        }
        Document query = new Document();
        for (Map.Entry<String, List<ColumnPredicate>> entry : predicates.entrySet()) {
            Document filter = convertColumnPredicatesToDoc(entry.getKey(), entry.getValue());
            if (filter != null) {
                query.putAll(filter);
            }
        }
        return query;
    }

    /**
     * Converts a LogicalExpression tree to MongoDB filter Document while preserving logical structure
     * Example: OR(EQUAL(job_title, 'A'), EQUAL(job_title, 'B')) → {"$or": [{"job_title": {"$eq": "A"}}, {"job_title": {"$eq": "B"}}]}
     */
    static Document makeQueryFromLogicalExpression(LogicalExpression expression)
    {
        if (expression == null) {
            return new Document();
        }

        // Handle leaf nodes (individual predicates like job_title = 'Engineer')
        if (expression.isLeaf()) {
            // Convert leaf predicate to MongoDB document using existing convertColumnPredicatesToDoc logic
            // This ensures all existing optimizations (like $in for multiple EQUAL values) are preserved
            ColumnPredicate predicate = expression.getLeafPredicate();
            return convertColumnPredicatesToDoc(predicate.getColumn(),
                    Collections.singletonList(predicate));
        }

        // Handle logical operators (AND/OR nodes with children)
        // Recursively convert each child expression to MongoDB document
        List<Document> childDocuments = new ArrayList<>();
        for (LogicalExpression child : expression.getChildren()) {
            Document childDoc = makeQueryFromLogicalExpression(child);
            if (childDoc != null && !childDoc.isEmpty()) {
                childDocuments.add(childDoc);
            }
        }

        if (childDocuments.isEmpty()) {
            return new Document();
        }
        if (childDocuments.size() == 1) {
            // Single child - no need for logical operator wrapper
            return childDocuments.get(0);
        }

        // Apply the logical operator to combine child documents
        // Example: AND → {"$and": [child1, child2]}, OR → {"$or": [child1, child2]}
        switch (expression.getOperator()) {
            case AND:
                return new Document(AND_OP, childDocuments); // {"$and": [{"col1": "val1"}, {"col2": "val2"}]}
            case OR:
                return new Document(OR_OP, childDocuments);   // {"$or": [{"col1": "val1"}, {"col2": "val2"}]}
            default:
                throw new UnsupportedOperationException(
                    "Unsupported logical operator: " + expression.getOperator());
        }
    }

    /**
     * Converts a list of ColumnPredicates into a MongoDB predicate Document
     */
    private static Document convertColumnPredicatesToDoc(String column, List<ColumnPredicate> colPreds)
    {
        if (colPreds == null || colPreds.isEmpty()) {
            return new Document();
        }
        
        List<Object> equalValues = new ArrayList<>();
        List<Document> otherPredicates = new ArrayList<>();
        for (ColumnPredicate pred : colPreds) {
            Object value = convertSubstraitValue(pred);
            SubstraitOperator op = pred.getOperator();
            switch (op) {
                case IS_NULL:
                    otherPredicates.add(isNullPredicate());
                    break;
                case IS_NOT_NULL:
                    otherPredicates.add(isNotNullPredicate());
                    break;
                case EQUAL:
                    equalValues.add(value);
                    break;
                case NOT_EQUAL:
                    otherPredicates.add(new Document(NOT_EQ_OP, value));
                    break;
                case GREATER_THAN:
                    otherPredicates.add(new Document(GT_OP, value));
                    break;
                case GREATER_THAN_OR_EQUAL_TO:
                    otherPredicates.add(new Document(GTE_OP, value));
                    break;
                case LESS_THAN:
                    otherPredicates.add(new Document(LT_OP, value));
                    break;
                case LESS_THAN_OR_EQUAL_TO:
                    otherPredicates.add(new Document(LTE_OP, value));
                    break;
                case NOT_IN:
                    if (value instanceof List) {
                        List<Object> notInValues = (List<Object>) value;
                        if (!notInValues.isEmpty()) {
                            Document notInPredicate;
                            if (column.equals(COLUMN_NAME_ID)) {
                                List<ObjectId> objectIdList = notInValues.stream()
                                        .map(v -> new ObjectId(v.toString()))
                                        .collect(Collectors.toList());
                                notInPredicate = new Document(NOTIN_OP, objectIdList);
                            }
                            else {
                                notInPredicate = new Document(NOTIN_OP, notInValues);
                            }
                            otherPredicates.add(notInPredicate);
                        }
                    }
                    break;
                case NAND:
                    // NAND operation: NOT(A AND B AND C) - exclude records where ALL conditions are true
                    // Also exclude records where any filtered field is null
                    List<Document> andConditions = new ArrayList<>();
                    Set<String> nandColumns = new HashSet<>();
                    
                    // Process each child predicate and collect column names
                    for (ColumnPredicate child : (List<ColumnPredicate>) value) {
                        Document childDoc = convertColumnPredicatesToDoc(
                                child.getColumn(),
                                Collections.singletonList(child)
                        );
                        andConditions.add(childDoc);
                        // Collect column names for null exclusion (silently skip null column names)
                        if (child.getColumn() != null) {
                            nandColumns.add(child.getColumn());
                        }
                    }
                    
                    // NAND = $nor applied to a single $and group, with null exclusion for filtered columns
                    // Example: {"$and": [{"col1": {"$ne": null}}, {"col2": {"$ne": null}}, {"$nor": [{"$and": [conditions]}]}]}
                    Document nandCondition = new Document(NOR_OP, Collections.singletonList(new Document(AND_OP, andConditions)));
                    List<Document> nandFinalConditions = new ArrayList<>();
                    
                    // Add null exclusion for each column involved in NAND operation
                    for (String col : nandColumns) {
                        nandFinalConditions.add(new Document(col, isNotNullPredicate()));
                    }
                    nandFinalConditions.add(nandCondition);
                    return new Document(AND_OP, nandFinalConditions);
                case NOR:
                    List<Document> orConditions = new ArrayList<>();
                    Set<String> norColumns = new HashSet<>();
                    for (ColumnPredicate child : (List<ColumnPredicate>) value) {
                        Document childDoc = convertColumnPredicatesToDoc(
                                child.getColumn(),
                                Collections.singletonList(child)
                        );
                        orConditions.add(childDoc);
                        if (child.getColumn() != null) {
                            norColumns.add(child.getColumn());
                        }
                    }
                    // NOR = $nor applied directly on child conditions, with null exclusion for filtered columns for
                    // filtered columns for maintaining backward compatibility with filtration through Constraints.
                    Document norCondition = new Document(NOR_OP, orConditions);
                    List<Document> norFinalConditions = new ArrayList<>();
                    for (String col : norColumns) {
                        norFinalConditions.add(new Document(col, isNotNullPredicate()));
                    }
                    norFinalConditions.add(norCondition);
                    return new Document(AND_OP, norFinalConditions);
                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + op);
            }
        }
        // Handle multiple EQUAL values -> $in
        if (equalValues.size() > 1) {
            Document inPredicate;
            if (column.equals(COLUMN_NAME_ID)) {
                List<ObjectId> objectIdList = equalValues.stream()
                        .map(v -> new ObjectId(v.toString()))
                        .collect(Collectors.toList());
                inPredicate = new Document(IN_OP, objectIdList);
            }
            else {
                inPredicate = new Document(IN_OP, equalValues);
            }
            if (!otherPredicates.isEmpty()) {
                List<Document> andConditions = new ArrayList<>();
                andConditions.add(new Document(column, inPredicate));
                for (Document otherPred : otherPredicates) {
                    andConditions.add(new Document(column, otherPred));
                }
                return new Document(AND_OP, andConditions);
            }
            return documentOf(column, inPredicate);
        }
        // Single EQUAL
        else if (equalValues.size() == 1) {
            Object eqValue = equalValues.get(0);
            Document equalPredicate;
            if (column.equals(COLUMN_NAME_ID)) {
                equalPredicate = new Document(EQ_OP, new ObjectId(eqValue.toString()));
            }
            else {
                equalPredicate = new Document(EQ_OP, eqValue);
            }
            if (!otherPredicates.isEmpty()) {
                List<Document> andConditions = new ArrayList<>();
                andConditions.add(new Document(column, equalPredicate));
                for (Document otherPred : otherPredicates) {
                    andConditions.add(new Document(column, otherPred));
                }
                return new Document(AND_OP, andConditions);
            }
            return documentOf(column, equalPredicate);
        }
        // Handle non-EQUAL predicates with special null exclusion for NOT_EQUAL operations
        // NOT_EQUAL operations should exclude records where the field is null to match SQL semantics
        else if (!otherPredicates.isEmpty()) {
            // Check if any predicate is NOT_EQUAL - these need null exclusion
            // Example: "column <> 'value'" should not match records where column is null
            boolean hasNotEqual = otherPredicates.stream()
                    .anyMatch(doc -> doc.containsKey(NOT_EQ_OP));

            if (hasNotEqual && otherPredicates.size() == 1) {
                // Single NOT_EQUAL case - wrap with null exclusion
                // Generate: {"$and": [{"column": {"$ne": null}}, {"column": {"$ne": "value"}}]}
                Document notEqualPred = otherPredicates.get(0);
                Document nullExclusion = new Document(column, isNotNullPredicate());
                return new Document(AND_OP, Arrays.asList(nullExclusion, new Document(column, notEqualPred)));
            }
            else if (otherPredicates.size() > 1) {
                // Multiple predicates in OR - handle NOT_EQUAL with null exclusion, others unchanged
                List<Document> orConditions = new ArrayList<>();
                for (Document predicate : otherPredicates) {
                    if (predicate.containsKey(NOT_EQ_OP)) {
                        // Add null exclusion only for NOT_EQUAL predicates
                        // Example: {"$and": [{"column": {"$ne": null}}, {"column": {"$ne": "value"}}]}
                        Document nullExclusion = new Document(column, isNotNullPredicate());
                        Document notEqualCondition = new Document(column, predicate);
                        orConditions.add(new Document(AND_OP, Arrays.asList(nullExclusion, notEqualCondition)));
                    }
                    else {
                        // Keep other predicates unchanged (GREATER_THAN, LESS_THAN, etc.)
                        // Example: {"column": {"$gt": 5}} remains as-is
                        orConditions.add(new Document(column, predicate));
                    }
                }
                // Combine all conditions with OR
                // Example: {"$or": [{"$and": [null_check, not_equal]}, {"column": {"$gt": 5}}]}
                return new Document(OR_OP, orConditions);
            }
            else {
                // Single non-NOT_EQUAL predicate - no null exclusion needed
                return documentOf(column, otherPredicates.get(0));
            }
        }
        return new Document();
    }

    private static Document documentOf(String key, Object value)
    {
        return new Document(key, value);
    }

    private static Document orPredicate(List<Document> values)
    {
        checkState(!values.isEmpty());
        if (values.size() == 1) {
            return values.get(0);
        }
        return new Document(OR_OP, values);
    }

    private static Document isNullPredicate()
    {
        return documentOf(EXISTS_OP, true).append(EQ_OP, null);
    }

    private static Document isNotNullPredicate()
    {
        return documentOf(NOT_EQ_OP, null);
    }

    private static Object convert(Object value)
    {
        if (value instanceof Text) {
            return ((Text) value).toString();
        }
        return value;
    }

    /**
     * Converts NumberLong values to Date objects for datetime fields
     */
    private static Object convertSubstraitValue(ColumnPredicate pred)
    {
        Object value = pred.getValue();
        // Check if this is a datetime field and value is NumberLong
        if (value instanceof Long && pred.getArrowType() instanceof ArrowType.Timestamp) {
            Long epochValue = (Long) value;
            // Convert microseconds to milliseconds (divide by 1000)
            Long milliseconds = epochValue / 1000;
            // Convert to Date object for MongoDB ISODate format
            return new Date(milliseconds);
        }
        else if (value instanceof Text) {
            return ((Text) value).toString();
        }
        else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
        }
        return value;
    }
}
