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
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * Converts a list of ColumnPredicates into a MongoDB predicate Document
     */
    private static Document convertColumnPredicatesToDoc(String column, List<ColumnPredicate> colPreds)
    {
        // Return empty document if no predicates provided
        if (colPreds == null || colPreds.isEmpty()) {
            return new Document();
        }
        
        // Handle NULL checks first - these take precedence and return immediately
        for (ColumnPredicate pred : colPreds) {
            if (pred.getOperator() == SubstraitOperator.IS_NULL) {
                return documentOf(column, isNullPredicate()); // { column: { $eq: null } }
            }
            if (pred.getOperator() == SubstraitOperator.IS_NOT_NULL) {
                return documentOf(column, isNotNullPredicate()); // { column: { $ne: null } }
            }
        }
        
        // Separate EQUAL values from other predicates for optimization
        List<Object> equalValues = new ArrayList<>(); // Collect all EQUAL values for potential $in operation
        List<Document> otherPredicates = new ArrayList<>(); // Collect non-EQUAL predicates
        
        for (ColumnPredicate pred : colPreds) {
            Object value = pred.getValue();
            SubstraitOperator op = pred.getOperator();
            switch (op) {
                case EQUAL:
                    equalValues.add(value); // Store for potential $in optimization
                    break;
                case NOT_EQUAL:
                    otherPredicates.add(new Document(NOT_EQ_OP, value)); // { $ne: value }
                    break;
                case GREATER_THAN:
                    otherPredicates.add(new Document(GT_OP, value)); // { $gt: value }
                    break;
                case GREATER_THAN_OR_EQUAL_TO:
                    otherPredicates.add(new Document(GTE_OP, value)); // { $gte: value }
                    break;
                case LESS_THAN:
                    otherPredicates.add(new Document(LT_OP, value)); // { $lt: value }
                    break;
                case LESS_THAN_OR_EQUAL_TO:
                    otherPredicates.add(new Document(LTE_OP, value)); // { $lte: value }
                    break;
                case NAND:
                    // NAND(A,B) = NOT(A AND B) = $nor: [{ $and: [A, B] }]
                    List<Document> andConditions = new ArrayList<>();
                    for (ColumnPredicate child : (List<ColumnPredicate>) value) {
                        // Recursively convert each child predicate
                        Document childDoc = convertColumnPredicatesToDoc(
                                child.getColumn(),
                                Collections.singletonList(child)
                        );
                        andConditions.add(childDoc);
                    }
                    // Wrap AND conditions in NOR to create NAND
                    return new Document(NOR_OP, Collections.singletonList(new Document(AND_OP, andConditions)));
                case NOR:
                    // NOR(A,B) = NOT(A OR B) = $nor: [A, B]
                    List<Document> orConditions = new ArrayList<>();
                    for (ColumnPredicate child : (List<ColumnPredicate>) value) {
                        // Recursively convert each child predicate
                        Document childDoc = convertColumnPredicatesToDoc(
                                child.getColumn(),
                                Collections.singletonList(child)
                        );
                        orConditions.add(childDoc);
                    }
                    // Apply NOR directly on child conditions
                    return new Document(NOR_OP, orConditions);
                case NOT:
                    // NOT(A) = $nor: [A] - negate single predicate using NOR
                    if (value instanceof ColumnPredicate) {
                        ColumnPredicate childPred = (ColumnPredicate) value;
                        // Recursively convert the child predicate
                        Document childDoc = convertColumnPredicatesToDoc(
                                childPred.getColumn(),
                                Collections.singletonList(childPred)
                        );
                        // Wrap in NOR to negate
                        return new Document(NOR_OP, Collections.singletonList(childDoc));
                    }
                    throw new IllegalArgumentException("NOT operator requires a ColumnPredicate as value");
                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + op);
            }
        }
        
        // Optimize multiple EQUAL values into $in operation
        if (equalValues.size() > 1) {
            Document inPredicate;
            // Special handling for _id field - convert to ObjectId
            if (column.equals(COLUMN_NAME_ID)) {
                List<ObjectId> objectIdList = equalValues.stream()
                        .map(v -> new ObjectId(v.toString()))
                        .collect(Collectors.toList());
                inPredicate = new Document(IN_OP, objectIdList); // { $in: [ObjectId(...), ...] }
            }
            else {
                inPredicate = new Document(IN_OP, equalValues); // { $in: [val1, val2, ...] }
            }
            // Combine $in with other predicates using $and if needed
            if (!otherPredicates.isEmpty()) {
                List<Document> andConditions = new ArrayList<>();
                andConditions.add(new Document(column, inPredicate)); // Add $in condition
                for (Document otherPred : otherPredicates) {
                    andConditions.add(new Document(column, otherPred)); // Add other conditions
                }
                return new Document(AND_OP, andConditions); // { $and: [{ column: { $in: [...] }}, ...] }
            }
            return documentOf(column, inPredicate); // { column: { $in: [...] } }
        }
        // Handle single EQUAL value
        else if (equalValues.size() == 1) {
            Object eqValue = equalValues.get(0);
            Document equalPredicate;
            // Special handling for _id field - convert to ObjectId
            if (column.equals(COLUMN_NAME_ID)) {
                equalPredicate = new Document(EQ_OP, new ObjectId(eqValue.toString())); // { $eq: ObjectId(...) }
            }
            else {
                equalPredicate = new Document(EQ_OP, eqValue); // { $eq: value }
            }
            // Combine single EQUAL with other predicates using $and if needed
            if (!otherPredicates.isEmpty()) {
                List<Document> andConditions = new ArrayList<>();
                andConditions.add(new Document(column, equalPredicate)); // Add equality condition
                for (Document otherPred : otherPredicates) {
                    andConditions.add(new Document(column, otherPred)); // Add other conditions
                }
                return new Document(AND_OP, andConditions); // { $and: [{ column: { $eq: val }}, ...] }
            }
            return documentOf(column, equalPredicate); // { column: { $eq: value } }
        }
        // Handle only non-EQUAL predicates (no EQUAL values present)
        else if (!otherPredicates.isEmpty()) {
            // Multiple non-EQUAL predicates use $or (e.g., col > 5 OR col < 10)
            if (otherPredicates.size() > 1) {
                List<Document> orConditions = new ArrayList<>();
                for (Document predicate : otherPredicates) {
                    orConditions.add(new Document(column, predicate)); // Wrap each predicate with column
                }
                return new Document(OR_OP, orConditions); // { $or: [{ column: pred1 }, { column: pred2 }] }
            }
            else {
                // Single non-EQUAL predicate
                return documentOf(column, otherPredicates.get(0)); // { column: predicate }
            }
        }
        // No predicates to process - return empty document
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
}
