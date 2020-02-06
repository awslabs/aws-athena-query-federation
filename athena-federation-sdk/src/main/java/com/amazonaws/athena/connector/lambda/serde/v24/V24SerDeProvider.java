/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.fasterxml.jackson.core.JsonFactory;

public class V24SerDeProvider
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    // static serdes that don't need an instance of BlockAllocator
    private static final ArrowTypeSerDe ARROW_TYPE_SER_DE = new ArrowTypeSerDe();
    private static final SchemaSerDe SCHEMA_SER_DE = new SchemaSerDe();
    private static final FederatedIdentitySerDe FEDERATED_IDENTITY_SER_DE = new FederatedIdentitySerDe();
    private static final TableNameSerDe TABLE_NAME_SER_DE = new TableNameSerDe();
    private static final SpillLocationSerDe SPILL_LOCATION_SER_DE = new SpillLocationSerDe(new S3SpillLocationSerDe());
    private static final EncryptionKeySerDe ENCRYPTION_KEY_SER_DE = new EncryptionKeySerDe();
    private static final SplitSerDe SPLIT_SER_DE = new SplitSerDe(SPILL_LOCATION_SER_DE, ENCRYPTION_KEY_SER_DE);
    private static final AllOrNoneValueSetSerDe ALL_OR_NONE_VALUE_SET_SER_DE = new AllOrNoneValueSetSerDe(ARROW_TYPE_SER_DE);

    private static final PingRequestSerDe PING_REQUEST_SER_DE = new PingRequestSerDe(FEDERATED_IDENTITY_SER_DE);
    private static final ListSchemasRequestSerDe LIST_SCHEMAS_REQUEST_SER_DE = new ListSchemasRequestSerDe(FEDERATED_IDENTITY_SER_DE);
    private static final ListTablesRequestSerDe LIST_TABLES_REQUEST_SER_DE = new ListTablesRequestSerDe(FEDERATED_IDENTITY_SER_DE);
    private static final GetTableRequestSerDe GET_TABLE_REQUEST_SER_DE = new GetTableRequestSerDe(FEDERATED_IDENTITY_SER_DE, TABLE_NAME_SER_DE);

    private static final PingResponseSerDe PING_RESPONSE_SER_DE = new PingResponseSerDe();
    private static final ListSchemasResponseSerDe LIST_SCHEMAS_RESPONSE_SER_DE = new ListSchemasResponseSerDe();
    private static final ListTablesResponseSerDe LIST_TABLES_RESPONSE_SER_DE = new ListTablesResponseSerDe(TABLE_NAME_SER_DE);
    private static final GetTableResponseSerDe GET_TABLE_RESPONSE_SER_DE = new GetTableResponseSerDe(TABLE_NAME_SER_DE, SCHEMA_SER_DE);
    private static final GetSplitsResponseSerDe GET_SPLITS_RESPONSE_SER_DE = new GetSplitsResponseSerDe(SPLIT_SER_DE);
    private static final RemoteReadRecordsResponseSerDe REMOTE_READ_RECORDS_RESPONSE_SER_DE = new RemoteReadRecordsResponseSerDe(SCHEMA_SER_DE, SPILL_LOCATION_SER_DE,
            ENCRYPTION_KEY_SER_DE);

    public FederationRequestSerDe getFederationRequestSerDe(BlockAllocator allocator)
    {
        BlockSerDe blockSerDe = new BlockSerDe(allocator, JSON_FACTORY, SCHEMA_SER_DE);
        ValueSetSerDe valueSetSerDe = createValueSetSerDe(blockSerDe);
        ConstraintsSerDe constraintsSerDe = new ConstraintsSerDe(valueSetSerDe);
        return new FederationRequestSerDe(
                PING_REQUEST_SER_DE,
                LIST_SCHEMAS_REQUEST_SER_DE,
                LIST_TABLES_REQUEST_SER_DE,
                GET_TABLE_REQUEST_SER_DE,
                new GetTableLayoutRequestSerDe(FEDERATED_IDENTITY_SER_DE, TABLE_NAME_SER_DE, constraintsSerDe, SCHEMA_SER_DE),
                new GetSplitsRequestSerDe(FEDERATED_IDENTITY_SER_DE, TABLE_NAME_SER_DE, blockSerDe, constraintsSerDe),
                new ReadRecordsRequestSerDe(FEDERATED_IDENTITY_SER_DE, TABLE_NAME_SER_DE, constraintsSerDe, SCHEMA_SER_DE, SPLIT_SER_DE),
                new UserDefinedFunctionRequestSerDe(FEDERATED_IDENTITY_SER_DE, blockSerDe, SCHEMA_SER_DE));
    }

    public FederationResponseSerDe getFederationResponseSerDe(BlockAllocator allocator)
    {
        BlockSerDe blockSerDe = new BlockSerDe(allocator, JSON_FACTORY, SCHEMA_SER_DE);
        return new FederationResponseSerDe(
                PING_RESPONSE_SER_DE,
                LIST_SCHEMAS_RESPONSE_SER_DE,
                LIST_TABLES_RESPONSE_SER_DE,
                GET_TABLE_RESPONSE_SER_DE,
                new GetTableLayoutResponseSerDe(TABLE_NAME_SER_DE, blockSerDe),
                GET_SPLITS_RESPONSE_SER_DE,
                new ReadRecordsResponseSerDe(blockSerDe),
                REMOTE_READ_RECORDS_RESPONSE_SER_DE,
                new UserDefinedFunctionResponseSerDe(blockSerDe));
    }

    public PingResponseSerDe getPingResponseSerDe()
    {
        return PING_RESPONSE_SER_DE;
    }

    public ListSchemasResponseSerDe getListSchemasResponseSerDe()
    {
        return LIST_SCHEMAS_RESPONSE_SER_DE;
    }

    public ListTablesResponseSerDe getListTablesResponseSerDe()
    {
        return LIST_TABLES_RESPONSE_SER_DE;
    }

    public GetTableResponseSerDe getGetTableResponseSerDe()
    {
        return GET_TABLE_RESPONSE_SER_DE;
    }

    public GetTableLayoutResponseSerDe getGetTableLayoutResponseSerDe(BlockAllocator allocator)
    {
        BlockSerDe blockSerDe = new BlockSerDe(allocator, JSON_FACTORY, SCHEMA_SER_DE);
        return new GetTableLayoutResponseSerDe(TABLE_NAME_SER_DE, blockSerDe);
    }

    public GetSplitsResponseSerDe getGetSplitsResponseSerDe()
    {
        return GET_SPLITS_RESPONSE_SER_DE;
    }

    private ValueSetSerDe createValueSetSerDe(BlockSerDe blockSerDe)
    {
        MarkerSerDe markerSerDe = new MarkerSerDe(blockSerDe);
        RangeSerDe rangeSerDe = new RangeSerDe(markerSerDe);
        EquatableValueSetSerDe equatableValueSetSerDe = new EquatableValueSetSerDe(blockSerDe);
        SortedRangeSetSerDe sortedRangeSetSerDe = new SortedRangeSetSerDe(ARROW_TYPE_SER_DE, rangeSerDe);
        return new ValueSetSerDe(equatableValueSetSerDe, sortedRangeSetSerDe, ALL_OR_NONE_VALUE_SET_SER_DE);
    }
}
