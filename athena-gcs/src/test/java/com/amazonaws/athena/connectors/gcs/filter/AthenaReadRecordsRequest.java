/*-
 * #%L
 * athena-storage-api
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs.filter;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import org.apache.arrow.vector.types.pojo.Schema;

public class AthenaReadRecordsRequest extends ReadRecordsRequest
{
    /**
     * Constructs a new ReadRecordsRequest object.
     *
     * @param identity           The identity of the caller.
     * @param catalogName        The catalog name that records should be read for.
     * @param queryId            The ID of the query requesting data.
     * @param tableName          The name of the table being read from.
     * @param schema             The schema of the table being read from.
     * @param split              The split being read.
     * @param constraints        The constraints to apply to read records.
     * @param maxBlockSize       The maximum supported block size.
     * @param maxInlineBlockSize The maximum block size before spilling.
     */
    public AthenaReadRecordsRequest(FederatedIdentity identity,
                                    String catalogName,
                                    String queryId,
                                    TableName tableName,
                                    Schema schema,
                                    Split split,
                                    Constraints constraints,
                                    long maxBlockSize,
                                    long maxInlineBlockSize)
    {
        super(identity, catalogName, queryId, tableName, schema, split, constraints, maxBlockSize, maxInlineBlockSize);
    }

    @Override
    public Constraints getConstraints()
    {
        return super.getConstraints();
    }

    @Override
    public TableName getTableName()
    {
        return super.getTableName();
    }

    @Override
    public Schema getSchema()
    {
        return super.getSchema();
    }

    @Override
    public Split getSplit()
    {
        return super.getSplit();
    }

    @Override
    public String getCatalogName()
    {
        return super.getCatalogName();
    }
}
