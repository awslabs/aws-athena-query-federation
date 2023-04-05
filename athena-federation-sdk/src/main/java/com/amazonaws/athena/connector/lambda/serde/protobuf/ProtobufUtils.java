/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde.protobuf;

import com.amazonaws.athena.connector.lambda.proto.domain.TableName;

/**
 * This class will hold the utility functions that the prior POJOs held. For example, the old TableName.java had a method to format the 
 * fully qualified table name, and I moved it here in order to be able to delete the TableName class altogether to make everything smoother.
 * 
 * The alternative, of maintaining parallel classes, requires needless translations to/from the proto message and allows for deviations to occur,
 * so this is better.
 */
public class ProtobufUtils
{
    private ProtobufUtils()
    {
        // do nothing
    }

    public static String getQualifiedTableName(TableName tableName)
    {
        return String.format("%s.%s", tableName.getSchemaName(), tableName.getTableName());
    }

    public static String buildS3SpillLocationKey(String prefix, String queryId, String splitId)
    {
        return prefix + "/" + queryId + "/" + splitId;
    }
}
