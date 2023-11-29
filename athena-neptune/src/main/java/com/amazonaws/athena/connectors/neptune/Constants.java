/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune;

public class Constants
{
   protected Constants() 
   {
       throw new UnsupportedOperationException(); 
   }
    
   public static final String SOURCE_TYPE = "neptune";
   public static final String CFG_GRAPH_TYPE = "neptune_graphtype";
   public static final String CFG_ENDPOINT = "neptune_endpoint";
   public static final String CFG_PORT = "neptune_port";
   public static final String CFG_IAM = "iam_enabled";
   public static final String CFG_REGION = "AWS_REGION";
    
   public static final String SCHEMA_QUERY = "query";
   public static final String SCHEMA_CASE_INSEN = "enable_caseinsensitivematch";
   public static final String SCHEMA_COMPONENT_TYPE = "componenttype";
   public static final String SCHEMA_GLABEL = "glabel";
   public static final String SCHEMA_STRIP_URI = "strip_uri";
   public static final String SCHEMA_PREFIX_PROP = "prefix_prop";
   public static final String SCHEMA_PREFIX_CLASS = "prefix_class";
   public static final String SCHEMA_QUERY_MODE = "querymode";
   public static final String SCHEMA_CLASS_URI = "classuri";
   public static final String SCHEMA_SUBJECT = "subject";
   public static final String SCHEMA_PREDS_PREFIX = "preds_prefix";

   public static final String QUERY_MODE_CLASS = "class";
   public static final String QUERY_MODE_SPARQL = "sparql";
    
   public static final String PREFIX_KEY = "prefix_";
   public static final int PREFIX_LEN = PREFIX_KEY.length();
}
