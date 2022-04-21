/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.Split;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@SuppressWarnings("deprecation")
@RunWith(MockitoJUnitRunner.class)
public class ImpalaQueryStringBuilderTest
{
	
	@Mock
	Split split;
	
	@Test
	public void testQueryBuilder()
	{
	    String expectedFrom1 = " FROM default.schema.table ";
	    String expectedFrom2 = " FROM default.table ";
		ImpalaQueryStringBuilder builder = new ImpalaQueryStringBuilder("");
		String fromResult1 = builder.getFromClauseWithSplit("default", "schema", "table", split);
		String fromResult2 = builder.getFromClauseWithSplit("default", "", "table", split);
		Assert.assertEquals(expectedFrom1, fromResult1);
		Assert.assertEquals(expectedFrom2, fromResult2);
	}

}
