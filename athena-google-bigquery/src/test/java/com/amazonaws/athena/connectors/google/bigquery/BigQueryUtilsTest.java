/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandler.class);
    @Mock
    BigQuery bigQuery;
    BigQueryPage<Dataset> datasets;
    BigQueryPage<Table> tables;
    String datasetName;

    @Before
    public void init()
    {
        System.setProperty("aws.region", "us-east-1");

        //Mock the BigQuery Client to return Datasets, and Table Schema information.
        datasets = new BigQueryPage<Dataset>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, 2));
        when(bigQuery.listDatasets(nullable(String.class))).thenReturn(datasets);
        //Get the first dataset name.
        datasetName = datasets.iterateAll().iterator().next().getDatasetId().getDataset();
        tables = new BigQueryPage<Table>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME, "dataset1", 2));
        when(bigQuery.listTables(nullable(DatasetId.class))).thenReturn(tables);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bigQueryUtils()
    {
        String newDatasetName = BigQueryUtils.fixCaseForDatasetName(BigQueryTestUtils.PROJECT_1_NAME, "testDataset", bigQuery);
        assertNull(newDatasetName);

        String tableName = BigQueryUtils.fixCaseForTableName(BigQueryTestUtils.PROJECT_1_NAME, datasetName, "test", bigQuery);
        assertNull(tableName);
    }
}
