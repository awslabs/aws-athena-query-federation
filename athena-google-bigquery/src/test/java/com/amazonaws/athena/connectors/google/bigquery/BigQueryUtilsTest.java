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
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*","org.w3c.*","javax.net.ssl.*","sun.security.*","jdk.internal.reflect.*","javax.crypto.*"})
public class BigQueryUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandler.class);
    private BigQueryCompositeHandler bigQueryCompositeHandler;

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
        when(bigQuery.listDatasets(any(String.class))).thenReturn(datasets);
        //Get the first dataset name.
        datasetName = datasets.iterateAll().iterator().next().getDatasetId().getDataset();
        tables = new BigQueryPage<Table>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME, "dataset1", 2));
        when(bigQuery.listTables(any(DatasetId.class))).thenReturn(tables);
    }

    @Test(expected = IllegalArgumentException.class)
    public void EnvVarException() throws IOException {
        bigQueryCompositeHandler = new BigQueryCompositeHandler();
    }

    @Test(expected = IllegalArgumentException.class)
    public void bigQueryUtils() {
        String newDatasetName = BigQueryUtils.fixCaseForDatasetName(BigQueryTestUtils.PROJECT_1_NAME, "testDataset", bigQuery);
        assertEquals(null, newDatasetName);

        String tableName = BigQueryUtils.fixCaseForTableName(BigQueryTestUtils.PROJECT_1_NAME, datasetName, "test", bigQuery);
        assertEquals(null, tableName);
    }
}
