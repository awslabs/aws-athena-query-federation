/*-
 * #%L
 * glue-lark-base-crawler
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.service;

import com.amazonaws.athena.connectors.lark.base.model.LarkDatabaseRecord;
import com.amazonaws.athena.connectors.lark.base.model.response.ListAllFolderResponse;
import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class LarkDriveService extends CommonLarkService
{
    private static final Logger logger = LoggerFactory.getLogger(LarkDriveService.class);
    private static final String LARK_DRIVE_URL = LARK_API_BASE_URL + "/drive/v1";
    final int pageSize = 200;

    public LarkDriveService(String larkAppId, String larkAppSecret)
    {
        super(larkAppId, larkAppSecret);
    }

    public LarkDriveService(String larkAppId, String larkAppSecret, HttpClientWrapper httpClient)
    {
        super(larkAppId, larkAppSecret, httpClient);
    }

    public List<LarkDatabaseRecord> getLarkBases(String folderToken)
    {
        try {
            refreshTenantAccessToken();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to refresh Lark access token", e);
        }

        List<LarkDatabaseRecord> allTables = new ArrayList<>();
        String pageToken = "";
        boolean hasMore;

        do {
            try {
                URIBuilder uriBuilder = new URIBuilder(LARK_DRIVE_URL + "/" + "files")
                        .addParameter("folder_token", folderToken)
                        .addParameter("page_size", String.valueOf(pageSize));

                if (!pageToken.isEmpty()) {
                    uriBuilder.addParameter("page_token", pageToken);
                }

                URI uri = uriBuilder.build();

                logger.info("Fetching bases from Lark Drive API, url: {}", uri);

                HttpGet request = new HttpGet(uri);
                request.setHeader(HEADER_AUTHORIZATION, AUTH_BEARER_PREFIX + tenantAccessToken);
                request.setHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    String responseBody = EntityUtils.toString(response.getEntity());

                    ListAllFolderResponse tableResponse = objectMapper.readValue(responseBody, ListAllFolderResponse.class);

                    // 1254002: No more data
                    if (tableResponse.getCode() == 0 || tableResponse.getCode() == 1254002) {
                        if (tableResponse.getFiles() != null) {
                            List<ListAllFolderResponse.DriveFile> filteredFiles = tableResponse.getFiles().stream()
                                    .filter(file -> file.getType().equalsIgnoreCase("bitable")).toList();

                            for (ListAllFolderResponse.DriveFile file : filteredFiles) {
                                allTables.add(
                                        new LarkDatabaseRecord(
                                                file.getToken(),
                                                CommonUtil.sanitizeGlueRelatedName(file.getName())
                                        )
                                );
                            }
                        }

                        pageToken = tableResponse.getNextPageToken();
                        hasMore = tableResponse.hasMore();
                    }
                    else {
                        throw new IOException("Failed to retrieve tables for folder: " + folderToken + ", Error: " + tableResponse.getMsg());
                    }
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to get records for folder: " + folderToken, e);
            }
        }
        while (hasMore && pageToken != null && !pageToken.isEmpty());
        return allTables;
    }
}
