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
package com.amazonaws.athena.connectors.gcs.common;

import java.util.List;

/**
 * Used to metadata pagination during table listing
 * Tables are files in a GCS bucket
 */
public class PagedObject
{
    private List<String> fileName;
    private String nextToken;

    public PagedObject(List<String> fileName, String nextToken)
    {
        this.fileName = fileName;
        this.nextToken = nextToken;
    }

    public List<String> getFileNames()
    {
        return fileName;
    }

    public void setFileName(List<String> fileName)
    {
        this.fileName = fileName;
    }

    public String getNextToken()
    {
        return nextToken;
    }

    public void setNextToken(String nextToken)
    {
        this.nextToken = nextToken;
    }

    @Override
    public String toString()
    {
        return "PagedObject{" +
                "fileName=" + fileName +
                ", continuationToken='" + nextToken + '\'' +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private List<String> fileNames;
        private String nextToken;

        public Builder fileNames(final List<String> fileNames)
        {
            this.fileNames = fileNames;
            return this;
        }

        public Builder nextToken(final String nextToken)
        {
            this.nextToken = nextToken;
            return this;
        }

        public PagedObject build()
        {
            return new PagedObject(this.fileNames, this.nextToken);
        }
    }
}
