/*-
 * #%L
 * athena-lark-base
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
package com.amazonaws.athena.connectors.lark.base.model.response;

import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import software.amazon.awssdk.utils.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum.BUTTON;
import static com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum.STAGE;
import static com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum.UNKNOWN;

/**
 * Response for List Field
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ListFieldResponse.Builder.class)
public final class ListFieldResponse extends BaseResponse<ListFieldResponse.ListData>
{
    private ListFieldResponse(Builder builder)
    {
        super(builder);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public List<FieldItem> getItems()
    {
        ListData data = getData();
        return (data != null && data.getItems() != null) ? data.getItems() : Collections.emptyList();
    }

    public String getPageToken()
    {
        ListData data = getData();
        return (data != null) ? data.getPageToken() : null;
    }

    public boolean hasMore()
    {
        ListData data = getData();
        return (data != null) && data.hasMore();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = FieldItem.Builder.class)
    public static final class FieldItem
    {
        private final String fieldId;
        private final String fieldName;
        private final Map<String, Object> property;
        private final String description;
        private final boolean isPrimary;
        private final boolean required;
        private final String uiType; // Store the raw string

        private FieldItem(Builder builder)
        {
            this.fieldId = builder.fieldId;
            this.fieldName = builder.fieldName;
            this.property = builder.property != null ? Map.copyOf(builder.property) : Collections.emptyMap();
            this.description = builder.description;
            this.isPrimary = builder.isPrimary;
            this.required = builder.required;
            this.uiType = builder.uiType;
        }

        @SuppressWarnings("unused")
        @JsonProperty("field_id")
        public String getFieldId()
        {
            return fieldId;
        }

        @JsonProperty("field_name")
        public String getFieldName()
        {
            return fieldName;
        }

        @SuppressWarnings("unused")
        @JsonProperty("description")
        public String getDescription()
        {
            return description;
        }

        @SuppressWarnings("unused")
        @JsonProperty("is_primary")
        public boolean isPrimary()
        {
            return isPrimary;
        }

        @JsonProperty("required")
        public boolean isRequired()
        {
            return required;
        }

        @SuppressWarnings("unused")
        @JsonProperty("ui_type")
        public String getUiTypeString()
        {
            return uiType;
        }

        public UITypeEnum getUIType()
        {
            return UITypeEnum.fromString(uiType);
        }

        public UITypeEnum getFormulaGlueCatalogUITypeEnum()
        {
            if (this.getUIType().equals(UITypeEnum.FORMULA)) {
                if (property == null || !property.containsKey("type")) {
                    return UITypeEnum.TEXT;
                }

                Object typeObj = property.get("type");
                if (!(typeObj instanceof Map)) {
                    return UITypeEnum.TEXT;
                }

                Map<String, Object> typeMap = (Map<String, Object>) typeObj;

                if (!typeMap.containsKey("ui_type")) {
                    return UITypeEnum.TEXT;
                }

                String dataTypeObj = typeMap.get("ui_type").toString();
                return UITypeEnum.fromString(dataTypeObj);
            }
            return UNKNOWN;
        }

        public Pair<String, String> getTargetFieldAndTableForLookup()
        {
            if (this.getUIType().equals(UITypeEnum.LOOKUP)) {
                String targetField;
                String targetTable;

                if (property == null || !property.containsKey("target_field")
                        || !property.containsKey("filter_info")) {
                    return Pair.of(null, null);
                }

                targetField = property.get("target_field").toString();

                Object filterInfoObj = property.get("filter_info");
                if (!(filterInfoObj instanceof Map)) {
                    return Pair.of(null, null);
                }

                Map<String, Object> filterInfoMap = (Map<String, Object>) filterInfoObj;

                if (!filterInfoMap.containsKey("target_table")) {
                    return Pair.of(null, null);
                }

                targetTable = filterInfoMap.get("target_table").toString();

                return Pair.of(targetField, targetTable);
            }

            return Pair.of(null, null);
        }

        public boolean blackListField()
        {
            // Due to no results from Lark API List Records/Search Records
            return this.getUIType() == BUTTON || this.getUIType() == STAGE ||
                    (this.getUIType() == UITypeEnum.FORMULA &&
                            (this.getFormulaGlueCatalogUITypeEnum() == BUTTON ||
                                    this.getFormulaGlueCatalogUITypeEnum() == STAGE));
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static final class Builder
        {
            private String fieldId;
            private String fieldName;
            private Map<String, Object> property;
            private String description;
            private boolean isPrimary;
            private boolean required;
            private String uiType;

            private Builder()
            {
            }

            @SuppressWarnings("unused")
            @JsonProperty("field_id")
            public Builder fieldId(String fieldId)
            {
                this.fieldId = fieldId;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("field_name")
            public Builder fieldName(String fieldName)
            {
                this.fieldName = fieldName;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("property")
            public Builder property(Map<String, Object> property)
            {
                this.property = property;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("description")
            public Builder description(String description)
            {
                this.description = description;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("is_primary")
            public Builder isPrimary(boolean isPrimary)
            {
                this.isPrimary = isPrimary;
                return this;
            }

            @JsonProperty("required")
            public Builder required(boolean required)
            {
                this.required = required;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("ui_type")
            public Builder uiType(String uiType)
            {
                this.uiType = uiType;
                return this;
            }

            public FieldItem build()
            {
                return new FieldItem(this);
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = ListData.Builder.class)
    public static final class ListData
    {
        private final List<FieldItem> items;
        private final String pageToken;
        private final boolean hasMore;
        private final Integer total; // Use Integer to handle potential null

        private ListData(Builder builder)
        {
            this.items = builder.items != null ? List.copyOf(builder.items) : Collections.emptyList();
            this.pageToken = builder.pageToken;
            this.hasMore = builder.hasMore;
            this.total = builder.total;
        }

        @JsonProperty("items")
        public List<FieldItem> getItems()
        {
            return items.stream().filter(item -> !item.blackListField()).toList();
        }

        @JsonProperty("page_token")
        public String getPageToken()
        {
            return pageToken;
        }

        @JsonProperty("has_more")
        public boolean hasMore()
        {
            return hasMore;
        }

        @SuppressWarnings("unused")
        @JsonProperty("total")
        public Integer getTotal()
        {
            return total;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static final class Builder
        {
            private List<FieldItem> items;
            private String pageToken;
            private boolean hasMore;
            private Integer total;

            private Builder()
            {
            }

            @SuppressWarnings("unused")
            @JsonProperty("items")
            public Builder items(List<FieldItem> items)
            {
                this.items = items;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("page_token")
            public Builder pageToken(String pageToken)
            {
                this.pageToken = pageToken;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("has_more")
            public Builder hasMore(boolean hasMore)
            {
                this.hasMore = hasMore;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("total")
            public Builder total(Integer total)
            {
                this.total = total;
                return this;
            }

            public ListData build()
            {
                return new ListData(this);
            }
        }
    }

    public static final class Builder extends BaseResponse.Builder<ListData>
    {
        private Builder()
        {
            super();
        }

        @Override
        public ListFieldResponse build()
        {
            return new ListFieldResponse(this);
        }
    }
}
