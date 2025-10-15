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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.model.enums;

/**
 * Enum for UI Type
 */
public enum UITypeEnum
{
    TEXT("Text"),
    BARCODE("Barcode"),
    SINGLE_SELECT("SingleSelect"),
    PHONE("Phone"),
    NUMBER("Number"),
    AUTO_NUMBER("AutoNumber"),
    PROGRESS("Progress"),
    CURRENCY("Currency"),
    RATING("Rating"),
    MULTI_SELECT("MultiSelect"),
    USER("User"),
    GROUP_CHAT("GroupChat"),
    ATTACHMENT("Attachment"),
    FORMULA("Formula"),
    SINGLE_LINK("SingleLink"),
    DUPLEX_LINK("DuplexLink"),
    DATE_TIME("DateTime"),
    CREATED_TIME("CreatedTime"),
    MODIFIED_TIME("ModifiedTime"),
    CHECKBOX("Checkbox"),
    URL("Url"),
    LOCATION("Location"),
    CREATED_USER("CreatedUser"),
    MODIFIED_USER("ModifiedUser"),
    EMAIL("Email"),
    LOOKUP("Lookup"), //Type: 19

    // Unavailable API at the moment
    BUTTON("Button"), //Type: 3001
    STAGE("Stage"), //Type: 24

    UNKNOWN("unknown");

    private final String uiType;

    UITypeEnum(String uiType)
    {
        this.uiType = uiType;
    }

    public String getUiType()
    {
        return uiType;
    }

    public static UITypeEnum fromString(String text)
    {
        for (UITypeEnum uiType : UITypeEnum.values()) {
            if (uiType.getUiType().equalsIgnoreCase(text)) {
                return uiType;
            }
        }
        return UNKNOWN;
    }
}
