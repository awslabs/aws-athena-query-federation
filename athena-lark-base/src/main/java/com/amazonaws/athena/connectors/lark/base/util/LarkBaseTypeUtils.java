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
package com.amazonaws.athena.connectors.lark.base.util;

import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class LarkBaseTypeUtils
{
    private LarkBaseTypeUtils()
    {
        // Prevent instantiation
    }

    /**
     * Maps a Lark field definition (using UITypeEnum) to an Arrow MinorType.
     * This reflects the logic used in UITypeEnum.getGlueCatalogType.
     *
     * @param larkField The FieldItem from Lark API response.
     * @return The corresponding Arrow MinorType.
     */
    public static Types.MinorType larkFieldToArrowMinorType(AthenaFieldLarkBaseMapping larkField)
    {
        UITypeEnum uiType = larkField.nestedUIType().uiType();

        return switch (uiType) {
            // Glue: string -> Arrow: VARCHAR
            case TEXT, BARCODE, SINGLE_SELECT, PHONE, AUTO_NUMBER, EMAIL -> Types.MinorType.VARCHAR;

            // Glue: decimal -> Arrow: DECIMAL
            case NUMBER, PROGRESS, CURRENCY -> Types.MinorType.DECIMAL;

            // Glue: tinyint -> Arrow: TINYINT
            case RATING -> Types.MinorType.TINYINT;

            // Glue: timestamp -> SDK -> Arrow: DATEMILLI
            case DATE_TIME, CREATED_TIME, MODIFIED_TIME -> Types.MinorType.DATEMILLI;

            // Glue: boolean -> Arrow: BIT
            case CHECKBOX -> Types.MinorType.BIT;

            // Glue: array<...> -> Arrow: LIST
            case MULTI_SELECT, USER, GROUP_CHAT, ATTACHMENT, SINGLE_LINK, DUPLEX_LINK, LOOKUP -> Types.MinorType.LIST;

            // Glue: struct<...> -> Arrow: STRUCT
            case URL, LOCATION, CREATED_USER, MODIFIED_USER -> Types.MinorType.STRUCT;

            // Glue: depends on formulaType -> Arrow: depends on resolved type
            case FORMULA -> {
                NestedUIType newNestedUIType = new NestedUIType(larkField.nestedUIType().childType(), UITypeEnum.UNKNOWN);
                AthenaFieldLarkBaseMapping newLarkBaseMapping = new AthenaFieldLarkBaseMapping(larkField.athenaName(), larkField.larkBaseFieldName(), newNestedUIType);

                yield larkFieldToArrowMinorType(newLarkBaseMapping);
            }

            default -> Types.MinorType.VARCHAR;
        };
    }

    /**
     * Provides the Arrow Field definition for the child element of a Lark LIST field,
     * based on the mapping defined in UITypeEnum.getGlueCatalogType.
     *
     * @param larkField The Lark FieldItem representing the LIST.
     * @return The Arrow Field definition for the list's child element.
     */
    public static Field getLarkListChildField(AthenaFieldLarkBaseMapping larkField)
    {
        UITypeEnum uiType = larkField.nestedUIType().uiType();

        return switch (uiType) {
            // Glue: array<string> -> Arrow Child: VARCHAR
            case MULTI_SELECT -> Field.nullable("item", ArrowType.Utf8.INSTANCE);

            // Glue: array<struct<email:string,en_name:string,id:string,name:string>>
            // Arrow Child: Struct<email:VARCHAR, en_name:VARCHAR, id:VARCHAR, name:VARCHAR>
            case USER -> new Field("user_info", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                    Field.nullable("email", ArrowType.Utf8.INSTANCE),
                    Field.nullable("en_name", ArrowType.Utf8.INSTANCE),
                    Field.nullable("id", ArrowType.Utf8.INSTANCE),
                    Field.nullable("name", ArrowType.Utf8.INSTANCE)
            ));

            // Glue: array<struct<avatar_url:string,id:string,name:string>>
            // Arrow Child: Struct<avatar_url:VARCHAR, id:VARCHAR, name:VARCHAR>
            case GROUP_CHAT -> new Field("group_chat_info", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                    Field.nullable("avatar_url", ArrowType.Utf8.INSTANCE),
                    Field.nullable("id", ArrowType.Utf8.INSTANCE),
                    Field.nullable("name", ArrowType.Utf8.INSTANCE)
            ));

            // Glue: array<struct<file_token:string,name:string,size:int,tmp_url:string,type:string,url:string>>
            // Arrow Child: Struct<file_token:VARCHAR, name:VARCHAR, size:INT, tmp_url:VARCHAR, type:VARCHAR, url:VARCHAR>
            case ATTACHMENT -> new Field("attachment_info", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                    Field.nullable("file_token", ArrowType.Utf8.INSTANCE),
                    Field.nullable("name", ArrowType.Utf8.INSTANCE),
                    Field.nullable("size", new ArrowType.Int(32, true)),
                    Field.nullable("tmp_url", ArrowType.Utf8.INSTANCE),
                    Field.nullable("type", ArrowType.Utf8.INSTANCE),
                    Field.nullable("url", ArrowType.Utf8.INSTANCE)
            ));

            // Glue: array<struct<record_ids:array<string>,table_id:string,text:string,text_arr:array<string>,type:string>>
            // Arrow Child: Struct<record_ids:LIST, table_id:VARCHAR, text:VARCHAR, text_arr:LIST, type:VARCHAR>
            case SINGLE_LINK, DUPLEX_LINK ->
                    new Field("linked_record", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                            new Field("record_ids",
                                    new FieldType(true, ArrowType.List.INSTANCE, null),
                                    Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE))),

                            Field.nullable("table_id", ArrowType.Utf8.INSTANCE),
                            Field.nullable("text", ArrowType.Utf8.INSTANCE),

                            new Field("text_arr",
                                    new FieldType(true, ArrowType.List.INSTANCE, null),
                                    Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE))),

                            Field.nullable("type", ArrowType.Utf8.INSTANCE)
                    ));

            // Glue: depends on lookupType -> Arrow: depends on resolved type
            case LOOKUP -> {
                NestedUIType newNestedUIType = new NestedUIType(larkField.nestedUIType().childType(), UITypeEnum.UNKNOWN);
                AthenaFieldLarkBaseMapping newLarkBaseMapping = new AthenaFieldLarkBaseMapping(larkField.athenaName(), larkField.larkBaseFieldName(), newNestedUIType);
                Field completeChildFieldDefinition = larkFieldToArrowField(newLarkBaseMapping);
                yield new Field("item", completeChildFieldDefinition.getFieldType(), completeChildFieldDefinition.getChildren());
            }

            default -> Field.nullable("item", ArrowType.Utf8.INSTANCE);
        };
    }

    /**
     * Provides the definitions for the child fields of a Lark STRUCT field,
     * based on the mapping defined in UITypeEnum.getGlueCatalogType.
     *
     * @param larkField The Lark FieldItem representing the STRUCT.
     * @return A List of Arrow Field definitions for the struct's children.
     */
    public static List<Field> getLarkStructChildFields(AthenaFieldLarkBaseMapping larkField)
    {
        UITypeEnum uiType = larkField.nestedUIType().uiType();

        return switch (uiType) {
            // Glue: struct<link:string,text:string>
            // Arrow Children: link:VARCHAR, text:VARCHAR
            case URL -> List.of(
                    Field.nullable("link", ArrowType.Utf8.INSTANCE),
                    Field.nullable("text", ArrowType.Utf8.INSTANCE)
            );

            // Glue: struct<address:string,adname:string,cityname:string,full_address:string,location:string,name:string,pname:string>
            // Arrow Children: Corresponding VARCHAR fields
            case LOCATION -> List.of(
                    Field.nullable("address", ArrowType.Utf8.INSTANCE),
                    Field.nullable("adname", ArrowType.Utf8.INSTANCE),
                    Field.nullable("cityname", ArrowType.Utf8.INSTANCE),
                    Field.nullable("full_address", ArrowType.Utf8.INSTANCE),
                    Field.nullable("location", ArrowType.Utf8.INSTANCE),
                    Field.nullable("name", ArrowType.Utf8.INSTANCE),
                    Field.nullable("pname", ArrowType.Utf8.INSTANCE)
            );

            // Glue: struct<id:string,name:string,en_name:string,email:string>
            // Arrow Children: Corresponding VARCHAR fields
            case CREATED_USER, MODIFIED_USER -> List.of(
                    Field.nullable("id", ArrowType.Utf8.INSTANCE),
                    Field.nullable("name", ArrowType.Utf8.INSTANCE),
                    Field.nullable("en_name", ArrowType.Utf8.INSTANCE),
                    Field.nullable("email", ArrowType.Utf8.INSTANCE)
            );

            default -> Collections.emptyList();
        };
    }

    /**
     * Helper to build an Arrow Field based on a Lark FieldItem.
     * Uses the other methods in this class to determine the correct Arrow type and children.
     *
     * @param larkField The FieldItem from Lark API.
     * @return The corresponding Arrow Field definition.
     */
    public static Field larkFieldToArrowField(AthenaFieldLarkBaseMapping larkField)
    {
        String fieldName = larkField.larkBaseFieldName();
        Types.MinorType minorType = larkFieldToArrowMinorType(larkField);
        boolean isNullable = true;
        List<Field> children = Collections.emptyList();
        FieldType fieldType;

        // Handle timestamp fields (when minorType is null)
        if (minorType == null) {
            UITypeEnum uiType = larkField.nestedUIType().uiType();
            if (uiType == UITypeEnum.DATE_TIME || uiType == UITypeEnum.CREATED_TIME || uiType == UITypeEnum.MODIFIED_TIME) {
                // Create Timestamp with millisecond precision and UTC timezone
                ArrowType timestampType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
                fieldType = new FieldType(isNullable, timestampType, null, null);
                return new Field(fieldName, fieldType, children);
            }
        }

        switch (requireNonNull(minorType)) {
            case LIST:
                Field childField = getLarkListChildField(larkField);
                children = Collections.singletonList(childField);
                fieldType = new FieldType(isNullable, ArrowType.List.INSTANCE, null, null);
                break;
            case STRUCT:
                children = getLarkStructChildFields(larkField);
                fieldType = new FieldType(isNullable, ArrowType.Struct.INSTANCE, null, null);
                break;
            case DECIMAL:
                fieldType = new FieldType(isNullable, new ArrowType.Decimal(38, 18, 128), null, null);
                break;
            case BIT:
                fieldType = new FieldType(isNullable, ArrowType.Bool.INSTANCE, null, null);
                break;
            case VARCHAR:
                fieldType = new FieldType(isNullable, ArrowType.Utf8.INSTANCE, null, null);
                break;
            default:
                ArrowType defaultArrowType = minorType.getType();
                fieldType = new FieldType(isNullable, defaultArrowType, null, null);
                break;
        }

        return new Field(fieldName, fieldType, children);
    }
}
