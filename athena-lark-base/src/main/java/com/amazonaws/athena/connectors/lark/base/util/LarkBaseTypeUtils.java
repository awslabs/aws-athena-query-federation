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
            // LOOKUP is included here to match the Glue Crawler path (UITypeEnum.getGlueCatalogType), which maps
            // LOOKUP to array<{target field's type}>. Without this, LOOKUP fell through to the default VARCHAR
            // case below, discarding the resolved target type entirely (see getLarkListChildField for how the
            // list's child element type is derived from nestedUIType().childType()).
            case MULTI_SELECT, USER, GROUP_CHAT, ATTACHMENT, CREATED_USER, MODIFIED_USER, LOOKUP -> Types.MinorType.LIST;

            // Glue: struct<...> -> Arrow: STRUCT
            case URL, LOCATION, SINGLE_LINK, DUPLEX_LINK -> Types.MinorType.STRUCT;

            // Glue: depends on formulaType -> Arrow: depends on resolved type
            case FORMULA -> larkFieldToArrowMinorType(unwrapFormulaTarget(larkField));

            default -> Types.MinorType.VARCHAR;
        };
    }

    /**
     * Unwraps a FORMULA field to the Lark field mapping for its resolved target type, so callers can
     * switch on the target's UITypeEnum directly instead of always seeing FORMULA. Mirrors the crawler's
     * equivalent unwrapping (a FORMULA's Glue type is built by calling the target UI type's own
     * getGlueCatalogType) - without this, a formula resolving to a LIST/STRUCT-shaped target (e.g.
     * Formula&lt;User&gt;, Formula&lt;Attachment&gt;) would get the correct Arrow MinorType (LIST/STRUCT,
     * via this method feeding larkFieldToArrowMinorType) but the wrong child structure, since
     * getLarkListChildField/getLarkStructChildFields would still see uiType=FORMULA - which neither
     * switches on - and fall through to their generic default instead of the target type's real shape.
     * Only unwraps one level, matching NestedUIType's own single-level (uiType, childType) shape and the
     * pre-existing behavior this mirrors; a formula resolving to a LOOKUP's own target is a deeper case
     * this shared model doesn't represent, unrelated to this fix.
     */
    private static AthenaFieldLarkBaseMapping unwrapFormulaTarget(AthenaFieldLarkBaseMapping larkField)
    {
        if (larkField.nestedUIType().uiType() != UITypeEnum.FORMULA) {
            return larkField;
        }
        NestedUIType targetNestedUIType = new NestedUIType(larkField.nestedUIType().childType(), UITypeEnum.UNKNOWN);
        return new AthenaFieldLarkBaseMapping(larkField.athenaName(), larkField.larkBaseFieldName(), targetNestedUIType);
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
        larkField = unwrapFormulaTarget(larkField);
        UITypeEnum uiType = larkField.nestedUIType().uiType();

        return switch (uiType) {
            // Glue: array<string> -> Arrow Child: VARCHAR
            case MULTI_SELECT -> Field.nullable("item", ArrowType.Utf8.INSTANCE);

            // Glue: array<struct<avatar_url:string,email:string,en_name:string,id:string,name:string>>
            // Arrow Child: Struct<avatar_url:VARCHAR, email:VARCHAR, en_name:VARCHAR, id:VARCHAR, name:VARCHAR>
            case USER -> new Field("user_info", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                    Field.nullable("avatar_url", ArrowType.Utf8.INSTANCE),
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

            // Glue: array<struct<avatar_url:string,email:string,en_name:string,id:string,name:string>>
            // Arrow Child: Struct<avatar_url:VARCHAR, email:VARCHAR, en_name:VARCHAR, id:VARCHAR, name:VARCHAR>
            case CREATED_USER, MODIFIED_USER -> new Field("user_info", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                    Field.nullable("avatar_url", ArrowType.Utf8.INSTANCE),
                    Field.nullable("email", ArrowType.Utf8.INSTANCE),
                    Field.nullable("en_name", ArrowType.Utf8.INSTANCE),
                    Field.nullable("id", ArrowType.Utf8.INSTANCE),
                    Field.nullable("name", ArrowType.Utf8.INSTANCE)
            ));

            // Glue: array<{target field's type}> -> Arrow Child: derived from the resolved LOOKUP target type
            // (nestedUIType().childType(), already resolved to a terminal, non-LOOKUP type by
            // LarkBaseService.getLookupType, which follows chained LOOKUPs to their final target).
            case LOOKUP -> lookupListItemField(larkField.nestedUIType().childType());

            default -> Field.nullable("item", ArrowType.Utf8.INSTANCE);
        };
    }

    /**
     * Builds the "item" Field for a LOOKUP list's child element from its resolved target type. The
     * target is already guaranteed terminal (not LOOKUP/FORMULA - LarkBaseService.getLookupType follows
     * chained LOOKUPs/FORMULAs to their final target before this ever runs), but it can still be
     * LIST-shaped (MULTI_SELECT, USER, ...) or STRUCT-shaped (URL, LOCATION, ...) itself, not just a
     * scalar. Matches the crawler's nesting for the same case: a LOOKUP aggregates one target-shaped
     * value per linked record, so a LIST-shaped target doubly-nests ("array&lt;array&lt;...&gt;&gt;" -
     * crawler's UITypeEnum.LOOKUP wraps the target's own "array&lt;...&gt;" Glue type in another array),
     * while a STRUCT-shaped target nests once ("array&lt;struct&lt;...&gt;&gt;"). Before this, only the
     * scalar case was handled - a Lookup&lt;User&gt; got a flat List&lt;Utf8&gt; instead of
     * List&lt;List&lt;Struct&lt;...&gt;&gt;&gt;, silently discarding the target's real shape entirely.
     */
    private static Field lookupListItemField(UITypeEnum targetUiType)
    {
        if (targetUiType == null) {
            return Field.nullable("item", ArrowType.Utf8.INSTANCE);
        }

        AthenaFieldLarkBaseMapping targetField = new AthenaFieldLarkBaseMapping(
                "item", "item", new NestedUIType(targetUiType, UITypeEnum.UNKNOWN));
        Types.MinorType targetMinorType = larkFieldToArrowMinorType(targetField);

        return switch (targetMinorType) {
            case LIST -> new Field("item", FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(getLarkListChildField(targetField)));
            case STRUCT -> new Field("item", FieldType.nullable(ArrowType.Struct.INSTANCE),
                    getLarkStructChildFields(targetField));
            default -> Field.nullable("item", scalarArrowTypeForLookupTarget(targetUiType));
        };
    }

    /**
     * Maps a LOOKUP field's resolved target UI type to a simple/scalar Arrow type, for use as the LOOKUP's
     * LIST child element type. Mirrors the Glue Crawler path's fallback semantics (glue-lark-base-crawler's
     * UITypeEnum.getGlueCatalogType), which resolves a LOOKUP target to its Glue type when the target is a
     * simple scalar (e.g. "decimal", "boolean", "timestamp"), and otherwise falls back to a plain string.
     *
     * @param targetUiType The resolved (terminal) UI type of the field the LOOKUP points to, or null/UNKNOWN
     *                     if it could not be resolved (e.g. a broken/circular reference or an API error).
     * @return The Arrow type to use for the LOOKUP list's child element.
     */
    private static ArrowType scalarArrowTypeForLookupTarget(UITypeEnum targetUiType)
    {
        if (targetUiType == null) {
            return ArrowType.Utf8.INSTANCE;
        }

        return switch (targetUiType) {
            case NUMBER, PROGRESS, CURRENCY -> new ArrowType.Decimal(38, 18, 128);
            case RATING -> Types.MinorType.TINYINT.getType();
            case CHECKBOX -> ArrowType.Bool.INSTANCE;
            // Matches the top-level DATE_TIME/CREATED_TIME/MODIFIED_TIME mapping in larkFieldToArrowField
            // (DATEMILLI, i.e. Arrow Date(MILLISECOND)) and the SDK's Glue "array<timestamp>" resolution,
            // so a LOOKUP-wrapped list child has the same Arrow type whichever metadata path serves it.
            case DATE_TIME, CREATED_TIME, MODIFIED_TIME -> Types.MinorType.DATEMILLI.getType();
            // TEXT, BARCODE, SINGLE_SELECT, PHONE, AUTO_NUMBER, EMAIL, and any type not yet supported as a
            // LOOKUP target (MULTI_SELECT, USER, ATTACHMENT, URL, LOCATION, LINK, UNKNOWN, ...) fall back to a
            // plain string representation, matching the Glue Crawler path's "array<string>" fallback.
            default -> ArrowType.Utf8.INSTANCE;
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
        larkField = unwrapFormulaTarget(larkField);
        UITypeEnum uiType = larkField.nestedUIType().uiType();

        return switch (uiType) {
            // Glue: struct<link:string,text:string,type:string>
            // Arrow Children: link:VARCHAR, text:VARCHAR, type:VARCHAR
            case URL -> List.of(
                    Field.nullable("link", ArrowType.Utf8.INSTANCE),
                    Field.nullable("text", ArrowType.Utf8.INSTANCE),
                    Field.nullable("type", ArrowType.Utf8.INSTANCE)
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

            // Glue: struct<link_record_ids:array<string>>
            // Arrow Children: link_record_ids:LIST
            // Search API format (returns as Map, not Array): { "link_record_ids": ["rec_xxx", "rec_yyy"] }
            case SINGLE_LINK, DUPLEX_LINK -> List.of(
                    new Field("link_record_ids",
                            new FieldType(true, ArrowType.List.INSTANCE, null),
                            Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE)))
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
        return larkFieldToArrowField(larkField, false);
    }

    /**
     * @param larkField The FieldItem from Lark API.
     * @param complexTypeAsJsonString When true, a field that would otherwise be LIST/STRUCT-shaped is
     * instead built as a plain VARCHAR column (see BaseConstants.DOES_ACTIVATE_COMPLEX_TYPE_AS_JSON_STRING_ENV_VAR).
     * Checked here, after the normal MinorType resolution, rather than threading it into
     * larkFieldToArrowMinorType/getLarkListChildField/getLarkStructChildFields - a LOOKUP wrapping a
     * List/Struct-shaped target is caught by the same single check without separate handling for the
     * wrapped case, and the children those methods would have computed are simply unused for VARCHAR.
     * @return The corresponding Arrow Field definition.
     */
    public static Field larkFieldToArrowField(AthenaFieldLarkBaseMapping larkField, boolean complexTypeAsJsonString)
    {
        String fieldName = larkField.larkBaseFieldName();
        // larkFieldToArrowMinorType always returns a non-null MinorType (it falls back to VARCHAR by
        // default), so DATE_TIME/CREATED_TIME/MODIFIED_TIME fields always resolve through the DATEMILLI
        // case below - there is no minorType==null case to special-case here.
        Types.MinorType minorType = larkFieldToArrowMinorType(larkField);
        if (complexTypeAsJsonString && (minorType == Types.MinorType.LIST || minorType == Types.MinorType.STRUCT)) {
            minorType = Types.MinorType.VARCHAR;
        }
        boolean isNullable = true;
        List<Field> children = Collections.emptyList();
        FieldType fieldType;

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
