/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.data.helpers;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.Types;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CustomFieldVector
{
    public List<Object> objList;
    public Field field;

    public CustomFieldVector(Field field) {
        this.objList = new ArrayList<Object>();
        this.field = field;
    }

    public void add(Object childObj) {
        if (childObj == null) {
            this.objList.add(null);
            return;
        }

        Types.MinorType fieldType = Types.getMinorTypeForArrowType(this.field.getType());

        switch (fieldType) {
            case LIST:
                this.objList.add(((CustomFieldVector) childObj).objList);
                break;
            case STRUCT:
                String childName = ((CustomFieldVector) childObj).field.getName();
                List<Object> childObjList = ((CustomFieldVector) childObj).objList;

                for (int i = 0; i < childObjList.size(); i++) {
                    if (i >= this.objList.size()) {
                        this.objList.add(new LinkedHashMap<String, Object>());
                    }
                    Map<String, Object> curr = (Map<String, Object>) this.objList.get(i);
                    if (childObjList.get(i) != null) {
                        curr.put(childName, childObjList.get(i));
                    }
                }
                break;
            default:
                this.objList.add(childObj);
        }
    }

    public void addMap(CustomFieldVector keysObject, CustomFieldVector valuesObject) {
        if (keysObject.objList.size() != valuesObject.objList.size()) {
            throw new RuntimeException("Mismatched key/value object sizes.");
        }

        Map<Object, Object> keyValueMap = new LinkedHashMap<Object, Object>();
        for (int i = 0; i < keysObject.objList.size(); i++) {
            keyValueMap.put(keysObject.objList.get(i), valuesObject.objList.get(i));
        }
        this.objList.add(keyValueMap);
    }
}
