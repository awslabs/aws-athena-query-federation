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
package com.amazonaws.athena.storage.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StorageNode2
{
    List<StorageNode2> childs;
    List<StorageNode2> leafs;
    String data;
    String incrementalPath;

    public StorageNode2(String nodeValue, String incrementalPath)
    {
        childs = new ArrayList<StorageNode2>();
        leafs = new ArrayList<StorageNode2>();
        data = nodeValue;
        this.incrementalPath = incrementalPath;
    }

    public boolean isLeaf()
    {
        return childs.isEmpty() && leafs.isEmpty();
    }

    public void addElement(String currentPath, String[] list)
    {
        //Avoid first element that can be an empty string if you split a string that has a starting slash as /sd/card/
        while (list[0] == null || list[0].equals("")) {
            list = Arrays.copyOfRange(list, 1, list.length);
        }

        StorageNode2 currentChild = new StorageNode2(list[0], currentPath + "/" + list[0]);
        if (list.length == 1) {
            leafs.add(currentChild);
            return;
        }
        else {
            int index = childs.indexOf(currentChild);
            if (index == -1) {
                childs.add(currentChild);
                currentChild.addElement(currentChild.incrementalPath, Arrays.copyOfRange(list, 1, list.length));
            }
            else {
                StorageNode2 nextChild = childs.get(index);
                nextChild.addElement(currentChild.incrementalPath, Arrays.copyOfRange(list, 1, list.length));
            }
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        StorageNode2 cmpObj = (StorageNode2) obj;
        return incrementalPath.equals(cmpObj.incrementalPath) && data.equals(cmpObj.data);
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
    public void printNode(int increment)
    {
        for (int i = 0; i < increment; i++) {
            System.out.print(" ");
        }
        System.out.println(incrementalPath + (isLeaf() ? " -> " + data : ""));
        for (StorageNode2 n : childs) {
            n.printNode(increment + 2);
        }
        for (StorageNode2 n : leafs) {
            n.printNode(increment + 2);
        }
    }

    @Override
    public String toString()
    {
        return data;
    }
}
