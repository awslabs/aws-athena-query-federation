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

public class StorageTree
{
    StorageNode2 root;
    StorageNode2 commonRoot;

    public StorageTree(StorageNode2 root)
    {
        this.root = root;
        commonRoot = null;
    }

    public void addElement(String elementValue)
    {
        String[] list = elementValue.split("/");

        // latest element of the list is the filename.extrension
        root.addElement(root.incrementalPath, list);
    }

    public void printTree()
    {
        //I move the tree common root to the current common root because I don't mind about initial folder
        //that has only 1 child (and no leaf)
        getCommonRoot();
        commonRoot.printNode(0);
    }

    public StorageNode2 getCommonRoot()
    {
        if (commonRoot != null) {
            return commonRoot;
        }
        else {
            StorageNode2 current = root;
            while (current.leafs.size() <= 0) {
                current = current.childs.get(0);
            }
            commonRoot = current;
            return commonRoot;
        }
    }
}
