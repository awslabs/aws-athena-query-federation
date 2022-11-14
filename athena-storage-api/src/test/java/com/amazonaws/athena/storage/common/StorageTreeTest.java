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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class StorageTreeTest
{
    @Test
    public void testStorageTreePrint()
    {
        String blobsNamesArray[] = new String[]{
                "zipcode/StateName='Tamil Nadu'/",
                "zipcode/StateName='UP'/",
                "zipcode/StateName='UP'/City='Lakhnow'/",
                "zipcode/StateName='UP'/City='Lakhnow'/PinCode=7000088/"
        };
        StorageTree tree = new StorageTree(new StorageNode2("root", "root"));
        for (String data : blobsNamesArray) {
            tree.addElement(data);
        }
        assertTrue("Children count didn't match", tree.getCommonRoot().childs.size() == 1);
        //tree.printTree();
        traverseTree(tree);
    }

    private void traverseTree(StorageTree tree)
    {
        printLeavesRecurse(tree.getCommonRoot(), tree.getCommonRoot().leafs);
//        List<StorageNode> nodeList = tree.getCommonRoot().getLeaves();
//        for (StorageNode node : nodeList) {
//            System.out.println("Node name: " + node.getNodeName() + ", @ path " + node.getPath());
//        }
    }

    private void printLeavesRecurse(StorageNode2 root, List<StorageNode2> nodeList)
    {
        for (StorageNode2 node : nodeList) {
            StorageNode2 actualNode = null;
            if (root.childs.contains(node)) {
                actualNode = root.childs.get(root.childs.indexOf(node));
                printChildrenRecurse(root, actualNode);
            }
            System.out.println("Node name: " + node.data + ", @ path " + node.incrementalPath);
            if (actualNode != null && actualNode.leafs.size() > 0) {
                printLeavesRecurse(root, node.leafs);
            }
        }
    }

    private void printChildrenRecurse(StorageNode2 root, StorageNode2 node)
    {
        System.out.println("Node name: " + node.data + ", @ path " + node.incrementalPath);
        for (StorageNode2 leaf : node.leafs) {
            System.out.println("Node name: " + leaf.data + ", @ path " + leaf.incrementalPath);
            StorageNode2 actualNode = null;
            if (root.childs.contains(leaf)) {
                actualNode = root.childs.get(root.childs.indexOf(leaf));
                printChildrenRecurse(root, actualNode);
            }
            if (actualNode != null && actualNode.leafs.size() > 0) {
                printLeavesRecurse(root, leaf.leafs);
            }
        }
    }
}
