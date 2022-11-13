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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.storage.datasource.parquet.filter.EqualsExpression;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.athena.storage.gcs.io.GcsStorageProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.IOException;
import java.util.*;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GcsStorageProvider.class})
public class StorageNodeTest
{
    private final static String BUCKET = "mydatalake1";

    @BeforeAll
    public static void setUp()
    {
//        mockStatic(GcsStorageProvider.class);
//        when(GcsStorageProvider.accept(anyString())).thenReturn(true);
    }

    @Test
    public void testParentPath()
    {
        String[] paths = {
                "birthday/",
                "birthday/year=2000/",
                "birthday/year=2000/birthday.parquet",
                "zipcode/",
                "zipcode/StateName='TamilNadu'/",
                "zipcode/StateName='TamilNadu'/zipcode.parquet",
                "zipcode/StateName='UP'/",
                "zipcode/StateName='UP'/zipcode.parquet",
        };
        for (String path : paths) {
            System.out.println(getParentPath(path));
        }
    }

    @Test
    public void testNodesAreSortedByName() throws Exception {
        TreeTraversalContext context = TreeTraversalContext.builder()
                .includeFile(false)
                .maxDepth(0)
                .storageDatasource(getTestDataSource("csv"))
                .build();
        StorageNode<String> root = new StorageNode<>("zipcode", "zipcode/", context);
        StorageNode<String> child = root.addChild("StateName='UP'", "zipcode/StateName='UP'/", context);
        child.addChild("D", "D:\\", context);
        child.addChild("C", "C:\\", context);
        root.addChild("StateName='Tamil Nadu'", "zipcode/StateName='Tamil Nadu'/", context);
        printChildrenRecurse(root.getChildren());
    }

    @Test
    public void testStorageTree() throws Exception {
        String[] paths = {
                "birthday/",
                "birthday/year=2000/",
                "birthday/year=2000/birthday.parquet",
                "zipcode/",
                "zipcode/StateName='TamilNadu'/",
                "zipcode/StateName='TamilNadu'/zipcode.parquet",
                "zipcode/StateName='UP'/",
                "zipcode/StateName/",
                "zipcode/StateName='UP'/zipcode.parquet",
                "zipcode/PinCode/"
        };
        TreeTraversalContext context = TreeTraversalContext.builder()
                .hasParent(false)
                .includeFile(false)
                .maxDepth(3)
//                .partitionDepth(1)
                .storageDatasource(getTestDataSource("parquet"))
                .build();
//        context.addAllFilers(List.of(
//                new EqualsExpression(1, "statename", "UP")
//                , new EqualsExpression(1, "statename", "TamilNadu")
//                , new EqualsExpression(1, "year", "2000")
//        ));
        StorageNode<String> root = new StorageNode<>("/", "/", context);
        for (String data : paths) {
            String[] names = context.normalizePaths(data.split("/"));
            if (names.length == 0 || !root.isChild(names[0])) {
                continue;
            }
            StorageNode<String> parent = root;
            for (int i = 0; i < names.length; i++) {
                if (parent.getPath().equals(names[i])) {
                    continue;
                }
                String path = String.join("/",
                        Arrays.copyOfRange(names, 0, i + 1));
                if (!context.isIncludeFile() && context.isFile(BUCKET, path)) {
                    continue;
                }
                if (context.getPartitionDepth() > -1 && !context.isPartitioned(i, names[i])) {
                    continue;
                }
                Optional<StorageNode<String>> optionalParent = root.findByPath(getParentPath(path));
                if (optionalParent.isPresent()) {
                    parent = optionalParent.get();
                    if (parent.getPath().equals(path)) {
                        continue;
                    }
                    parent = parent.addChild(names[i], path, context);
                }
                else {
                    parent = parent.addChild(names[i], path, context);
                }
            }
        }
        printChildrenRecurse(root.getChildren());
    }

    private Optional<StorageNode<String>> addRootWithChildren(String[] paths, TreeTraversalContext context)
    {
        if (context.getMaxDepth() != 0 && paths.length > context.getMaxDepth()) {
            paths = Arrays.copyOfRange(paths, 0, context.getMaxDepth());
        }

        StorageNode<String> root;
        if (paths.length == 1) {
            return Optional.of(new StorageNode<>(paths[0], paths[0], context));
        }
        root = new StorageNode<>(paths[0], paths[0], context);
        String[] restOfThePaths;
        if (paths.length > 1) {
            restOfThePaths = Arrays.copyOfRange(paths, 1, paths.length);
        }
        else {
            return Optional.of(root);
        }
        StorageNode<String> parent = root;
        for (int i = 0; i < restOfThePaths.length; i++) {
            String path = root.getPath() + "/" + String.join("/",
                    Arrays.copyOfRange(restOfThePaths, 0, i + 1));
            parent = parent.addChild(restOfThePaths[i], path, context);
        }
        return Optional.of(root);
    }

    private String getParentPath(String path)
    {
        if (path.endsWith("/") && path.trim().length() == 1) {
            return null;
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
            if (path.trim().length() == 1) {
                return path;
            }
            return finalParentPath(path);
        }
        else if (path.trim().length() > 0) {
           return finalParentPath(path);
        }
        return null;

    }

    private static String finalParentPath(String path)
    {
        int lastPathSeparatorIndex = path.lastIndexOf("/");
        if (lastPathSeparatorIndex > -1) {
            return path.substring(0, lastPathSeparatorIndex);
        }
        return null;
    }

    private void printChildrenRecurse(Set<StorageNode<String>> children)
    {
        System.out.println(children);
        for (StorageNode<String> node : children) {
            if (!node.isLeaf()) {
                printChildrenRecurse(node.getChildren());
            }
        }
    }

    private StorageDatasource getTestDataSource(final String extension) throws Exception
    {
        String appCredentialsJsonString = "{\n" +
                "  \"type\": \"service_account\",\n" +
                "  \"project_id\": \"athena-federated-query\",\n" +
                "  \"private_key_id\": \"2ad4078f1692a6d3887ab6c4a97254c755566646\",\n" +
                "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDIPRq3S8SHygD/\\nDXjq927DtcJ2cLSn0Gl3hFuArAUJjtRb8q0/Zw17drzkH2X+OrtMCutI8rwRmk/M\\nbR3nwetaAS/lSTtw+oN4PeYHAFokjLa12VqJl+zIM2GE0DLXRdar0YlAvwJ89P0B\\nV1/Uk7wdeOGxEfLrc5TIhxV5adV+QFS7jjgRBnkFxsMRMqKb1OMDvaHreqaiXJpP\\nd75Ut1prwB2JLkElfqMBBVcm8o1pSqsEGUamluQVEDK3E96XolsGPVe31k0o7rzf\\nyLP7MUvcwniIq4lrzSjeZ25N3FPkquUGbn+mPsNkLxCbOwcrY/jDxXNOUZTMLeqp\\n7Vkt01OxAgMBAAECggEACoNy0KbwxbRsgvfBHo0pSqtTb4aRZbJCp1zStRnTFeJi\\n8gT25bpSceYVGuEvKL8KsH5uRiFAkKgKgpBEHrQG2G3xhtdmgJMWrgyJ9FonYX3l\\n5fxojYrlislv2FpaIQVwtQrGaxjcV5VBZ32f8XhkSyParcJkf8pMyI6XKQ3OgNdm\\nCTVDD4C60KNq9a4sJqKBopB+FJLn4TnUGLZiZAV+A4uKLUA38zwga6oMWCygxgZz\\nE5eWy17I211NqSju9PErsGH6z4zApR4pTWKctpfn+dZQTBS8ZSoC7g2BBxLk3C0w\\niYlUQXt/KUCZ0xaI+6mQqq1QrvGCa54EKArjc+oxNQKBgQDw9Lst+U5QDNFD4vEg\\n9/NwsOxc79pREG2FgLrw4lZqfHX8o8llg7b8Y1c2ysrYj0B/0apCQVIVDZoRHerj\\nJr0dyXWklotRiXPZ67itPXhun95GXVdUNiSRYwwJEwkIhK584d90j5eXtTCyr5FM\\nWuZadBsAAtv4ZgqIYObiLy3yOwKBgQDUvZO1Jhf1r4MHnhCQ1Nv+2qgnhEtp3uHQ\\ni5h9N3cH0DmPBOisp1qcSlIYwUM8ufVBcEhNTsIAffbDAViWa05ju9/wUcIA6jBi\\nR8P8PF/Ex8TdZtgUCTabysfcONKOLZ2Sa7E0/O4Y9EZgnzsFkZWfx/eSvtW0a3Za\\neB19tH+nAwKBgQCPtIXF/3/zQhG0eS7ySK7JsNrm+q2r1y5ahtH3RCXh0GTVziEZ\\nCBskH2MubHfZ/GWtVbBDX43CvJ/8QWmLG9mCYFpnVNm2QVH00B8OQzEGWRZJxPWG\\nZdwdUYMmDlI+4FLobBXHALSaaBepGgiAD15+5+wKb6odVU5G0/QfRaATbQKBgQDJ\\n+suYO3iYDHDs9Idp2o6cYuEv04z+EVx38XFvwQ9D3dAoF1MJSULgDDfxxNufdjaC\\nUKZ0r4fFi9KSxl5jQbIFQsSUmCsHT1FsnhJXEsMiQ0CHrDMOosi0FUy3q0NNNcXa\\n1GBEnLc5/gIrjkItQVG7h9FoA8NGLpkJv+zQAmUIHQKBgQCG4I9+AsyDJsJvhAyy\\nT5T4oFIHHMD4ODZl+2rdmtEW07KbHatPme0hcMTwlVyZdN/AFJpZZ94Rm0/Zc6c8\\nwaz0nY3XnDw39+6TI6STjLeANIdc1GGTOVsUydsdr7P/MvRHrMABzbbPC7vp7N0v\\nf7lV2ouT0TltRUnyQ3GsqR31fw==\\n-----END PRIVATE KEY-----\\n\",\n" +
                "  \"client_email\": \"akshay-kachore-trianz-com@athena-federated-query.iam.gserviceaccount.com\",\n" +
                "  \"client_id\": \"105657958960441983486\",\n" +
                "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
                "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
                "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
                "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/akshay-kachore-trianz-com%40athena-federated-query.iam.gserviceaccount.com\"\n" +
                "}";
        return StorageDatasourceFactory.createDatasource(appCredentialsJsonString, Map.of(FILE_EXTENSION_ENV_VAR, extension));
    }
}
