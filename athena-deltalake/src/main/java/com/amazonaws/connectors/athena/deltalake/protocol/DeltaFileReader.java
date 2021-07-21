package com.amazonaws.connectors.athena.deltalake.protocol;

import java.io.BufferedReader;
import java.util.List;

public interface DeltaFileReader {

    boolean fileExists(String path);

    BufferedReader readFile(String path);

    List<String> listDirectoryFilesFrom(String fromPath, String directory);

    String fileSystemScheme();

}