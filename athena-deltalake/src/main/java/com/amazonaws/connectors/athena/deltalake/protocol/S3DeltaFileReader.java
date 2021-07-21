package com.amazonaws.connectors.athena.deltalake.protocol;

import java.io.BufferedReader;
import java.util.List;

public class S3DeltaFileReader implements DeltaFileReader {
    @Override
    public boolean fileExists(String path) {
        return false;
    }

    @Override
    public BufferedReader readFile(String path) {
        return null;
    }

    @Override
    public List<String> listDirectoryFilesFrom(String fromPath, String directory) {
        return null;
    }

    @Override
    public String fileSystemScheme() {
        return null;
    }
}
