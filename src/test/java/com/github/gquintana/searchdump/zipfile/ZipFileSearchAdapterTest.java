package com.github.gquintana.searchdump.zipfile;

import com.github.gquintana.searchdump.SearchPortHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

class ZipFileSearchAdapterTest {
    @TempDir
    Path tempDir;

    @Test
    void testExportImport() throws Exception {
        File zipFile = tempDir.resolve("test.zip").toFile();
        var helper = new SearchPortHelper("test-1");
        try(ZipFileSearchWriter writer = ZipFileSearchWriter.write(zipFile, 10)) {
            helper.createAndFill(writer);
        }
        try(ZipFileSearchReader reader = ZipFileSearchReader.read(zipFile)) {
            helper.readAndCheck(reader);
        }
    }
}