package com.github.gquintana.searchdump.zipfile;

import com.github.gquintana.searchdump.AbstractAdapterTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

class ZipFileSearchAdapterTest extends AbstractAdapterTest<ZipFileSearchWriter, ZipFileSearchReader> {
    @TempDir
    Path tempDir;

    @Override
    protected ZipFileSearchReader createReader() {
        return ZipFileSearchReader.read(createZipFile());
    }

    @Override
    protected ZipFileSearchWriter createWriter() {
        return ZipFileSearchWriter.write(createZipFile(), 10);
    }

    private @NotNull File createZipFile() {
        return tempDir.resolve("test.zip").toFile();
    }
}