package com.github.gquintana.searchdump.zipfile;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileSearchWriter implements SearchWriter, QuietCloseable {
    private final JsonMapper jsonMapper;
    private final ZipOutputStream zipOutputStream;
    private final int writeFileSize;

    private ZipFileSearchWriter(ZipOutputStream zipOutputStream, int writeFileSize) {
        this.zipOutputStream = zipOutputStream;
        this.writeFileSize = writeFileSize;
        jsonMapper = JsonMapper.builder().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false).build();
    }

    public static ZipFileSearchWriter write(File file, int writeFileSize) {
        try {
            return new ZipFileSearchWriter(new ZipOutputStream(new FileOutputStream(file)), writeFileSize);
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public void createIndex(SearchIndex index) {
        try {
            zipOutputStream.putNextEntry(new ZipEntry(index.name() + "/index.json"));
            jsonMapper.writeValue(zipOutputStream, index);
            zipOutputStream.closeEntry();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public SearchDocumentWriter writeDocuments(String index) {
        return new ZipFileSearchDocumentWriter(jsonMapper, zipOutputStream, index, writeFileSize);
    }

    @Override
    public void close() {
        try {
            zipOutputStream.close();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }
}
