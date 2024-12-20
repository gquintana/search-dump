package com.github.gquintana.searchdump.zipfile;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentWriter;
import com.github.gquintana.searchdump.core.TechnicalException;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileSearchDocumentWriter implements SearchDocumentWriter {
    private final JsonMapper jsonMapper;
    private final ZipOutputStream zipOutputStream;
    private final String index;
    private final int fileSize;
    private ZipEntry zipEntry;
    private int fileCount;
    private int fileLines;

    public ZipFileSearchDocumentWriter(JsonMapper jsonMapper, ZipOutputStream zipOutputStream, String index, int fileSize) {
        this.jsonMapper = jsonMapper;
        this.zipOutputStream = zipOutputStream;
        this.index = index;
        this.fileSize = fileSize;
    }

    private void startZipEntry() throws IOException {
        zipEntry = new ZipEntry(String.format("%s/documents-%06d.json", index, fileCount++));
        this.zipOutputStream.putNextEntry(zipEntry);
        this.fileLines = 0;
    }

    @Override
    public void write(SearchDocument document) {
        try {
            if (zipEntry == null) {
                startZipEntry();
            }
            jsonMapper.writeValue(zipOutputStream, document);
            zipOutputStream.write('\n');
            fileLines++;
            if (fileLines >= fileSize) {
                endZipEntry();
            }
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public void flush() {
        try {
            endZipEntry();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private void endZipEntry() throws IOException {
        zipOutputStream.closeEntry();
        zipEntry = null;
    }

    @Override
    public void close() {
        try {
            zipOutputStream.closeEntry();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }
}
