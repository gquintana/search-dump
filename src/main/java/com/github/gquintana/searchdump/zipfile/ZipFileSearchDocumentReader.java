package com.github.gquintana.searchdump.zipfile;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentReader;
import com.github.gquintana.searchdump.core.TechnicalException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipFileSearchDocumentReader implements SearchDocumentReader {
    private final JsonMapper jsonMapper;
    private final ZipFile zipFile;
    private final Iterator<? extends ZipEntry> zipEntryIterator;
    private BufferedReader bufferedReader;
    private SearchDocument nextDocument;

    public ZipFileSearchDocumentReader(JsonMapper jsonMapper, ZipFile zipFile, List<ZipEntry> zipEntries) {
        this.jsonMapper = jsonMapper;
        this.zipFile = zipFile;
        try {
            this.zipEntryIterator = zipEntries.iterator();
            bufferedReader = nextZipEntry();
            nextDocument = nextDocument();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private SearchDocument nextDocument() throws IOException {
        if (bufferedReader == null) {
            return null;
        }
        String line = bufferedReader.readLine();
        if (line == null) {
            return null;
        }
        return jsonMapper.readValue(line, SearchDocument.class);
    }

    private BufferedReader nextZipEntry() throws IOException {
        if (!zipEntryIterator.hasNext()) {
            return null;
        }
        return new BufferedReader(new InputStreamReader(zipFile.getInputStream(zipEntryIterator.next())));
    }

    @Override
    public void close()  {
        try {
            closeZipEntry();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextDocument != null;
    }

    @Override
    public SearchDocument next() {
        try {
            SearchDocument result = nextDocument;
            nextDocument = nextDocument();
            if (nextDocument == null) {
                closeZipEntry();
                bufferedReader = nextZipEntry();
                nextDocument = nextDocument();
            }
            return result;
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private void closeZipEntry() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
            bufferedReader = null;
        }
    }
}
