package com.github.gquintana.searchdump.zipfile;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipFileSearchReader implements SearchReader, QuietCloseable {
    private final JsonMapper jsonMapper;
    private final ZipFile zipInputFile;

    private ZipFileSearchReader(ZipFile zipInputFile) {
        this.zipInputFile = zipInputFile;
        jsonMapper = JsonMapper.builder().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false).build();
    }

    public static ZipFileSearchReader read(File file) {
        try {
            return new ZipFileSearchReader(new ZipFile(file));
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    public List<String> listIndices(List<String> indexGlobs) {
        MultiGlobMatcher indexGlobMatcher = new MultiGlobMatcher(indexGlobs);
        return zipInputFile.stream()
                .flatMap(e -> {
                    var split = e.getName().split("[\\\\/]", 2);
                    var index = split[0];
                    return indexGlobMatcher.matches(index) ? Stream.of(index) : Stream.empty();
                })
                .distinct()
                .toList();

    }


    @Override
    public SearchIndex getIndex(String name) {
        ZipEntry entry = zipInputFile.getEntry(name + "/index.json");
        try (InputStream inputStream = zipInputFile.getInputStream(entry)) {
            Map<String, Object> index = jsonMapper.readValue(inputStream, Map.class);
            return new SearchIndex((String) index.get("name"),
                    (Map<String, Object>) index.get("settings"),
                    (Map<String, Object>) index.get("mappings"),
                    (Map<String, Object>) index.get("aliases"));
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public SearchDocumentReader readDocuments(String index) {
        return new ZipFileSearchDocumentReader(jsonMapper, zipInputFile, index);
    }

    @Override
    public void close() {
        try {
            zipInputFile.close();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }
}

