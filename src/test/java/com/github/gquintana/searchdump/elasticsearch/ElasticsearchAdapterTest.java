package com.github.gquintana.searchdump.elasticsearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.SearchPortHelper;
import com.github.gquintana.searchdump.zipfile.ZipFileSearchReader;
import com.github.gquintana.searchdump.zipfile.ZipFileSearchWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.nio.file.Path;

@Testcontainers
class ElasticsearchAdapterTest {
    private static final String ELASTICSEARCH_USERNAME = "elastic";
    private static final String ELASTICSEARCH_PASSWORD = "Test.Adapter:1";
    @Container
    final ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.17.26")
            .withPassword(ELASTICSEARCH_PASSWORD);
    @TempDir
    Path tempDir;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Test
    void testExportImport() {
        try (ElasticsearchWriter writer = createWriter();
             ElasticsearchReader reader = createReader()
        ) {
            SearchPortHelper helper = new SearchPortHelper("test-1");
            helper.createAndFill(writer);
            writer.refreshIndex("test-1");
            helper.readAndCheck(reader);
        }
    }

    private @NotNull ElasticsearchReader createReader() {
        return new ElasticsearchReader("http://" + container.getHttpHostAddress(), ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD, true,10, "1m", jsonMapper);
    }

    private @NotNull ElasticsearchWriter createWriter() {
        return new ElasticsearchWriter("http://" + container.getHttpHostAddress(), ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD, true, 10, jsonMapper);
    }

    @Test
    void testCopyToZipFile() {
        File zipFile = tempDir.resolve("test-2.zip").toFile();
        SearchPortHelper helper = new SearchPortHelper("test-2");
        try (ElasticsearchWriter writer = createWriter()) {
            helper.createAndFill(writer);
            writer.refreshIndex("test-2");
        }
        try (ElasticsearchReader reader = createReader();
             ZipFileSearchWriter zipWriter = ZipFileSearchWriter.write(zipFile, 10)) {
            helper.copy(reader, zipWriter);
        }
        try (ZipFileSearchReader zipReader = ZipFileSearchReader.read(zipFile)) {
            helper.readAndCheck(zipReader);
        }
    }

    @Test
    void testCopyFromZipFile() {
        File zipFile = tempDir.resolve("test-3.zip").toFile();
        SearchPortHelper helper = new SearchPortHelper("test-3");
        try (ZipFileSearchWriter zipWriter = ZipFileSearchWriter.write(zipFile, 10)) {
            helper.createAndFill(zipWriter);
        }
        try (ZipFileSearchReader zipReader = ZipFileSearchReader.read(zipFile);
             ElasticsearchWriter writer = createWriter()) {
            helper.copy(zipReader, writer);
            writer.refreshIndex("test-3");
        }
        try (ElasticsearchReader reader = createReader()) {
            helper.readAndCheck(reader);
        }
    }

}