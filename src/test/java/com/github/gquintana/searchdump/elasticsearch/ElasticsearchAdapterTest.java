package com.github.gquintana.searchdump.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.AbstractAdapterTest;
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
import java.io.IOException;
import java.nio.file.Path;

@Testcontainers
class ElasticsearchAdapterTest extends AbstractAdapterTest<ElasticsearchWriter, ElasticsearchReader> {
    private static final String ELASTICSEARCH_USERNAME = "elastic";
    private static final String ELASTICSEARCH_PASSWORD = "Test.Adapter:1";
    @Container
    static final ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.17.26")
            .withPassword(ELASTICSEARCH_PASSWORD);
    @TempDir
    Path tempDir;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    static ElasticsearchClientFactory createClientFactory() {
        return new ElasticsearchClientFactory("http://" + container.getHttpHostAddress(), ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD,true);
    }
    protected @NotNull ElasticsearchReader createReader() {
        return new ElasticsearchReader(createClientFactory(), 10, "1m", jsonMapper);
    }

    protected @NotNull ElasticsearchWriter createWriter() {
        return new ElasticsearchWriter(createClientFactory(), 10, jsonMapper);
    }

    @Override
    protected void refreshIndex(ElasticsearchWriter writer, String index) {
        writer.refreshIndex(index);
    }

    @Test
    void copyToZipFile() {
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
    void copyFromZipFile() {
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

    @Test
    void copyToAndFromZipFile() throws IOException {
        File zipFile = tempDir.resolve("test-4.zip").toFile();
        SearchPortHelper helper = new SearchPortHelper("test-4");
        try (ElasticsearchWriter writer = createWriter()) {
            helper.createAndFill(writer);
            writer.refreshIndex("test-4");
        }
        // Backup
        try (ElasticsearchReader reader = createReader();
             ZipFileSearchWriter zipWriter = ZipFileSearchWriter.write(zipFile, 10)) {
            helper.copy(reader, zipWriter);
        }
        // Delete
        try(ElasticsearchClient client = createClientFactory().create()) {
            client.indices().delete(new DeleteIndexRequest.Builder().index("test-4").build());
        }
        // Restore
        try (ZipFileSearchReader zipReader = ZipFileSearchReader.read(zipFile);
             ElasticsearchWriter writer = createWriter()) {
            helper.copy(zipReader, writer);
            writer.refreshIndex("test-4");
        }
    }
}