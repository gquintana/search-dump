package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.SearchPortHelper;
import org.junit.jupiter.api.Test;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class OpenSearchAdapterTest {
    @Container
    final OpensearchContainer container = new OpensearchContainer("opensearchproject/opensearch:1.3.20");
    final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Test
    void testExportImport() {
        SearchPortHelper helper = new SearchPortHelper("test-1");
        try(OpenSearchWriter writer = new OpenSearchWriter(container.getHttpHostAddress(), container.getUsername(), container.getPassword(), true, 10, jsonMapper)) {
            helper.createAndFill(writer);
            writer.refreshIndex("test-1");
        }
        try(OpenSearchReader reader = new OpenSearchReader(container.getHttpHostAddress(), container.getUsername(), container.getPassword(), true, 10, "1m", jsonMapper)) {
            helper.readAndCheck(reader);
        }
    }
}