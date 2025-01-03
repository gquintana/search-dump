package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.AbstractSearchTest;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
class OpenSearchTest extends AbstractSearchTest<OpenSearchWriter, OpenSearchReader> {
    @Container
    static final OpensearchContainer container = new OpensearchContainer("opensearchproject/opensearch:1.3.20");
    final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Override
    protected OpenSearchReader createReader() {
        return new OpenSearchReader(createClientFactory(), 10, "1m", jsonMapper);
    }

    @Override
    protected OpenSearchWriter createWriter() {
        return new OpenSearchWriter(createClientFactory(), 10, jsonMapper);
    }

    @Override
    protected void refreshIndex(OpenSearchWriter writer, String index) {
        writer.refreshIndex(index);
    }

    private OpenSearchClientFactory createClientFactory() {
        return new OpenSearchClientFactory(container.getHttpHostAddress(), container.getUsername(), container.getPassword(), true);

    }

    @Override
    protected void deleteIndex(String index) {
        try {
            OpenSearchClient client = createClientFactory().create();
            client.indices().delete(new DeleteIndexRequest.Builder().index(index).build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}