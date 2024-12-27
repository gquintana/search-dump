package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.AbstractAdapterTest;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class OpenSearchAdapterTest extends AbstractAdapterTest<OpenSearchWriter, OpenSearchReader> {
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
}