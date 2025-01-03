package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.configuration.Configuration;
import com.github.gquintana.searchdump.configuration.MissingConfigurationException;
import com.github.gquintana.searchdump.core.SearchFactory;

public class OpenSearchFactory implements SearchFactory {
    private final JsonMapper jsonMapper;

    public OpenSearchFactory(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    private OpenSearchClientFactory createClientFactory(Configuration configuration, String prefix) {
        return new OpenSearchClientFactory(
                configuration.getString(prefix + ".url").orElseThrow(() -> new MissingConfigurationException("reader.url")),
                configuration.getString(prefix + ".username").orElse(null),
                configuration.getString(prefix + ".password").orElse(null),
                configuration.getBoolean(prefix + ".ssl.verify").orElse(true));

    }

    @Override
    public OpenSearchReader createReader(Configuration configuration) {
        return new OpenSearchReader(
                createClientFactory(configuration, "reader"),
                configuration.getInt("reader.page.size").orElse(1000),
                configuration.getString("reader.scroll.time").orElse("5m"),
                jsonMapper
        );
    }

    @Override
    public OpenSearchWriter createWriter(Configuration configuration) {
        return new OpenSearchWriter(
                createClientFactory(configuration, "writer"),
                configuration.getInt("writer.bulk.size").orElse(1000),
                jsonMapper
        );
    }
}
