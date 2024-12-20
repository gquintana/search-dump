package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.configuration.Configuration;
import com.github.gquintana.searchdump.core.SearchAdapterFactory;

public class OpenSearchFactory implements SearchAdapterFactory {
    private final JsonMapper jsonMapper;

    public OpenSearchFactory(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    public OpenSearchReader createReader(Configuration configuration) {
        return new OpenSearchReader(
                configuration.getString("reader.url").orElseThrow(() -> new IllegalArgumentException("Missing reader.url")),
                configuration.getString("reader.username").orElse(null),
                configuration.getString("reader.password").orElse(null),
                configuration.getBoolean("reader.ssl-verify").orElse(true),
                configuration.getInt("reader.page-size").orElse(1000),
                configuration.getString("reader.scroll-time").orElse("5m"),
                jsonMapper
        );
    }

    @Override
    public OpenSearchWriter createWriter(Configuration configuration) {
        return new OpenSearchWriter(
                configuration.getString("writer.url").orElseThrow(() -> new IllegalArgumentException("Missing reader.url")),
                configuration.getString("writer.username").orElse(null),
                configuration.getString("writer.password").orElse(null),
                configuration.getBoolean("writer.ssl-verify").orElse(true),
                configuration.getInt("writer.bulk-size").orElse(1000),
                jsonMapper
        );
    }
}
